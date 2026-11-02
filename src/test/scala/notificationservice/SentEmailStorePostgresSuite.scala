package notificationservice

import cats.effect.IO
import cats.syntax.all._
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.CatsEffectSuite
import org.testcontainers.utility.DockerImageName

import java.sql.DriverManager
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID

class SentEmailStorePostgresSuite
    extends CatsEffectSuite
    with TestContainerForAll {

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(dockerImageName =
      DockerImageName.parse("postgres:16-alpine")
    )

  private def configFor(postgres: PostgreSQLContainer): PostgresConfig =
    PostgresConfig(
      host = postgres.host,
      port = postgres.mappedPort(5432),
      database = postgres.databaseName,
      user = postgres.username,
      password = postgres.password
    )

  private def truncate(postgres: PostgreSQLContainer): IO[Unit] =
    IO.blocking {
      val conn = DriverManager.getConnection(
        postgres.jdbcUrl,
        postgres.username,
        postgres.password
      )
      try {
        conn.createStatement().executeUpdate("delete from sent_email")
        ()
      } finally conn.close()
    }

  private def setCreatedAt(
      postgres: PostgreSQLContainer,
      sourceEventId: String,
      createdAt: Instant
  ): IO[Unit] =
    IO.blocking {
      val conn = DriverManager.getConnection(
        postgres.jdbcUrl,
        postgres.username,
        postgres.password
      )
      try {
        val stmt = conn.prepareStatement(
          "update sent_email set created_at = ? where source_event_id = ?"
        )
        try {
          stmt.setObject(1, OffsetDateTime.ofInstant(createdAt, ZoneOffset.UTC))
          stmt.setString(2, sourceEventId)
          stmt.executeUpdate()
          ()
        } finally stmt.close()
      } finally conn.close()
    }

  private def newEmail(
      sourceEventId: String,
      customerId: Option[String] = Some("customer-1"),
      orderId: Option[String] = Some("order-1"),
      recipientEmail: Option[String] = None
  ): NewSentEmail =
    NewSentEmail(
      id = UUID.randomUUID(),
      sourceEventId = sourceEventId,
      eventType = "OrderConfirmed",
      reason = None,
      recipientType = RecipientType.Customer,
      recipientKey = customerId.getOrElse("customer-1"),
      recipientEmail = recipientEmail,
      customerId = customerId,
      orderId = orderId,
      subject = "subject",
      body = "body"
    )

  private def withStore(
      name: String
  )(body: (SentEmailStore[IO], PostgreSQLContainer) => IO[Unit]): Unit =
    test(name) {
      withContainers { postgres =>
        Migrations.run[IO](configFor(postgres)) *> truncate(postgres) *>
          SentEmailStore
            .postgres[IO](configFor(postgres))
            .use(store => body(store, postgres))
      }
    }

  withStore("ping is true while Postgres answers") { (store, _) =>
    store.ping.map(assert(_))
  }

  test("ping is false when Postgres is unreachable") {
    SentEmailStore
      .postgres[IO](
        PostgresConfig("localhost", 1, "notification", "nobody", "nothing")
      )
      .use(_.ping)
      .map(reachable => assert(!reachable))
  }

  withStore("claim inserts a pending row") { (store, _) =>
    val email = newEmail("event-1")
    for {
      claim <- store.claim(email)
      row <- store.get("event-1")
    } yield {
      assertEquals(claim, EmailClaim.Send(email.id))
      assertEquals(row.map(_.status), Some(EmailStatus.Pending))
      assertEquals(row.map(_.sourceEventId), Some("event-1"))
      assertEquals(row.flatMap(_.customerId), Some("customer-1"))
      assertEquals(row.flatMap(_.orderId), Some("order-1"))
      assert(row.exists(_.createdAt.isAfter(Instant.EPOCH)))
      assertEquals(row.flatMap(_.sentAt), None)
    }
  }

  withStore("claim reuses a pending row") { (store, _) =>
    val first = newEmail("event-1")
    for {
      _ <- store.claim(first)
      claim <- store.claim(newEmail("event-1"))
      row <- store.get("event-1")
    } yield {
      assertEquals(claim, EmailClaim.Send(first.id))
      assertEquals(row.map(_.id), Some(first.id))
      assertEquals(row.map(_.status), Some(EmailStatus.Pending))
    }
  }

  withStore("claim is a duplicate once the row is sent") { (store, _) =>
    val first = newEmail("event-1")
    for {
      _ <- store.claim(first)
      _ <- store.markSent(first.id)
      claim <- store.claim(newEmail("event-1"))
      row <- store.get("event-1")
    } yield {
      assertEquals(claim, EmailClaim.Duplicate)
      assertEquals(row.map(_.status), Some(EmailStatus.Sent))
      assert(row.flatMap(_.sentAt).isDefined)
    }
  }

  withStore("claim retries a failed row, clearing the failure") { (store, _) =>
    val first = newEmail("event-1")
    for {
      _ <- store.claim(first)
      _ <- store.markFailed(first.id, "smtp down")
      failed <- store.get("event-1")
      claim <- store.claim(newEmail("event-1"))
      retried <- store.get("event-1")
    } yield {
      assertEquals(failed.map(_.status), Some(EmailStatus.Failed))
      assertEquals(failed.flatMap(_.error), Some("smtp down"))
      assert(failed.flatMap(_.failedAt).isDefined)
      assertEquals(claim, EmailClaim.Send(first.id))
      assertEquals(retried.map(_.status), Some(EmailStatus.Pending))
      assertEquals(retried.flatMap(_.error), None)
      assertEquals(retried.flatMap(_.failedAt), None)
    }
  }

  withStore("markSent records the sent timestamp") { (store, _) =>
    val email = newEmail("event-1")
    for {
      _ <- store.claim(email)
      _ <- store.markSent(email.id)
      row <- store.get("event-1")
    } yield {
      assertEquals(row.map(_.status), Some(EmailStatus.Sent))
      assert(row.flatMap(_.sentAt).isDefined)
      assertEquals(row.flatMap(_.failedAt), None)
    }
  }

  withStore("markResolved records attribution and survives a retry") {
    (store, _) =>
      val first = newEmail("event-1", Some("customer-1"), recipientEmail = None)
      for {
        _ <- store.claim(first)
        _ <- store.markResolved(
          first.id,
          Some("customer-1"),
          Some("ada@example.com")
        )
        resolved <- store.get("event-1")
        _ <- store.markFailed(first.id, "smtp down")
        claim <- store.claim(newEmail("event-1"))
        retried <- store.get("event-1")
      } yield {
        assertEquals(
          resolved.flatMap(_.recipientEmail),
          Some("ada@example.com")
        )
        assertEquals(claim, EmailClaim.Send(first.id))
        assertEquals(
          retried.flatMap(_.recipientEmail),
          Some("ada@example.com")
        )
        assertEquals(retried.flatMap(_.customerId), Some("customer-1"))
      }
  }

  withStore("markUnresolved is terminal and reported as a duplicate") {
    (store, _) =>
      val first = newEmail("event-1")
      for {
        _ <- store.claim(first)
        _ <- store.markUnresolved(first.id, "unknown_customer:customer-1")
        row <- store.get("event-1")
        claim <- store.claim(newEmail("event-1"))
      } yield {
        assertEquals(row.map(_.status), Some(EmailStatus.Unresolved))
        assertEquals(
          row.flatMap(_.error),
          Some("unknown_customer:customer-1")
        )
        assertEquals(claim, EmailClaim.Duplicate)
      }
  }

  withStore("list filters by customerId, orderId, both or neither") {
    (store, _) =>
      val ada1 = newEmail("event-1", Some("ada"), Some("order-1"))
      val ada2 = newEmail("event-2", Some("ada"), Some("order-2"))
      val grace = newEmail("event-3", Some("grace"), Some("order-1"))
      for {
        _ <- List(ada1, ada2, grace).traverse_(store.claim)
        _ <- store.claim(newEmail("event-4", None, Some("order-3")))
        byCustomer <- store.list(Some("ada"), None)
        byOrder <- store.list(None, Some("order-1"))
        both <- store.list(Some("ada"), Some("order-1"))
        all <- store.list(None, None)
        none <- store.list(Some("nobody"), None)
      } yield {
        assertEquals(
          byCustomer.map(_.sourceEventId).sorted,
          List("event-1", "event-2")
        )
        assertEquals(
          byOrder.map(_.sourceEventId).sorted,
          List("event-1", "event-3")
        )
        assertEquals(both.map(_.sourceEventId), List("event-1"))
        assertEquals(all.size, 4)
        assertEquals(none, Nil)
      }
  }

  withStore("list returns the newest row first") { (store, postgres) =>
    val now = Instant.parse("2026-11-02T10:00:00Z")
    for {
      _ <- store.claim(newEmail("event-1"))
      _ <- store.claim(newEmail("event-2"))
      _ <- setCreatedAt(postgres, "event-1", now.minusSeconds(60))
      _ <- setCreatedAt(postgres, "event-2", now)
      rows <- store.list(None, None)
    } yield assertEquals(rows.map(_.sourceEventId), List("event-2", "event-1"))
  }
}
