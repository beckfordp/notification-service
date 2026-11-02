package notificationservice

import cats.effect.IO
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import io.circe.parser.decode
import munit.CatsEffectSuite
import org.http4s.Method.GET
import org.http4s.{Request, Status, Uri}
import org.testcontainers.utility.DockerImageName
import org.typelevel.log4cats.testing.StructuredTestingLogger
import sttp.tapir.server.http4s.Http4sServerInterpreter

import java.util.UUID

class EmailRoutesSuite extends CatsEffectSuite with TestContainerForAll {

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

  private def email(
      eventId: String,
      customerId: Option[String],
      orderId: Option[String]
  ): NewSentEmail =
    NewSentEmail(
      id = UUID.randomUUID(),
      sourceEventId = eventId,
      eventType = "OrderConfirmed",
      reason = None,
      recipientType =
        if (customerId.isDefined) RecipientType.Customer
        else RecipientType.Order,
      recipientKey = customerId.orElse(orderId).getOrElse("x"),
      customerId = customerId,
      orderId = orderId,
      subject = s"subject-$eventId",
      body = s"body-$eventId"
    )

  private def withRoutes[A](
      body: (org.http4s.HttpRoutes[IO], SentEmailStore[IO]) => IO[A]
  ): IO[A] =
    withContainers { postgres =>
      Migrations.run[IO](configFor(postgres)) *> truncate(postgres) *>
        SentEmailStore
          .postgres[IO](configFor(postgres))
          .use { store =>
            val logger = StructuredTestingLogger.impl[IO]()
            val routes = Http4sServerInterpreter[IO]().toRoutes(
              List(EmailRoutes.serverEndpoint[IO](store, logger))
            )
            body(routes, store)
          }
    }

  private def truncate(postgres: PostgreSQLContainer): IO[Unit] =
    IO.blocking {
      val conn = java.sql.DriverManager.getConnection(
        postgres.jdbcUrl,
        postgres.username,
        postgres.password
      )
      try {
        conn.createStatement().executeUpdate("delete from sent_email")
        ()
      } finally conn.close()
    }

  private def get(
      routes: org.http4s.HttpRoutes[IO],
      path: String
  ): IO[(Status, String)] =
    routes.orNotFound
      .run(Request[IO](GET, Uri.unsafeFromString(path)))
      .flatMap { response =>
        response.bodyText.compile.string.map(response.status -> _)
      }

  test("GET /emails returns newest first and exposes the audit fields") {
    withRoutes { (routes, store) =>
      for {
        first <- store.claim(email("e1", Some("ada"), Some("o1")))
        _ <- store.markSent(first match {
          case EmailClaim.Send(id)  => id
          case EmailClaim.Duplicate => fail("first claim must insert")
        })
        _ <- store.claim(email("e2", Some("grace"), Some("o2")))
        response <- get(routes, "/emails")
        decoded <- IO.fromEither(
          decode[List[SentEmailResponse]](response._2)
        )
      } yield {
        assertEquals(response._1, Status.Ok)
        assertEquals(decoded.map(_.sourceEventId), List("e2", "e1"))
        val sent = decoded.find(_.sourceEventId == "e1").get
        assertEquals(sent.eventType, "OrderConfirmed")
        assertEquals(sent.recipientType, "customer")
        assertEquals(sent.recipientKey, "ada")
        assertEquals(sent.customerId, Some("ada"))
        assertEquals(sent.orderId, Some("o1"))
        assertEquals(sent.status, "sent")
        assert(sent.sentAt.isDefined)
        assertEquals(sent.failedAt, None)
        assertEquals(sent.error, None)
      }
    }
  }

  test("GET /emails filters by customerId, orderId, both or neither") {
    withRoutes { (routes, store) =>
      for {
        _ <- store.claim(email("e1", Some("ada"), Some("o1")))
        _ <- store.claim(email("e2", Some("ada"), Some("o2")))
        _ <- store.claim(email("e3", Some("grace"), Some("o1")))
        _ <- store.claim(email("e4", None, Some("o3")))
        byCustomer <- get(routes, "/emails?customerId=ada")
        byOrder <- get(routes, "/emails?orderId=o1")
        both <- get(routes, "/emails?customerId=ada&orderId=o1")
        unknown <- get(routes, "/emails?customerId=nobody")
      } yield {
        def ids(body: String): List[String] =
          decode[List[SentEmailResponse]](body) match {
            case Right(rows) => rows.map(_.sourceEventId)
            case Left(error) =>
              fail(s"invalid /emails JSON: ${error.getMessage}")
          }
        assertEquals(ids(byCustomer._2).sorted, List("e1", "e2"))
        assertEquals(ids(byOrder._2).sorted, List("e1", "e3"))
        assertEquals(ids(both._2), List("e1"))
        assertEquals(ids(unknown._2), Nil)
        assertEquals(unknown._1, Status.Ok)
      }
    }
  }
}
