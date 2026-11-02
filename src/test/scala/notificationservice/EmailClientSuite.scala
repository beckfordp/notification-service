package notificationservice

import cats.effect.{IO, Ref}
import cats.syntax.all._
import munit.CatsEffectSuite
import org.typelevel.log4cats.testing.StructuredTestingLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger.INFO

import java.time.Instant
import java.util.UUID

class EmailClientSuite extends CatsEffectSuite {

  private val message = EmailMessage(
    sourceEventId = "e1",
    eventType = "OrderConfirmed",
    reason = None,
    recipientType = RecipientType.Customer,
    recipientKey = "cust-1",
    customerId = Some("cust-1"),
    orderId = Some("order-1"),
    subject = "Your order is confirmed",
    body = "Order order-1 has been confirmed."
  )

  test("EmailClient.logging logs the recipient and subject on send") {
    for {
      testLogger <- IO.pure(StructuredTestingLogger.impl[IO]())
      client = EmailClient.logging[IO](testLogger)
      _ <- client.send(message)
      logged <- testLogger.logged
    } yield {
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.ctx.get("to").contains("cust-1") &&
            m.ctx.get("subject").contains("Your order is confirmed")
        ),
        s"expected an INFO line logging the recipient and subject, got: $infos"
      )
    }
  }

  test("the audited client inserts pending, sends, then marks sent") {
    for {
      store <- InMemorySentEmailStore.create
      transport <- RecordingEmailClient.create
      (client, emails) = transport
      logger = StructuredTestingLogger.impl[IO]()
      audited = EmailClient.audited[IO](store, client, logger)
      _ <- audited.send(message)
      rows <- store.list(None, None)
      sent <- emails.get
    } yield {
      assertEquals(sent.map(_.to), List("cust-1"))
      assertEquals(rows.map(_.status), List(EmailStatus.Sent))
      assertEquals(rows.map(_.sourceEventId), List("e1"))
      assertEquals(rows.flatMap(_.sentAt).size, 1)
    }
  }

  test("the audited client does not send a duplicate sent event again") {
    for {
      store <- InMemorySentEmailStore.create
      _ <- store
        .claim(
          NewSentEmail(
            UUID.randomUUID(),
            "e1",
            "OrderConfirmed",
            None,
            RecipientType.Customer,
            "cust-1",
            Some("cust-1"),
            Some("order-1"),
            "subject",
            "body"
          )
        )
        .flatMap {
          case EmailClaim.Send(id)  => store.markSent(id)
          case EmailClaim.Duplicate => IO.unit
        }
      transport <- RecordingEmailClient.create
      (client, emails) = transport
      logger = StructuredTestingLogger.impl[IO]()
      audited = EmailClient.audited[IO](store, client, logger)
      _ <- audited.send(message)
      sent <- emails.get
      rows <- store.list(None, None)
    } yield {
      assertEquals(sent, Nil)
      assertEquals(rows.map(_.status), List(EmailStatus.Sent))
    }
  }

  test("the audited client marks failed and rethrows when the send fails") {
    for {
      store <- InMemorySentEmailStore.create
      transport = new EmailClient[IO] {
        def send(message: EmailMessage): IO[Unit] =
          IO.raiseError(new RuntimeException("smtp down"))
      }
      logger = StructuredTestingLogger.impl[IO]()
      audited = EmailClient.audited[IO](store, transport, logger)
      result <- audited.send(message).attempt
      rows <- store.list(None, None)
    } yield {
      assert(result.isLeft)
      assertEquals(rows.map(_.status), List(EmailStatus.Failed))
      assertEquals(rows.flatMap(_.error), List("smtp down"))
    }
  }

  private object InMemorySentEmailStore {
    def create: IO[SentEmailStore[IO]] =
      Ref.of[IO, Map[String, SentEmailRow]](Map.empty).map(fromRef)

    private def fromRef(
        ref: Ref[IO, Map[String, SentEmailRow]]
    ): SentEmailStore[IO] =
      new SentEmailStore[IO] {
        def claim(email: NewSentEmail): IO[EmailClaim] =
          ref.modify { rows =>
            rows.get(email.sourceEventId) match {
              case Some(existing) if existing.status == EmailStatus.Sent =>
                rows -> EmailClaim.Duplicate
              case Some(existing) =>
                val pending = existing.copy(status = EmailStatus.Pending)
                rows.updated(email.sourceEventId, pending) -> EmailClaim.Send(
                  existing.id
                )
              case None =>
                val row = SentEmailRow(
                  email.id,
                  email.sourceEventId,
                  email.eventType,
                  email.reason,
                  email.recipientType,
                  email.recipientKey,
                  email.customerId,
                  email.orderId,
                  email.subject,
                  email.body,
                  EmailStatus.Pending,
                  Instant.now(),
                  None,
                  None,
                  None
                )
                rows.updated(email.sourceEventId, row) -> EmailClaim.Send(
                  email.id
                )
            }
          }

        def markSent(id: UUID): IO[Unit] =
          ref.update(rows =>
            rows.map { case (k, row) =>
              if (row.id == id)
                k -> row.copy(
                  status = EmailStatus.Sent,
                  sentAt = Some(Instant.now()),
                  failedAt = None,
                  error = None
                )
              else k -> row
            }
          )

        def markFailed(id: UUID, error: String): IO[Unit] =
          ref.update(rows =>
            rows.map { case (k, row) =>
              if (row.id == id)
                k -> row.copy(
                  status = EmailStatus.Failed,
                  failedAt = Some(Instant.now()),
                  error = Some(error)
                )
              else k -> row
            }
          )

        def get(sourceEventId: String): IO[Option[SentEmailRow]] =
          ref.get.map(_.get(sourceEventId))

        def list(
            customerId: Option[String],
            orderId: Option[String]
        ): IO[List[SentEmailRow]] =
          ref.get.map(
            _.values
              .filter(row => customerId.forall(row.customerId.contains))
              .filter(row => orderId.forall(row.orderId.contains))
              .toList
              .sortBy(_.createdAt)
              .reverse
          )
      }
  }
}
