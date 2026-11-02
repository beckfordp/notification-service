package notificationservice

import cats.effect.{IO, Ref}
import io.circe.Json
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.testing.StructuredTestingLogger

import java.time.Instant

/** Assertions for ADM-2's recipient mapping: every event type must carry the
  * envelope eventId plus the right recipient/customer/order attribution.
  */
class SentEmailMappingSuite extends CatsEffectSuite {

  private val ts = Instant.parse("2026-10-31T10:15:30Z")

  private def envelope(eventType: String, payload: Json): String =
    EventEnvelope(
      "event-42",
      eventType,
      ts,
      "test",
      "c1",
      payload
    ).asJson.noSpaces

  private def recordingClient
      : IO[(EmailClient[IO], Ref[IO, List[EmailMessage]])] =
    Ref.of[IO, List[EmailMessage]](Nil).map { ref =>
      (
        new EmailClient[IO] {
          def send(message: EmailMessage): IO[EmailSendOutcome] =
            ref.update(_ :+ message).map(_ => EmailSendOutcome.Sent)
        },
        ref
      )
    }

  private def messageFor(
      handle: (
          EmailClient[IO],
          org.typelevel.log4cats.StructuredLogger[IO],
          purekafka.consumer.DeadLetter[IO, String, String]
      ) => (String, Option[String]) => IO[Unit],
      eventType: String,
      payload: Json
  ): IO[EmailMessage] =
    for {
      recording <- recordingClient
      (client, messages) = recording
      dlq <- TestDeadLetter.create
      logger = StructuredTestingLogger.impl[IO]()
      _ <- handle(client, logger, dlq.deadLetter)(
        "k",
        Some(envelope(eventType, payload))
      )
      sent <- messages.get
    } yield {
      assertEquals(sent.size, 1, s"expected one email for $eventType")
      sent.head
    }

  private val order = OrderEventsConsumer.handle[IO]
  private val user = UserRegisteredConsumer.handle[IO]
  private val fulfilment = FulfilmentEventConsumer.handle[IO]
  private val payment = PaymentEventConsumer.handle[IO]

  test("OrderConfirmed maps to the customer") {
    messageFor(
      order,
      "OrderConfirmed",
      OrderConfirmedEvent("o1", "c1", ts).asJson
    ).map { message =>
      assertEquals(message.sourceEventId, "event-42")
      assertEquals(message.eventType, "OrderConfirmed")
      assertEquals(message.recipientType, RecipientType.Customer)
      assertEquals(message.recipientKey, "c1")
      assertEquals(message.customerId, Some("c1"))
      assertEquals(message.orderId, Some("o1"))
      assertEquals(message.reason, None)
    }
  }

  test("OrderPaymentFailed maps to the customer and keeps the reason") {
    messageFor(
      order,
      "OrderPaymentFailed",
      OrderPaymentFailedEvent("o1", "c1", "capture_failed", ts).asJson
    ).map { message =>
      assertEquals(message.recipientType, RecipientType.Customer)
      assertEquals(message.recipientKey, "c1")
      assertEquals(message.customerId, Some("c1"))
      assertEquals(message.orderId, Some("o1"))
      assertEquals(message.reason, Some("capture_failed"))
    }
  }

  test(
    "OrderCancelled maps to the customer and keeps the cancellation reason"
  ) {
    messageFor(
      order,
      "OrderCancelled",
      OrderCancelledEvent("o1", "c1", ts, Some("auth_expired")).asJson
    ).map { message =>
      assertEquals(message.recipientType, RecipientType.Customer)
      assertEquals(message.recipientKey, "c1")
      assertEquals(message.customerId, Some("c1"))
      assertEquals(message.orderId, Some("o1"))
      assertEquals(message.reason, Some("auth_expired"))
    }
  }

  test("PaymentCaptured maps to the order") {
    messageFor(
      payment,
      "PaymentCaptured",
      PaymentCapturedEvent("o1", 4999, ts).asJson
    ).map { message =>
      assertEquals(message.recipientType, RecipientType.Order)
      assertEquals(message.recipientKey, "o1")
      assertEquals(message.customerId, None)
      assertEquals(message.orderId, Some("o1"))
      assertEquals(message.reason, None)
    }
  }

  test("PaymentFailed maps to the order and keeps the reason") {
    messageFor(
      payment,
      "PaymentFailed",
      PaymentFailedEvent("o1", "p1", 4999, "capture_failed", ts).asJson
    ).map { message =>
      assertEquals(message.recipientType, RecipientType.Order)
      assertEquals(message.recipientKey, "o1")
      assertEquals(message.customerId, None)
      assertEquals(message.orderId, Some("o1"))
      assertEquals(message.reason, Some("capture_failed"))
    }
  }

  test("DispatchConfirmed maps to the order") {
    messageFor(
      fulfilment,
      "DispatchConfirmed",
      DispatchConfirmedEvent("o1", "TRK-1", Nil).asJson
    ).map { message =>
      assertEquals(message.recipientType, RecipientType.Order)
      assertEquals(message.recipientKey, "o1")
      assertEquals(message.orderId, Some("o1"))
      assertEquals(message.reason, None)
    }
  }

  test("DispatchFailed maps to the order and keeps the reason") {
    messageFor(
      fulfilment,
      "DispatchFailed",
      DispatchFailedEvent("o1", "carrier_unavailable").asJson
    ).map { message =>
      assertEquals(message.recipientType, RecipientType.Order)
      assertEquals(message.recipientKey, "o1")
      assertEquals(message.orderId, Some("o1"))
      assertEquals(message.reason, Some("carrier_unavailable"))
    }
  }

  test("UserRegistered maps to the email address and the user as customer") {
    messageFor(
      user,
      "UserRegistered",
      UserRegisteredEvent("u1", "jane@example.com").asJson
    ).map { message =>
      assertEquals(message.recipientType, RecipientType.Email)
      assertEquals(message.recipientKey, "jane@example.com")
      assertEquals(message.customerId, Some("u1"))
      assertEquals(message.orderId, None)
      assertEquals(message.reason, None)
    }
  }
}
