// SPDX-License-Identifier: MIT

package notificationservice

import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger.INFO
import purekafka.consumer.DeadLetter

import java.time.Instant

/** Broker-free tests of each consumer's `handle` (one record, minus the offset
  * commit): which event types it routes, the email it sends, and who to.
  */
class ConsumerHandlersSuite extends CatsEffectSuite {

  private type Handle = (
      EmailClient[IO],
      StructuredLogger[IO],
      DeadLetter[IO, String, String]
  ) => (String, Option[String]) => IO[Unit]

  private val ts = Instant.parse("2026-10-31T10:15:30Z")

  private def envelope(eventType: String, payload: Json): String =
    EventEnvelope("e1", eventType, ts, "test", "c1", payload).asJson.noSpaces

  private final case class Result(
      emails: List[SentEmail],
      routed: List[(String, String, String)],
      logged: List[StructuredTestingLogger.LogMessage]
  )

  private def runOne(handle: Handle, raw: String): IO[Result] =
    for {
      (client, emails) <- RecordingEmailClient.create
      dlq <- TestDeadLetter.create
      logger = StructuredTestingLogger.impl[IO]()
      _ <- handle(client, logger, dlq.deadLetter)("k", Some(raw))
      sent <- emails.get
      routed <- dlq.routed.get
      logged <- logger.logged
    } yield Result(sent, routed, logged.toList)

  private def assertSent(
      handle: Handle,
      eventType: String,
      payload: Json,
      expected: SentEmail
  )(implicit loc: munit.Location): IO[Unit] =
    runOne(handle, envelope(eventType, payload)).map { r =>
      assertEquals(r.emails, List(expected))
      assertEquals(r.routed, Nil)
    }

  private val order: Handle = OrderEventsConsumer.handle[IO]
  private val user: Handle = UserRegisteredConsumer.handle[IO]
  private val fulfilment: Handle = FulfilmentEventConsumer.handle[IO]
  private val payment: Handle = PaymentEventConsumer.handle[IO]

  private val confirmed = OrderConfirmedEvent("o1", "c1", ts)
  private val paymentFailed = OrderPaymentFailedEvent("o1", "c1", "x", ts)
  private val cancelled = OrderCancelledEvent("o1", "c1", ts)

  test("order-events: OrderConfirmed emails the customer") {
    val (s, b) = OrderEventsConsumer.confirmedEmail(confirmed)
    assertSent(order, "OrderConfirmed", confirmed.asJson, SentEmail("c1", s, b))
  }

  test("order-events: OrderPaymentFailed emails the customer") {
    val (s, b) = OrderEventsConsumer.paymentFailedEmail(paymentFailed)
    assertSent(
      order,
      "OrderPaymentFailed",
      paymentFailed.asJson,
      SentEmail("c1", s, b)
    )
  }

  test("order-events: OrderCancelled emails the customer") {
    val (s, b) = OrderEventsConsumer.cancelledEmail(cancelled)
    assertSent(order, "OrderCancelled", cancelled.asJson, SentEmail("c1", s, b))
  }

  test("order-events: the cancellation actor is logged (US-22)") {
    val event = OrderCancelledEvent(
      "o1",
      "c1",
      ts,
      Some("operator"),
      Some(OrderActor("operator", "dev-operator"))
    )
    runOne(order, envelope("OrderCancelled", event.asJson)).map { result =>
      assert(
        result.logged.exists(m =>
          m.ctx.get("actor_type").contains("operator") &&
            m.ctx.get("actor_id").contains("dev-operator")
        ),
        result.logged
      )
      assertEquals(result.emails.map(_.to), List("c1"))
    }
  }

  test("order-events: cancellation copy distinguishes the reason (US-9.9)") {
    val reasonCopy = List(
      Some("customer") -> "at your request",
      Some("auth_expired") -> "payment authorization expired",
      Some("stale") -> "could not be completed in time",
      Some("operator") -> "support team",
      Some("something_else") -> "Order o1 was cancelled.",
      None -> "Order o1 was cancelled."
    )
    for ((reason, expected) <- reasonCopy) {
      val event = OrderCancelledEvent("o1", "c1", ts, reason)
      val (_, body) = OrderEventsConsumer.cancelledEmail(event)
      assert(
        body.contains(expected),
        s"reason=$reason body='$body' did not contain '$expected'"
      )
    }
  }

  test("user-events: UserRegistered emails the new user's address") {
    val event = UserRegisteredEvent("u1", "jane@example.com")
    val (s, b) = UserRegisteredConsumer.emailFor(event)
    assertSent(
      user,
      "UserRegistered",
      event.asJson,
      SentEmail("jane@example.com", s, b)
    )
  }

  test(
    "fulfilment-events: DispatchConfirmed and DispatchFailed email the order"
  ) {
    val ok = DispatchConfirmedEvent("o1", "TRK-1", Nil)
    val ko = DispatchFailedEvent("o2", "carrier_unavailable")
    val (s1, b1) = FulfilmentEventConsumer.shippedEmail(ok)
    val (s2, b2) = FulfilmentEventConsumer.failedEmail(ko)
    assertSent(
      fulfilment,
      "DispatchConfirmed",
      ok.asJson,
      SentEmail("o1", s1, b1)
    ) *>
      assertSent(
        fulfilment,
        "DispatchFailed",
        ko.asJson,
        SentEmail("o2", s2, b2)
      )
  }

  test("payment-events: PaymentCaptured and PaymentFailed email the order") {
    val ok = PaymentCapturedEvent("o1", 1234, ts)
    val ko = PaymentFailedEvent("o2", "p1", 500, "capture_failed", ts)
    val (s1, b1) = PaymentEventConsumer.receiptEmail(ok)
    val (s2, b2) = PaymentEventConsumer.failedEmail(ko)
    assertSent(
      payment,
      "PaymentCaptured",
      ok.asJson,
      SentEmail("o1", s1, b1)
    ) *>
      assertSent(payment, "PaymentFailed", ko.asJson, SentEmail("o2", s2, b2))
  }

  test("every consumer logs unrelated event types and sends nothing") {
    List(order, user, fulfilment, payment).traverse_ { handle =>
      runOne(handle, envelope("SomethingElse", Json.obj())).map { r =>
        assertEquals(r.emails, Nil)
        assertEquals(r.routed, Nil)
        assert(
          r.logged
            .collect { case m: INFO => m }
            .exists(
              _.ctx.get("event_type").contains("SomethingElse")
            ),
          r.logged
        )
      }
    }
  }

  test("every consumer dead-letters a malformed known payload on its topic") {
    List(
      (order, OrderEventsConsumer.topic, "OrderConfirmed"),
      (user, UserRegisteredConsumer.topic, "UserRegistered"),
      (fulfilment, FulfilmentEventConsumer.topic, "DispatchFailed"),
      (payment, PaymentEventConsumer.topic, "PaymentCaptured")
    ).traverse_ { case (handle, topic, eventType) =>
      val raw = envelope(eventType, Json.obj())
      runOne(handle, raw).map { r =>
        assertEquals(r.emails, Nil)
        assertEquals(r.routed, List((topic, "k", raw)))
      }
    }
  }
}
