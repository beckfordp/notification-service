package notificationservice

import io.circe.parser.decode
import io.circe.syntax._
import munit.CatsEffectSuite

import java.time.Instant

class PaymentEventsSuite extends CatsEffectSuite {

  private val captured = PaymentCapturedEvent(
    orderId = "order-1",
    amountCents = 1234,
    timestamp = Instant.parse("2026-10-28T12:00:00Z")
  )

  private val failed = PaymentFailedEvent(
    orderId = "order-1",
    paymentId = "payment-1",
    amountCents = 1234,
    reason = "capture_failed",
    timestamp = Instant.parse("2026-10-28T12:00:00Z")
  )

  test("decodes PaymentCaptured") {
    assertEquals(
      decode[PaymentCapturedEvent](captured.asJson.noSpaces),
      Right(captured)
    )
  }

  test("decodes PaymentFailed") {
    assertEquals(
      decode[PaymentFailedEvent](failed.asJson.noSpaces),
      Right(failed)
    )
  }

  test("malformed PaymentCaptured payload fails decode") {
    assert(decode[PaymentCapturedEvent]("""{"orderId":"order-1"}""").isLeft)
  }

  test("malformed PaymentFailed payload fails decode") {
    assert(
      decode[PaymentFailedEvent](
        """{"orderId":"order-1","reason":"capture_failed"}"""
      ).isLeft
    )
  }

  test("receiptEmail names the order and formats the captured amount") {
    val (subject, body) = PaymentEventConsumer.receiptEmail(captured)

    assertEquals(subject, "Your payment was received")
    assert(body.contains("order-1"))
    assert(body.contains("$12.34"))
  }

  test("failedEmail names the order, amount and reason") {
    val (subject, body) = PaymentEventConsumer.failedEmail(failed)

    assertEquals(subject, "We couldn't take your payment")
    assert(body.contains("order-1"))
    assert(body.contains("$12.34"))
    assert(body.contains("capture_failed"))
  }

  test("amount formatting pads cents and handles whole amounts") {
    assertEquals(PaymentEventConsumer.formatAmount(5), "$0.05")
    assertEquals(PaymentEventConsumer.formatAmount(10000), "$100.00")
  }
}
