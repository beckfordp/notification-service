package notificationservice

import io.circe.parser.decode
import io.circe.syntax._
import munit.CatsEffectSuite

import java.time.Instant

class OrderEventsSuite extends CatsEffectSuite {

  private val at = Instant.parse("2026-01-01T00:00:00Z")

  test("decodes OrderConfirmed") {
    val json =
      """{"orderId":"order-1","customerId":"cust-1","timestamp":"2026-01-01T00:00:00Z"}"""
    assertEquals(
      decode[OrderConfirmedEvent](json),
      Right(OrderConfirmedEvent("order-1", "cust-1", at))
    )
  }

  test("decodes OrderPaymentFailed with its reason") {
    val json =
      """{"orderId":"order-1","customerId":"cust-1","reason":"capture_failed","timestamp":"2026-01-01T00:00:00Z"}"""
    assertEquals(
      decode[OrderPaymentFailedEvent](json),
      Right(
        OrderPaymentFailedEvent("order-1", "cust-1", "capture_failed", at)
      )
    )
  }

  test("decodes OrderCancelled") {
    val json =
      """{"orderId":"order-1","customerId":"cust-1","timestamp":"2026-01-01T00:00:00Z"}"""
    assertEquals(
      decode[OrderCancelledEvent](json),
      Right(OrderCancelledEvent("order-1", "cust-1", at))
    )
  }

  test("round-trips each event through encode/decode") {
    val confirmed = OrderConfirmedEvent("order-1", "cust-1", at)
    val paymentFailed =
      OrderPaymentFailedEvent("order-2", "cust-2", "capture_failed", at)
    val cancelled = OrderCancelledEvent("order-3", "cust-3", at)

    assertEquals(
      decode[OrderConfirmedEvent](confirmed.asJson.noSpaces),
      Right(confirmed)
    )
    assertEquals(
      decode[OrderPaymentFailedEvent](paymentFailed.asJson.noSpaces),
      Right(paymentFailed)
    )
    assertEquals(
      decode[OrderCancelledEvent](cancelled.asJson.noSpaces),
      Right(cancelled)
    )
  }
}
