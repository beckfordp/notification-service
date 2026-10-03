package notificationservice

import io.circe.parser.decode
import io.circe.syntax._
import munit.CatsEffectSuite

import java.time.Instant

class OrderStatusChangedEventSuite extends CatsEffectSuite {

  private def jsonFor(status: String): String =
    s"""{"orderId":"order-1","customerId":"cust-1","status":"$status","timestamp":"2026-01-01T00:00:00Z"}"""

  test("decodes reservation_failed") {
    val result = decode[OrderStatusChangedEvent](jsonFor("reservation_failed"))
    assertEquals(
      result.map(_.status),
      Right(OrderStatusChanged.ReservationFailed)
    )
  }

  test("decodes confirmed") {
    val result = decode[OrderStatusChangedEvent](jsonFor("confirmed"))
    assertEquals(result.map(_.status), Right(OrderStatusChanged.Confirmed))
  }

  test("decodes payment_failed") {
    val result = decode[OrderStatusChangedEvent](jsonFor("payment_failed"))
    assertEquals(result.map(_.status), Right(OrderStatusChanged.PaymentFailed))
  }

  test("fails to decode an unrecognized status") {
    val result = decode[OrderStatusChangedEvent](jsonFor("pending"))
    assert(result.isLeft, s"expected a decode failure for 'pending', got $result")
  }

  test("round-trips through encode/decode") {
    val event = OrderStatusChangedEvent(
      orderId = "order-1",
      customerId = "cust-1",
      status = OrderStatusChanged.Confirmed,
      timestamp = Instant.parse("2026-01-01T00:00:00Z")
    )
    assertEquals(
      decode[OrderStatusChangedEvent](event.asJson.noSpaces),
      Right(event)
    )
  }
}
