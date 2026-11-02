package notificationservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

import java.time.Instant

/** Local mirrors of order-service's per-outcome order lifecycle events, pinned
  * in `gluon/docs/system-design.md`'s "Payload contracts" section - that
  * cross-repo doc, not these case classes, is the source of truth. Replaces the
  * single `OrderStatusChanged` event in TD-15; dispatch outcomes stay on
  * `fulfilment-events`.
  *
  * Descriptions/examples are copied from the producer's artifact in gluon's
  * `registry/` (TD-19); nothing checks they stay current.
  */
@description(
  "Order confirmed: order-service consumed PaymentCaptured and moved the order to confirmed."
)
final case class OrderConfirmedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Customer who placed the order.")
    @encodedExample("7c9e6679-7425-40de-944b-e07fc1f90ae7")
    customerId: String,
    @description("When the order was confirmed (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
    timestamp: Instant
)

object OrderConfirmedEvent {
  implicit val codec: Codec[OrderConfirmedEvent] = deriveCodec
  implicit val schema: Schema[OrderConfirmedEvent] = Schema.derived
}

@description(
  "Payment capture failed: order-service consumed PaymentFailed and moved the order to payment_failed."
)
final case class OrderPaymentFailedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Customer who placed the order.")
    @encodedExample("7c9e6679-7425-40de-944b-e07fc1f90ae7")
    customerId: String,
    @description("payment-service's capture-failure reason.")
    @encodedExample("capture_failed")
    reason: String,
    @description("When the order moved to payment_failed (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
    timestamp: Instant
)

object OrderPaymentFailedEvent {
  implicit val codec: Codec[OrderPaymentFailedEvent] = deriveCodec
  implicit val schema: Schema[OrderPaymentFailedEvent] = Schema.derived
}

/** Who triggered a cancellation (US-22, `OrderCancelled` v3). */
@description("Cancellation actor: customer, operator or system.")
final case class OrderActor(
    @description("Actor type: customer | operator | system.")
    @encodedExample("operator")
    `type`: String,
    @description("Actor id: the customerId, operator id, or system component.")
    @encodedExample("dev-operator")
    id: String
)

object OrderActor {
  implicit val codec: Codec[OrderActor] = deriveCodec
  implicit val schema: Schema[OrderActor] =
    Schema.derived[OrderActor].copy(name = None)
}

@description(
  "Order cancelled: a customer/operator cancel or a consumed PaymentAuthorizationExpired moved the order to cancelled."
)
final case class OrderCancelledEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Customer who placed the order.")
    @encodedExample("7c9e6679-7425-40de-944b-e07fc1f90ae7")
    customerId: String,
    @description("When the order was cancelled (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
    timestamp: Instant,
    @description(
      "Cancellation trigger: customer | auth_expired | stale | operator."
    )
    @encodedExample("customer")
    reason: Option[String] = None,
    @description("Who cancelled; absent on pre-v3 messages.")
    actor: Option[OrderActor] = None
)

object OrderCancelledEvent {
  implicit val codec: Codec[OrderCancelledEvent] = deriveCodec
  implicit val schema: Schema[OrderCancelledEvent] = Schema.derived
}
