package notificationservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema

import java.time.Instant

/** Local mirrors of order-service's per-outcome order lifecycle events, pinned
  * in `gluon/docs/system-design.md`'s "Payload contracts" section - that
  * cross-repo doc, not these case classes, is the source of truth. Replaces the
  * single `OrderStatusChanged` event in TD-15; dispatch outcomes stay on
  * `fulfilment-events`.
  */
final case class OrderConfirmedEvent(
    orderId: String,
    customerId: String,
    timestamp: Instant
)

object OrderConfirmedEvent {
  implicit val codec: Codec[OrderConfirmedEvent] = deriveCodec
  implicit val schema: Schema[OrderConfirmedEvent] = Schema.derived
}

final case class OrderPaymentFailedEvent(
    orderId: String,
    customerId: String,
    reason: String,
    timestamp: Instant
)

object OrderPaymentFailedEvent {
  implicit val codec: Codec[OrderPaymentFailedEvent] = deriveCodec
  implicit val schema: Schema[OrderPaymentFailedEvent] = Schema.derived
}

final case class OrderCancelledEvent(
    orderId: String,
    customerId: String,
    timestamp: Instant
)

object OrderCancelledEvent {
  implicit val codec: Codec[OrderCancelledEvent] = deriveCodec
  implicit val schema: Schema[OrderCancelledEvent] = Schema.derived
}
