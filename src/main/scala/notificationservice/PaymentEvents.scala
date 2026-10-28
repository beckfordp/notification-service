package notificationservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema

import java.time.Instant

/** Local mirrors of payment-service's pinned `payment-events` payloads, per
  * `gluon/docs/system-design.md`'s "Payload contracts" section - that
  * cross-repo doc, not payment-service's own case classes, is the source of
  * truth. US-19.4: both capture outcomes trigger a customer email.
  */

/** Published once per consumed `DispatchConfirmed` (US-19.1) - the point money
  * actually moves.
  */
final case class PaymentCapturedEvent(
    orderId: String,
    amountCents: Int,
    timestamp: Instant
)

object PaymentCapturedEvent {
  implicit val codec: Codec[PaymentCapturedEvent] = deriveCodec
  implicit val schema: Schema[PaymentCapturedEvent] = Schema.derived
}

/** Published on capture decline only, with `reason = "capture_failed"`
  * (US-19.1). `paymentId` is payment-service's own record id, used here only
  * for log correlation.
  */
final case class PaymentFailedEvent(
    orderId: String,
    paymentId: String,
    amountCents: Int,
    reason: String,
    timestamp: Instant
)

object PaymentFailedEvent {
  implicit val codec: Codec[PaymentFailedEvent] = deriveCodec
  implicit val schema: Schema[PaymentFailedEvent] = Schema.derived
}
