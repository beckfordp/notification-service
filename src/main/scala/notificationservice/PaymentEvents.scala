// SPDX-License-Identifier: MIT

package notificationservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

import java.time.Instant

/** Local mirrors of payment-service's pinned `payment-events` payloads, per
  * `gluon/docs/system-design.md`'s "Payload contracts" section - that
  * cross-repo doc, not payment-service's own case classes, is the source of
  * truth. US-19.4: both capture outcomes trigger a customer email.
  */

/** Published once per consumed `DispatchConfirmed` (US-19.1) - the point money
  * actually moves.
  *
  * Descriptions/examples are copied from the producer's artifact in gluon's
  * `registry/` (TD-19); nothing checks they stay current.
  */
@description(
  "Money moved: capture succeeded after DispatchConfirmed. Published once per order."
)
final case class PaymentCapturedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Amount captured, in cents.")
    @encodedExample(4999)
    amountCents: Int,
    @description("When the capture happened (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
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
@description(
  "Capture declined after DispatchConfirmed. Authorize declines are synchronous and publish nothing."
)
final case class PaymentFailedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description(
      "payment-service's own Payment id, for log/trace correlation only."
    )
    @encodedExample("9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d")
    paymentId: String,
    @description("Amount that failed to charge, in cents.")
    @encodedExample(4999)
    amountCents: Int,
    @description("Failure reason.")
    @encodedExample("capture_failed")
    reason: String,
    @description("When the capture was declined (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
    timestamp: Instant
)

object PaymentFailedEvent {
  implicit val codec: Codec[PaymentFailedEvent] = deriveCodec
  implicit val schema: Schema[PaymentFailedEvent] = Schema.derived
}
