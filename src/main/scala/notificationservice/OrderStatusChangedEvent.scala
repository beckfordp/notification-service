package notificationservice

import io.circe.{Codec, Decoder, Encoder}
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.{Schema, Validator}

import java.time.Instant

/** The three statuses order-service publishes on `order.status-changed`, per
  * `gluon/docs/system-design.md`'s "Payload contracts". Any other wire value is
  * a decode failure, not a silently-accepted case.
  */
sealed trait OrderStatusChanged {
  def asString: String = this match {
    case OrderStatusChanged.ReservationFailed => "reservation_failed"
    case OrderStatusChanged.Confirmed         => "confirmed"
    case OrderStatusChanged.PaymentFailed     => "payment_failed"
  }
}

object OrderStatusChanged {
  case object ReservationFailed extends OrderStatusChanged
  case object Confirmed extends OrderStatusChanged
  case object PaymentFailed extends OrderStatusChanged

  def fromString(raw: String): Either[String, OrderStatusChanged] = raw match {
    case "reservation_failed" => Right(ReservationFailed)
    case "confirmed"          => Right(Confirmed)
    case "payment_failed"     => Right(PaymentFailed)
    case other                => Left(s"Unrecognized order status: '$other'")
  }

  implicit val decoder: Decoder[OrderStatusChanged] =
    Decoder.decodeString.emap(fromString)

  implicit val encoder: Encoder[OrderStatusChanged] =
    Encoder.encodeString.contramap(_.asString)

  // A plain string on the wire (matching the circe encoder above) - not a
  // derived coproduct/oneOf schema, which would misdocument this as a JSON
  // object with a discriminator. Validator.enumeration documents the fixed
  // 3-value set in the generated AsyncAPI JSON Schema ("enum": [...]),
  // mirroring order-service's own OrderStatusChangedEvent.status technique
  // (there a plain String field; here a real ADT, so the base schema itself
  // - not just a field modifier - needs to be `Schema.string`). The
  // `.encode` step is required: tapir-apispec-docs silently drops an
  // enumeration validator with no encode function rather than rendering it.
  implicit val schema: Schema[OrderStatusChanged] =
    Schema
      .string[OrderStatusChanged]
      .validate(
        Validator
          .enumeration(
            List(ReservationFailed, Confirmed, PaymentFailed)
          )
          .encode(_.asString)
      )
}

/** Local mirror of order-service's pinned `order.status-changed` payload, per
  * `gluon/docs/system-design.md`'s "Payload contracts" section - that
  * cross-repo doc, not order-service's own case class, is the source of truth.
  * No shared library between the two services; each side keeps its own copy.
  */
final case class OrderStatusChangedEvent(
    orderId: String,
    customerId: String,
    status: OrderStatusChanged,
    timestamp: Instant
)

object OrderStatusChangedEvent {
  implicit val codec: Codec[OrderStatusChangedEvent] = deriveCodec
  implicit val schema: Schema[OrderStatusChangedEvent] = Schema.derived
}
