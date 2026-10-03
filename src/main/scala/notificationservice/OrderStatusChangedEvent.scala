package notificationservice

import io.circe.{Codec, Decoder, Encoder}
import io.circe.generic.semiauto.deriveCodec

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
}
