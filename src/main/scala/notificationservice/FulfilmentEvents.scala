package notificationservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema

/** Full item snapshot carried by DispatchConfirmed (US-17.5). */
final case class DispatchItem(
    sku: String,
    productName: String,
    quantity: Int,
    unitPriceCents: Int
)

object DispatchItem {
  implicit val codec: Codec[DispatchItem] = deriveCodec
  implicit val schema: Schema[DispatchItem] =
    Schema.derived[DispatchItem].copy(name = None)
}

final case class DispatchConfirmedEvent(
    orderId: String,
    trackingNo: String,
    items: List[DispatchItem]
)

object DispatchConfirmedEvent {
  implicit val codec: Codec[DispatchConfirmedEvent] = deriveCodec
  implicit val schema: Schema[DispatchConfirmedEvent] = Schema.derived
}

final case class DispatchFailedEvent(orderId: String, reason: String)

object DispatchFailedEvent {
  implicit val codec: Codec[DispatchFailedEvent] = deriveCodec
  implicit val schema: Schema[DispatchFailedEvent] = Schema.derived
}
