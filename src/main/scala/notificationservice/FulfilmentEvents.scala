// SPDX-License-Identifier: MIT

package notificationservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

/** Full item snapshot carried by DispatchConfirmed (US-17.5). */
final case class DispatchItem(
    @description("Product SKU.")
    @encodedExample("SKU-0042")
    sku: String,
    @description("Product name at dispatch time.")
    @encodedExample("Espresso cup")
    productName: String,
    @description("Units dispatched.")
    @encodedExample(2)
    quantity: Int,
    @description("Unit price, in cents.")
    @encodedExample(1250)
    unitPriceCents: Int
)

object DispatchItem {
  implicit val codec: Codec[DispatchItem] = deriveCodec
  implicit val schema: Schema[DispatchItem] =
    Schema.derived[DispatchItem].copy(name = None)
}

@description(
  "Order dispatched: pick/pack/dispatch succeeded. Carries a full item snapshot so consumers need no order lookup."
)
final case class DispatchConfirmedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Carrier tracking number.")
    @encodedExample("TRK-123456789")
    trackingNo: String,
    @description("Items dispatched. Always sent.")
    items: List[DispatchItem]
)

object DispatchConfirmedEvent {
  implicit val codec: Codec[DispatchConfirmedEvent] = deriveCodec
  implicit val schema: Schema[DispatchConfirmedEvent] = Schema.derived
}

@description(
  "Dispatch failed: pick/pack/dispatch hit an error. payment-service voids the hold; order-service marks the order failed."
)
final case class DispatchFailedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Why dispatch failed.")
    @encodedExample("carrier_unavailable")
    reason: String
)

object DispatchFailedEvent {
  implicit val codec: Codec[DispatchFailedEvent] = deriveCodec
  implicit val schema: Schema[DispatchFailedEvent] = Schema.derived
}
