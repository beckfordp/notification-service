package notificationservice

import munit.FunSuite
import purekafka.docs.ChannelDirection

class AsyncApiChannelsSuite extends FunSuite {

  test(
    "AsyncApiChannels.channels registers all consumed event types as Subscribe"
  ) {
    val channels = AsyncApiChannels.channels
      .map(c => (c.topic, c.eventType, c.direction))
      .toSet

    assertEquals(
      channels,
      Set(
        (
          "order-events",
          "OrderStatusChanged",
          ChannelDirection.Subscribe: ChannelDirection
        ),
        (
          "user-events",
          "UserRegistered",
          ChannelDirection.Subscribe: ChannelDirection
        ),
        (
          "fulfilment-events",
          "DispatchConfirmed",
          ChannelDirection.Subscribe: ChannelDirection
        ),
        (
          "fulfilment-events",
          "DispatchFailed",
          ChannelDirection.Subscribe: ChannelDirection
        ),
        (
          "payment-events",
          "PaymentCaptured",
          ChannelDirection.Subscribe: ChannelDirection
        ),
        (
          "payment-events",
          "PaymentFailed",
          ChannelDirection.Subscribe: ChannelDirection
        )
      )
    )
  }
}
