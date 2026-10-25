package notificationservice

import munit.FunSuite
import purekafka.docs.{AsyncApiChannel, ChannelDirection}

class AsyncApiChannelsSuite extends FunSuite {

  test(
    "AsyncApiChannel constructs for notification-service's one registered event type"
  ) {
    val orderStatusChanged =
      AsyncApiChannel[OrderStatusChangedEvent](
        topic = "order-events",
        eventType = "OrderStatusChanged"
      )

    assertEquals(orderStatusChanged.topic, "order-events")
  }

  test(
    "AsyncApiChannels.channels registers OrderStatusChanged on order-events, as Subscribe"
  ) {
    assertEquals(AsyncApiChannels.channels.length, 1)

    val channel = AsyncApiChannels.channels.head
    assertEquals(channel.topic, "order-events")
    assertEquals(channel.eventType, "OrderStatusChanged")
    assertEquals(
      channel.direction,
      ChannelDirection.Subscribe: ChannelDirection,
      "expected OrderStatusChanged to be Subscribe-only - notification-service only consumes it, no producer side at all"
    )
  }
}
