// SPDX-License-Identifier: MIT

package notificationservice

import munit.FunSuite
import purekafka.docs.{AsyncApiDoc, ChannelDirection}

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
          "OrderConfirmed",
          ChannelDirection.Subscribe: ChannelDirection
        ),
        (
          "order-events",
          "OrderPaymentFailed",
          ChannelDirection.Subscribe: ChannelDirection
        ),
        (
          "order-events",
          "OrderCancelled",
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

  test(
    "every consumed event's generated AsyncAPI message carries a description (copied from the producer's registry artifact)"
  ) {
    val doc = AsyncApiDoc.generate(
      "test",
      "1.0",
      AsyncApiChannels.channels
    )
    val consumed = AsyncApiChannels.channels.filter(
      _.direction == ChannelDirection.Subscribe
    )
    assert(consumed.nonEmpty)
    for (c <- consumed)
      assert(
        doc.hcursor
          .downField("channels")
          .downField(c.topic)
          .downField("messages")
          .downField(c.eventType)
          .downField("payload")
          .downField("description")
          .as[String]
          .isRight,
        s"${c.topic}/${c.eventType} has no description"
      )
  }
}
