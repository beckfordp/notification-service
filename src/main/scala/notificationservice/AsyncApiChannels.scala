package notificationservice

import purekafka.docs.{
  AsyncApiChannel,
  AsyncApiChannelsProvider,
  ChannelDirection
}

/** Registers notification-service's Kafka event contracts for AsyncAPI
  * documentation (ADR 0010's AsyncAPI follow-up). `OrderStatusChangedConsumer`
  * only ever consumes `OrderStatusChanged` on `order-events` - `Subscribe`. No
  * producer side at all - this service is a pure Kafka-to-email relay, no
  * outbox, no direct publish call.
  *
  * Implements `AsyncApiChannelsProvider` (not just a plain `List`) so the
  * sbt-purekafka-docs plugin's `AsyncApiSpecWriter` can load this singleton
  * reflectively by class name, configured via `build.sbt`'s
  * `asyncApiChannelsClass`.
  */
object AsyncApiChannels extends AsyncApiChannelsProvider {

  val channels: List[AsyncApiChannel[?]] = List(
    AsyncApiChannel[OrderStatusChangedEvent](
      topic = OrderStatusChangedConsumer.topic,
      eventType = "OrderStatusChanged",
      direction = ChannelDirection.Subscribe
    )
  )
}
