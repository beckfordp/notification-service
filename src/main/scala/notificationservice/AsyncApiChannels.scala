package notificationservice

import purekafka.docs.{
  AsyncApiChannel,
  AsyncApiChannelsProvider,
  ChannelDirection
}

/** Registers notification-service's Kafka event contracts for AsyncAPI
  * documentation (ADR 0010's AsyncAPI follow-up). It consumes the three
  * per-outcome order events (`OrderConfirmed`/`OrderPaymentFailed`/
  * `OrderCancelled`) on `order-events` and `UserRegistered` on `user-events` -
  * `Subscribe` only. No producer side at all - this service is a pure
  * Kafka-to-email relay, no outbox, no direct publish call.
  *
  * Implements `AsyncApiChannelsProvider` (not just a plain `List`) so the
  * sbt-purekafka-docs plugin's `AsyncApiSpecWriter` can load this singleton
  * reflectively by class name, configured via `build.sbt`'s
  * `asyncApiChannelsClass`.
  */
object AsyncApiChannels extends AsyncApiChannelsProvider {

  val channels: List[AsyncApiChannel[?]] = List(
    AsyncApiChannel[OrderConfirmedEvent](
      topic = OrderEventsConsumer.topic,
      eventType = "OrderConfirmed",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[OrderPaymentFailedEvent](
      topic = OrderEventsConsumer.topic,
      eventType = "OrderPaymentFailed",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[OrderCancelledEvent](
      topic = OrderEventsConsumer.topic,
      eventType = "OrderCancelled",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[UserRegisteredEvent](
      topic = UserRegisteredConsumer.topic,
      eventType = "UserRegistered",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[DispatchConfirmedEvent](
      topic = FulfilmentEventConsumer.topic,
      eventType = "DispatchConfirmed",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[DispatchFailedEvent](
      topic = FulfilmentEventConsumer.topic,
      eventType = "DispatchFailed",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[PaymentCapturedEvent](
      topic = PaymentEventConsumer.topic,
      eventType = "PaymentCaptured",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[PaymentFailedEvent](
      topic = PaymentEventConsumer.topic,
      eventType = "PaymentFailed",
      direction = ChannelDirection.Subscribe
    )
  )
}
