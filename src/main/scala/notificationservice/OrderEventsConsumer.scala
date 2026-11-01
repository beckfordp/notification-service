package notificationservice

import cats.{FlatMap, MonadThrow}
import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import org.typelevel.log4cats.StructuredLogger
import purekafka.consumer.DeadLetter

/** Consumes order-service's per-outcome lifecycle events on `order-events`
  * (`OrderConfirmed`/`OrderPaymentFailed`/`OrderCancelled`, TD-15) and sends
  * the matching email. Replaces the single `OrderStatusChanged` consumer.
  *
  * Per-record handling (null skip, decode failure to DLQ, unknown types logged,
  * processing under `DeadLetter.guard`) is the shared [[EnvelopeHandler]]. Raw
  * payloads are never logged.
  */
object OrderEventsConsumer {

  val topic: String = "order-events"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId("notification-service-order-events")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  /** Exposed for tests: the email content is pure. */
  def confirmedEmail(event: OrderConfirmedEvent): (String, String) =
    (
      "Your order is confirmed",
      s"Good news - order ${event.orderId} is confirmed."
    )

  def paymentFailedEmail(event: OrderPaymentFailedEvent): (String, String) =
    (
      "We couldn't process your payment",
      s"We couldn't process your payment for order ${event.orderId}."
    )

  def cancelledEmail(event: OrderCancelledEvent): (String, String) =
    (
      "Your order was cancelled",
      s"Order ${event.orderId} was cancelled because its payment authorization expired."
    )

  private def send[F[_]: FlatMap](
      emailClient: EmailClient[F],
      logger: StructuredLogger[F],
      customerId: String,
      orderId: String,
      eventType: String,
      email: (String, String)
  ): F[Unit] =
    emailClient.send(customerId, email._1, email._2) *>
      logger.info(
        Map("order_id" -> orderId, "event_type" -> eventType)
      )(s"Sent $eventType email (order_id=$orderId)")

  /** One `order-events` record, minus the offset commit - split from [[run]] so
    * the routing is testable without a broker.
    */
  def handle[F[_]: MonadThrow](
      emailClient: EmailClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String]
  )(key: String, value: Option[String]): F[Unit] =
    EnvelopeHandler.handle[F](
      topic,
      List(
        EnvelopeRoute.of[F, OrderConfirmedEvent]("OrderConfirmed") { e =>
          send(
            emailClient,
            logger,
            e.customerId,
            e.orderId,
            "OrderConfirmed",
            confirmedEmail(e)
          )
        },
        EnvelopeRoute.of[F, OrderPaymentFailedEvent]("OrderPaymentFailed") {
          e =>
            send(
              emailClient,
              logger,
              e.customerId,
              e.orderId,
              "OrderPaymentFailed",
              paymentFailedEmail(e)
            )
        },
        EnvelopeRoute.of[F, OrderCancelledEvent]("OrderCancelled") { e =>
          send(
            emailClient,
            logger,
            e.customerId,
            e.orderId,
            "OrderCancelled",
            cancelledEmail(e)
          )
        }
      ),
      logger,
      deadLetter
    )(key, value)

  def run[F[_]: Async](
      config: KafkaConfig,
      emailClient: EmailClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalMap { committable =>
        handle(emailClient, logger, deadLetter)(
          committable.record.key.getOrElse(""),
          committable.record.value
        ) *> committable.offset.commit
      }
}
