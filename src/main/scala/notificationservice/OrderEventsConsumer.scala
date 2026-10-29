package notificationservice

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.parser.decode
import org.typelevel.log4cats.StructuredLogger
import purekafka.consumer.DeadLetter

/** Consumes order-service's per-outcome lifecycle events on `order-events`
  * (`OrderConfirmed`/`OrderPaymentFailed`/`OrderCancelled`, TD-15) and sends
  * the matching email. Replaces the single `OrderStatusChanged` consumer.
  *
  * A decode failure (null value, envelope, or payload) is logged and the offset
  * still committed - the existing accepted gap, same as the other consumers.
  * Only processing failures go through `DeadLetter.guard` (TD-2.4, ADR 0010).
  * Unknown event types on the topic are logged, not silently swallowed. Raw
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

  private def send[F[_]: Async](
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

  private def handleConfirmed[F[_]: Async](
      emailClient: EmailClient[F],
      logger: StructuredLogger[F],
      event: OrderConfirmedEvent
  ): F[Unit] =
    send(
      emailClient,
      logger,
      event.customerId,
      event.orderId,
      "OrderConfirmed",
      confirmedEmail(event)
    )

  private def handlePaymentFailed[F[_]: Async](
      emailClient: EmailClient[F],
      logger: StructuredLogger[F],
      event: OrderPaymentFailedEvent
  ): F[Unit] =
    send(
      emailClient,
      logger,
      event.customerId,
      event.orderId,
      "OrderPaymentFailed",
      paymentFailedEmail(event)
    )

  private def handleCancelled[F[_]: Async](
      emailClient: EmailClient[F],
      logger: StructuredLogger[F],
      event: OrderCancelledEvent
  ): F[Unit] =
    send(
      emailClient,
      logger,
      event.customerId,
      event.orderId,
      "OrderCancelled",
      cancelledEmail(event)
    )

  private def decodeFailureLog(
      raw: String
  ): Map[String, String] =
    Map("topic" -> topic, "bytes" -> raw.length.toString)

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
        val key = committable.record.key.getOrElse("")

        val handled: F[Unit] = committable.record.value match {
          case None =>
            logger.error(Map.empty)(
              "Received order-events record with a null value - skipping"
            )
          case Some(raw) =>
            decode[EventEnvelope](raw) match {
              case Left(error) =>
                logger.error(
                  decodeFailureLog(raw),
                  error
                )("Failed to decode order-events envelope")
              case Right(envelope) =>
                envelope.eventType match {
                  case "OrderConfirmed" =>
                    envelope.payload.as[OrderConfirmedEvent] match {
                      case Left(error) =>
                        logger.error(
                          decodeFailureLog(raw),
                          error
                        )("Failed to decode OrderConfirmed payload")
                      case Right(event) =>
                        deadLetter.guard(topic, key, raw)(
                          handleConfirmed(emailClient, logger, event)
                        )
                    }
                  case "OrderPaymentFailed" =>
                    envelope.payload.as[OrderPaymentFailedEvent] match {
                      case Left(error) =>
                        logger.error(
                          decodeFailureLog(raw),
                          error
                        )("Failed to decode OrderPaymentFailed payload")
                      case Right(event) =>
                        deadLetter.guard(topic, key, raw)(
                          handlePaymentFailed(emailClient, logger, event)
                        )
                    }
                  case "OrderCancelled" =>
                    envelope.payload.as[OrderCancelledEvent] match {
                      case Left(error) =>
                        logger.error(
                          decodeFailureLog(raw),
                          error
                        )("Failed to decode OrderCancelled payload")
                      case Right(event) =>
                        deadLetter.guard(topic, key, raw)(
                          handleCancelled(emailClient, logger, event)
                        )
                    }
                  case other =>
                    logger.info(
                      Map("topic" -> topic, "event_type" -> other)
                    )("Ignoring unrelated order-events event type")
                }
            }
        }
        handled *> committable.offset.commit
      }
}
