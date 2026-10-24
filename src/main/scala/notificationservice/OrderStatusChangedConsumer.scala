package notificationservice

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.parser.decode
import org.typelevel.log4cats.StructuredLogger

/** Consumes `OrderStatusChanged` envelopes on `order-events` (US-7.1; migrated
  * to the domain topic + envelope shape per ADR 0009): decodes the envelope,
  * then the event, and sends the matching email via `EmailClient` for each of
  * the three recognized statuses. No retry on the consume/processing side (a
  * decode failure is logged and the offset still committed) - same
  * at-least-once, commit-after-process risk profile as payment-service's own
  * `OrderReservedConsumer`. No duplicate-delivery guard: a redelivered event
  * sends a second email for the same order - acceptable for the walking
  * skeleton (no real email provider to worry about double-sending to yet).
  * `order-events` carries other event types (e.g. `OrderReserved`) this
  * consumer doesn't care about - those are silently skipped by `eventType`, not
  * logged as malformed. Stream crash recovery is no longer this consumer's own
  * concern - `purekafka`'s `StreamSupervisor` (TD-2.3, ADR 0010) handles
  * restart-with-backoff externally, wired in `KafkaConsumers.scala`.
  */
object OrderStatusChangedConsumer {

  val topic: String = "order-events"

  private val eventType: String = "OrderStatusChanged"

  // Key/value typed as Option[String] (via fs2-kafka's null-safe
  // Deserializer.option) rather than plain String: a plain String
  // deserializer throws on a null key or value, which kills this whole
  // background consumer stream silently, since Main.scala runs it via
  // `.background.use` and never observes the fiber's outcome otherwise -
  // the same gap payment-service's OrderReservedConsumer already closed,
  // baked in here from the start rather than retrofitted.
  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId("notification-service-order-events")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  private def emailFor(event: OrderStatusChangedEvent): (String, String) =
    event.status match {
      case OrderStatusChanged.ReservationFailed =>
        (
          "Your order could not be fulfilled",
          s"We're sorry, but order ${event.orderId} could not be fulfilled."
        )
      case OrderStatusChanged.Confirmed =>
        (
          "Your order is confirmed",
          s"Good news - order ${event.orderId} is confirmed."
        )
      case OrderStatusChanged.PaymentFailed =>
        (
          "We couldn't process your payment",
          s"We couldn't process your payment for order ${event.orderId}."
        )
    }

  def run[F[_]: Async](
      config: KafkaConfig,
      emailClient: EmailClient[F],
      logger: StructuredLogger[F]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalMap { committable =>
        def handleEvent(event: OrderStatusChangedEvent): F[Unit] = {
          val (subject, body) = emailFor(event)
          emailClient.send(event.customerId, subject, body) *>
            logger.info(
              Map(
                "order_id" -> event.orderId,
                "status" -> event.status.asString
              )
            )(
              s"Sent OrderStatusChanged email (order_id=${event.orderId}, status=${event.status.asString})"
            )
        }

        val handled: F[Unit] = committable.record.value match {
          case None =>
            logger.error(Map.empty)(
              "Received order-events record with a null value - skipping"
            )
          case Some(raw) =>
            decode[EventEnvelope](raw) match {
              case Left(error) =>
                logger.error(
                  Map("raw" -> raw),
                  error
                )("Failed to decode order-events envelope")
              case Right(envelope) if envelope.eventType != eventType =>
                Async[F].unit
              case Right(envelope) =>
                envelope.payload.as[OrderStatusChangedEvent] match {
                  case Left(error) =>
                    logger.error(
                      Map("raw" -> raw),
                      error
                    )("Failed to decode OrderStatusChanged payload")
                  case Right(event) => handleEvent(event)
                }
            }
        }
        handled *> committable.offset.commit
      }
}
