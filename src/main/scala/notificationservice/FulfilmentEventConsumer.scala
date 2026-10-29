package notificationservice

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.parser.decode
import org.typelevel.log4cats.StructuredLogger
import purekafka.consumer.DeadLetter

/** Consumes `DispatchConfirmed`/`DispatchFailed` on `fulfilment-events`
  * (US-17.5/18.4) and sends the shipped / dispatch-failed email. Fulfilment
  * events carry no email or customerId, so the stub recipient is the orderId —
  * real recipient identity is a later feature. Duplicates send duplicates.
  */
object FulfilmentEventConsumer {

  val topic: String = "fulfilment-events"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId("notification-service-fulfilment-events")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  /** Exposed for tests: the email content is pure. */
  def shippedEmail(event: DispatchConfirmedEvent): (String, String) =
    (
      "Your order has shipped",
      s"Your order ${event.orderId} has shipped. Tracking number: ${event.trackingNo}."
    )

  def failedEmail(event: DispatchFailedEvent): (String, String) =
    (
      "We couldn't ship your order",
      s"Order ${event.orderId} could not be dispatched: ${event.reason}."
    )

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
        def send(
            orderId: String,
            email: (String, String),
            outcome: String
        ): F[Unit] =
          emailClient.send(orderId, email._1, email._2) *>
            logger.info(Map("order_id" -> orderId, "outcome" -> outcome))(
              s"Sent fulfilment email (order_id=$orderId, outcome=$outcome)"
            )

        def handleConfirmed(event: DispatchConfirmedEvent): F[Unit] =
          send(event.orderId, shippedEmail(event), "shipped")

        def handleFailed(event: DispatchFailedEvent): F[Unit] =
          send(event.orderId, failedEmail(event), "dispatch_failed")

        val key = committable.record.key.getOrElse("")

        val handled: F[Unit] = committable.record.value match {
          case None =>
            logger.error(Map.empty)(
              "Received fulfilment-events record with a null value - skipping"
            )
          case Some(raw) =>
            decode[EventEnvelope](raw) match {
              case Left(error) =>
                logger.error(
                  Map("topic" -> topic, "bytes" -> raw.length.toString),
                  error
                )(
                  "Failed to decode fulfilment-events envelope"
                )
              case Right(envelope) =>
                envelope.eventType match {
                  case "DispatchConfirmed" =>
                    envelope.payload.as[DispatchConfirmedEvent] match {
                      case Left(error) =>
                        logger.error(
                          Map("topic" -> topic, "bytes" -> raw.length.toString),
                          error
                        )(
                          "Failed to decode DispatchConfirmed payload"
                        )
                      case Right(event) =>
                        deadLetter.guard(topic, key, raw)(
                          handleConfirmed(event)
                        )
                    }
                  case "DispatchFailed" =>
                    envelope.payload.as[DispatchFailedEvent] match {
                      case Left(error) =>
                        logger.error(
                          Map("topic" -> topic, "bytes" -> raw.length.toString),
                          error
                        )(
                          "Failed to decode DispatchFailed payload"
                        )
                      case Right(event) =>
                        deadLetter.guard(topic, key, raw)(handleFailed(event))
                    }
                  case other =>
                    logger.info(
                      Map("topic" -> topic, "event_type" -> other)
                    )("Ignoring unrelated fulfilment-events event type")
                }
            }
        }
        handled *> committable.offset.commit
      }
}
