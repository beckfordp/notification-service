package notificationservice

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.parser.decode
import org.typelevel.log4cats.StructuredLogger
import purekafka.consumer.DeadLetter

/** Consumes `UserRegistered` envelopes on `user-events` (US-16.4) and sends the
  * welcome email to the address in the event. Same conventions as
  * `OrderEventsConsumer`: null-safe deserializers, malformed
  * envelope/payload logged and dropped (offset still committed), unknown
  * `eventType`s logged and skipped, and only processing (the email send) wrapped
  * in `DeadLetter.guard` (bounded retry, then `user-events-dlq`). A redelivered
  * event sends a second welcome email - accepted, same as the order-events
  * consumer (no datastore).
  */
object UserRegisteredConsumer {

  val topic: String = "user-events"

  private val eventType: String = "UserRegistered"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId("notification-service-user-events")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  /** Exposed for tests: the email content is pure. */
  def emailFor(event: UserRegisteredEvent): (String, String) =
    (
      "Welcome to gshop",
      s"Welcome, ${event.email}. Your gshop account is ready."
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
        def handleEvent(event: UserRegisteredEvent): F[Unit] = {
          val (subject, body) = emailFor(event)
          emailClient.send(event.email, subject, body) *>
            logger.info(Map("user_id" -> event.userId))(
              s"Sent UserRegistered welcome email (user_id=${event.userId})"
            )
        }

        val key = committable.record.key.getOrElse("")

        val handled: F[Unit] = committable.record.value match {
          case None =>
            logger.error(Map.empty)(
              "Received user-events record with a null value - skipping"
            )
          case Some(raw) =>
            decode[EventEnvelope](raw) match {
              case Left(error) =>
                logger.error(
                  Map("topic" -> topic, "bytes" -> raw.length.toString),
                  error
                )(
                  "Failed to decode user-events envelope"
                )
              case Right(envelope) if envelope.eventType != eventType =>
                Async[F].unit
              case Right(envelope) =>
                envelope.payload.as[UserRegisteredEvent] match {
                  case Left(error) =>
                    logger.error(
                      Map("topic" -> topic, "bytes" -> raw.length.toString),
                      error
                    )(
                      "Failed to decode UserRegistered payload"
                    )
                  case Right(event) =>
                    deadLetter.guard(topic, key, raw)(handleEvent(event))
                }
            }
        }
        handled *> committable.offset.commit
      }
}
