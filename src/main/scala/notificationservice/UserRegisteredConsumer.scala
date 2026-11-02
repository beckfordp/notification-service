package notificationservice

import cats.{FlatMap, MonadThrow}
import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import org.typelevel.log4cats.StructuredLogger
import purekafka.consumer.DeadLetter

/** Consumes `UserRegistered` envelopes on `user-events` (US-16.4) and sends the
  * welcome email to the address in the event. Same conventions as
  * `OrderEventsConsumer`: null-safe deserializers and the shared
  * [[EnvelopeHandler]] rules (malformed envelope/payload to `user-events-dlq`,
  * unknown `eventType`s logged and skipped, the send under `DeadLetter.guard`).
  * A redelivered event sends a second welcome email - accepted, same as the
  * order-events consumer (no datastore).
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

  /** One `user-events` record, minus the offset commit - split from [[run]] so
    * the routing is testable without a broker. Per-record rules:
    * [[EnvelopeHandler]].
    */
  def handle[F[_]: MonadThrow](
      emailClient: EmailClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String]
  )(key: String, value: Option[String]): F[Unit] =
    EnvelopeHandler.handle[F](
      topic,
      List(
        EnvelopeRoute.of[F, UserRegisteredEvent](eventType) {
          (eventId, event) =>
            sendWelcome(emailClient, logger, eventId, event)
        }
      ),
      logger,
      deadLetter
    )(key, value)

  private def sendWelcome[F[_]: FlatMap](
      emailClient: EmailClient[F],
      logger: StructuredLogger[F],
      eventId: String,
      event: UserRegisteredEvent
  ): F[Unit] = {
    val (subject, body) = emailFor(event)
    emailClient.send(
      EmailMessage(
        sourceEventId = eventId,
        eventType = eventType,
        reason = None,
        recipientType = RecipientType.Email,
        recipientKey = event.email,
        customerId = Some(event.userId),
        orderId = None,
        subject = subject,
        body = body
      )
    ) *>
      logger.info(Map("user_id" -> event.userId))(
        s"Sent UserRegistered welcome email (user_id=${event.userId})"
      )
  }

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
