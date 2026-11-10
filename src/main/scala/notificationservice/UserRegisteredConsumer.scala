// SPDX-License-Identifier: MIT

package notificationservice

import cats.{FlatMap, MonadThrow}
import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.trace.Tracer
import purekafka.consumer.{DeadLetter, EventAge}
import purekafka.tracing.KafkaTracing

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

  val groupId: String = "notification-service-user-events"

  private val eventType: String = "UserRegistered"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId(groupId)
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
    emailClient
      .send(
        EmailMessage(
          sourceEventId = eventId,
          eventType = eventType,
          reason = None,
          recipientType = RecipientType.Email,
          recipientKey = event.email,
          customerId = Some(event.userId),
          orderId = None,
          subject = subject,
          body = body,
          recipientEmail = Some(event.email)
        )
      )
      .flatMap(
        EmailSendLog.log(
          logger,
          Map("user_id" -> event.userId, "event_type" -> eventType),
          eventType
        )
      )
  }

  def run[F[_]: Async](
      config: KafkaConfig,
      emailClient: EmailClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String],
      eventAge: EventAge[F],
      tracer: Tracer[F]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalTap(committable =>
        committable.record.timestamp.createTime.traverse_(
          eventAge.record(topic, groupId, _)
        )
      )
      .evalMap { committable =>
        KafkaTracing.traced(tracer)(
          committable.record.headers,
          s"$topic process",
          topic
        )(
          handle(emailClient, logger, deadLetter)(
            committable.record.key.getOrElse(""),
            committable.record.value
          )
        ) *> committable.offset.commit
      }
}
