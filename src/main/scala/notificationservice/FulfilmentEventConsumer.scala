package notificationservice

import cats.{FlatMap, MonadThrow}
import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
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

  /** One `fulfilment-events` record, minus the offset commit - split from
    * [[run]] so the routing is testable without a broker. Per-record rules:
    * [[EnvelopeHandler]].
    */
  def handle[F[_]: MonadThrow](
      emailClient: EmailClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String]
  )(key: String, value: Option[String]): F[Unit] = {
    def send(
        eventId: String,
        eventType: String,
        orderId: String,
        email: (String, String),
        reason: Option[String]
    ) =
      sendEmail(
        emailClient,
        logger,
        eventId,
        eventType,
        orderId,
        email,
        reason
      )
    EnvelopeHandler.handle[F](
      topic,
      List(
        EnvelopeRoute.of[F, DispatchConfirmedEvent]("DispatchConfirmed") {
          (eventId, e) =>
            send(
              eventId,
              "DispatchConfirmed",
              e.orderId,
              shippedEmail(e),
              None
            )
        },
        EnvelopeRoute.of[F, DispatchFailedEvent]("DispatchFailed") {
          (eventId, e) =>
            send(
              eventId,
              "DispatchFailed",
              e.orderId,
              failedEmail(e),
              Some(e.reason)
            )
        }
      ),
      logger,
      deadLetter
    )(key, value)
  }

  private def sendEmail[F[_]: FlatMap](
      emailClient: EmailClient[F],
      logger: StructuredLogger[F],
      eventId: String,
      eventType: String,
      orderId: String,
      email: (String, String),
      reason: Option[String]
  ): F[Unit] =
    emailClient
      .send(
        EmailMessage(
          sourceEventId = eventId,
          eventType = eventType,
          reason = reason,
          recipientType = RecipientType.Order,
          recipientKey = orderId,
          customerId = None,
          orderId = Some(orderId),
          subject = email._1,
          body = email._2
        )
      )
      .flatMap(
        EmailSendLog.log(
          logger,
          Map("order_id" -> orderId, "event_type" -> eventType),
          eventType
        )
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
        handle(emailClient, logger, deadLetter)(
          committable.record.key.getOrElse(""),
          committable.record.value
        ) *> committable.offset.commit
      }
}
