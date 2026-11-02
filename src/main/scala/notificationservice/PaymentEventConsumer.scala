package notificationservice

import cats.{FlatMap, MonadThrow}
import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import org.typelevel.log4cats.StructuredLogger
import purekafka.consumer.DeadLetter

/** Consumes `PaymentCaptured`/`PaymentFailed` on `payment-events` (US-19.4) and
  * sends the receipt / payment-problem email. Payment events carry no email or
  * customerId, so the stub recipient is the orderId - real recipient identity
  * is a later feature. Duplicates send duplicates.
  */
object PaymentEventConsumer {

  val topic: String = "payment-events"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId("notification-service-payment-events")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  /** Cents to the platform's `$12.34` display convention (gshop's
    * `formatPrice`, the only amount formatter in the codebase). Payment events
    * carry no currency field. Total: a negative amount gets a leading `-`
    * (widened to `Long` so `Int.MinValue` has an absolute value).
    */
  def formatAmount(amountCents: Int): String = {
    val abs = math.abs(amountCents.toLong)
    val sign = if (amountCents < 0) "-" else ""
    f"$sign$$${abs / 100}.${abs % 100}%02d"
  }

  /** Exposed for tests: the email content is pure. */
  def receiptEmail(event: PaymentCapturedEvent): (String, String) =
    (
      "Your payment was received",
      s"We received your payment of ${formatAmount(event.amountCents)} for order ${event.orderId}."
    )

  def failedEmail(event: PaymentFailedEvent): (String, String) =
    (
      "We couldn't take your payment",
      s"We couldn't take payment of ${formatAmount(event.amountCents)} for order ${event.orderId}: ${event.reason}."
    )

  /** One `payment-events` record, minus the offset commit - split from [[run]]
    * so the routing is testable without a broker. Per-record rules:
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
        outcome: String,
        reason: Option[String]
    ) =
      sendEmail(
        emailClient,
        logger,
        eventId,
        eventType,
        orderId,
        email,
        outcome,
        reason
      )
    EnvelopeHandler.handle[F](
      topic,
      List(
        EnvelopeRoute.of[F, PaymentCapturedEvent]("PaymentCaptured") {
          (eventId, e) =>
            send(
              eventId,
              "PaymentCaptured",
              e.orderId,
              receiptEmail(e),
              "payment_captured",
              None
            )
        },
        EnvelopeRoute.of[F, PaymentFailedEvent]("PaymentFailed") {
          (eventId, e) =>
            send(
              eventId,
              "PaymentFailed",
              e.orderId,
              failedEmail(e),
              "payment_failed",
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
      outcome: String,
      reason: Option[String]
  ): F[Unit] =
    emailClient.send(
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
    ) *>
      logger.info(Map("order_id" -> orderId, "outcome" -> outcome))(
        s"Sent payment email (order_id=$orderId, outcome=$outcome)"
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
