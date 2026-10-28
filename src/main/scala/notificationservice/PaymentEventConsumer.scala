package notificationservice

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.parser.decode
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
    * carry no currency field.
    */
  def formatAmount(amountCents: Int): String =
    f"$$${amountCents / 100}.${amountCents % 100}%02d"

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
              s"Sent payment email (order_id=$orderId, outcome=$outcome)"
            )

        def handleCaptured(event: PaymentCapturedEvent): F[Unit] =
          send(event.orderId, receiptEmail(event), "payment_captured")

        def handleFailed(event: PaymentFailedEvent): F[Unit] =
          send(event.orderId, failedEmail(event), "payment_failed")

        val key = committable.record.key.getOrElse("")

        val handled: F[Unit] = committable.record.value match {
          case None =>
            logger.error(Map.empty)(
              "Received payment-events record with a null value - skipping"
            )
          case Some(raw) =>
            decode[EventEnvelope](raw) match {
              case Left(error) =>
                logger.error(Map("raw" -> raw), error)(
                  "Failed to decode payment-events envelope"
                )
              case Right(envelope) =>
                envelope.eventType match {
                  case "PaymentCaptured" =>
                    envelope.payload.as[PaymentCapturedEvent] match {
                      case Left(error) =>
                        logger.error(Map("raw" -> raw), error)(
                          "Failed to decode PaymentCaptured payload"
                        )
                      case Right(event) =>
                        deadLetter.guard(topic, key, raw)(
                          handleCaptured(event)
                        )
                    }
                  case "PaymentFailed" =>
                    envelope.payload.as[PaymentFailedEvent] match {
                      case Left(error) =>
                        logger.error(Map("raw" -> raw), error)(
                          "Failed to decode PaymentFailed payload"
                        )
                      case Right(event) =>
                        deadLetter.guard(topic, key, raw)(handleFailed(event))
                    }
                  case _ => Async[F].unit
                }
            }
        }
        handled *> committable.offset.commit
      }
}
