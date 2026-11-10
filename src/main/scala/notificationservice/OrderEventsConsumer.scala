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

  val groupId: String = "notification-service-order-events"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId(groupId)
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

  def cancelledEmail(event: OrderCancelledEvent): (String, String) = {
    val body = event.reason match {
      case Some("customer") =>
        s"Order ${event.orderId} was cancelled at your request."
      case Some("auth_expired") =>
        s"Order ${event.orderId} was cancelled because its payment authorization expired."
      case Some("stale") =>
        s"Order ${event.orderId} was cancelled because it could not be completed in time."
      case Some("operator") =>
        s"Order ${event.orderId} was cancelled by our support team."
      case Some(_) | None =>
        s"Order ${event.orderId} was cancelled."
    }
    ("Your order was cancelled", body)
  }

  private def send[F[_]: FlatMap](
      emailClient: EmailClient[F],
      logger: StructuredLogger[F],
      eventId: String,
      customerId: String,
      orderId: String,
      eventType: String,
      reason: Option[String],
      email: (String, String),
      actor: Option[OrderActor] = None
  ): F[Unit] =
    emailClient
      .send(
        EmailMessage(
          sourceEventId = eventId,
          eventType = eventType,
          reason = reason,
          recipientType = RecipientType.Customer,
          recipientKey = customerId,
          customerId = Some(customerId),
          orderId = Some(orderId),
          subject = email._1,
          body = email._2
        )
      )
      .flatMap(
        EmailSendLog.log(
          logger,
          Map("order_id" -> orderId, "event_type" -> eventType) ++
            actor.fold(Map.empty[String, String])(a =>
              Map("actor_type" -> a.`type`, "actor_id" -> a.id)
            ),
          eventType
        )
      )

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
        EnvelopeRoute.of[F, OrderConfirmedEvent]("OrderConfirmed") {
          (eventId, e) =>
            send(
              emailClient,
              logger,
              eventId,
              e.customerId,
              e.orderId,
              "OrderConfirmed",
              None,
              confirmedEmail(e)
            )
        },
        EnvelopeRoute.of[F, OrderPaymentFailedEvent]("OrderPaymentFailed") {
          (eventId, e) =>
            send(
              emailClient,
              logger,
              eventId,
              e.customerId,
              e.orderId,
              "OrderPaymentFailed",
              Some(e.reason),
              paymentFailedEmail(e)
            )
        },
        EnvelopeRoute.of[F, OrderCancelledEvent]("OrderCancelled") {
          (eventId, e) =>
            send(
              emailClient,
              logger,
              eventId,
              e.customerId,
              e.orderId,
              "OrderCancelled",
              e.reason,
              cancelledEmail(e),
              e.actor
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
