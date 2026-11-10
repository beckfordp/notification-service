// SPDX-License-Identifier: MIT

package notificationservice

import cats.effect.Sync
import cats.syntax.all._
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.Meter
import purerest.logging.Milestones

/** Notification telemetry (gluon PERF-4.8, 4.11), as a decorator on the
  * outermost `EmailClient`: `notifications.emails{event_type, outcome}` with
  * `outcome` one of `sent`, `duplicate`, `unresolved` or `error` (the send
  * raised, so the consumer retries / dead-letters). Event types are the fixed
  * set the consumers handle. A sent email carrying an order id logs the
  * `order_notified` milestone — the journey's last step.
  */
object EmailJourneyMetrics {

  def outcome(result: Either[Throwable, EmailSendOutcome]): String =
    result match {
      case Right(EmailSendOutcome.Sent)          => "sent"
      case Right(EmailSendOutcome.AlreadySent)   => "duplicate"
      case Right(EmailSendOutcome.Unresolved(_)) => "unresolved"
      case Left(_)                               => "error"
    }

  def instrument[F[_]: Sync](
      client: EmailClient[F],
      meter: Meter[F],
      logger: StructuredLogger[F]
  ): F[EmailClient[F]] =
    meter.counter[Long]("notifications.emails").create.map { emails =>
      new EmailClient[F] {
        def send(message: EmailMessage): F[EmailSendOutcome] =
          client
            .send(message)
            .attempt
            .flatTap { result =>
              emails.inc(
                Attribute("event_type", message.eventType),
                Attribute("outcome", outcome(result))
              ) *> message.orderId
                .filter(_ => result == Right(EmailSendOutcome.Sent))
                .traverse_(orderId =>
                  Milestones.log(logger)(
                    "order_notified",
                    orderId,
                    Map("event_type" -> message.eventType)
                  )
                )
            }
            .rethrow
      }
    }
}
