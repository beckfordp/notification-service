// SPDX-License-Identifier: MIT

package notificationservice

import cats.effect.Sync
import cats.syntax.all._
import org.typelevel.log4cats.StructuredLogger

import java.util.UUID

/** Everything the audit store needs to know about one email, independent of how
  * it is sent. `recipientType`/`recipientKey` record what the event identified
  * (the customerId for order events, the orderId for payment/fulfilment events,
  * the real email address for `UserRegistered`); `recipientEmail` is the
  * resolved destination address, when known (ADM-3/ADR 0025).
  */
final case class EmailMessage(
    sourceEventId: String,
    eventType: String,
    reason: Option[String],
    recipientType: RecipientType,
    recipientKey: String,
    customerId: Option[String],
    orderId: Option[String],
    subject: String,
    body: String,
    recipientEmail: Option[String] = None
) {

  def toNewSentEmail(id: UUID): NewSentEmail =
    NewSentEmail(
      id = id,
      sourceEventId = sourceEventId,
      eventType = eventType,
      reason = reason,
      recipientType = recipientType,
      recipientKey = recipientKey,
      recipientEmail = recipientEmail,
      customerId = customerId,
      orderId = orderId,
      subject = subject,
      body = body
    )
}

/** What happened to one `send` call, so consumer logs stay truthful (ADM-3).
  * `Unresolved` is terminal — no transport call was made.
  */
sealed trait EmailSendOutcome

object EmailSendOutcome {
  case object Sent extends EmailSendOutcome
  case object AlreadySent extends EmailSendOutcome
  final case class Unresolved(reason: String) extends EmailSendOutcome
}

/** Consumer-side logging for a send outcome, so a duplicate or unresolved event
  * never logs as "sent" (ADM-3).
  */
object EmailSendLog {
  def log[F[_]: cats.FlatMap](
      logger: StructuredLogger[F],
      context: Map[String, String],
      what: String
  )(outcome: EmailSendOutcome): F[Unit] =
    outcome match {
      case EmailSendOutcome.Sent =>
        logger.info(context + ("outcome" -> "sent"))(s"Sent $what email")
      case EmailSendOutcome.AlreadySent =>
        logger.info(context + ("outcome" -> "duplicate"))(
          s"$what email already sent or unresolved - skipped"
        )
      case EmailSendOutcome.Unresolved(reason) =>
        logger.warn(
          context + ("outcome" -> "unresolved") + ("reason" -> reason)
        )(s"$what email not sent - recipient unresolved ($reason)")
    }
}

/** Sends a customer-facing email. No real provider is decided yet (per
  * `gluon/PLAN.md` Phase 5) — [[EmailClient.logging]] is the transport stub.
  * Consumers use [[EmailClient.audited]] so every attempt is recorded in
  * `sent_email` first (ADM-2) and recipients are resolved (ADM-3).
  */
trait EmailClient[F[_]] {
  def send(message: EmailMessage): F[EmailSendOutcome]
}

object EmailClient {

  /** Logs the send as a structured line instead of calling a real provider. The
    * `to` field is the resolved address when known, falling back to the event's
    * identity key for unresolved/legacy messages.
    */
  def logging[F[_]: cats.Functor](logger: StructuredLogger[F]): EmailClient[F] =
    new EmailClient[F] {
      def send(message: EmailMessage): F[EmailSendOutcome] = {
        val to = message.recipientEmail.getOrElse(message.recipientKey)
        logger
          .info(
            Map(
              "to" -> to,
              "subject" -> message.subject,
              "body" -> message.body,
              "event_type" -> message.eventType,
              "source_event_id" -> message.sourceEventId
            )
          )(
            s"Sending email (stub - logged only) to=$to subject=${message.subject}"
          )
          .as(EmailSendOutcome.Sent)
      }
    }

  /** Claims the event in `sent_email`, resolves the recipient (ADM-3), sends
    * through `transport`, then marks the row `sent`. A duplicate whose row is
    * already `sent`/`unresolved` is not sent again; `pending`/`failed` rows are
    * retried. An unresolvable recipient marks the row `unresolved` (terminal,
    * no send, no rethrow); a transient resolution or send failure marks it
    * `failed` and rethrows so the caller's DLQ guard retries.
    */
  def audited[F[_]: Sync](
      store: SentEmailStore[F],
      transport: EmailClient[F],
      logger: StructuredLogger[F],
      resolver: RecipientResolver[F]
  ): EmailClient[F] =
    new EmailClient[F] {
      def send(message: EmailMessage): F[EmailSendOutcome] =
        Sync[F].delay(UUID.randomUUID()).flatMap { id =>
          store.claim(message.toNewSentEmail(id)).flatMap {
            case EmailClaim.Duplicate =>
              logger
                .info(
                  Map(
                    "source_event_id" -> message.sourceEventId,
                    "event_type" -> message.eventType
                  )
                )(
                  "Duplicate event - email already sent or unresolved; skipping"
                )
                .as(EmailSendOutcome.AlreadySent)
            case EmailClaim.Send(rowId) =>
              def failed(error: Throwable): F[EmailSendOutcome] = {
                val detail =
                  Option(error.getMessage).getOrElse(error.getClass.getName)
                store.markFailed(rowId, detail) *>
                  Sync[F].raiseError[EmailSendOutcome](error)
              }

              resolver.resolve(message).attempt.flatMap {
                case Left(error) => failed(error)
                case Right(RecipientResolution.Unresolved(_, reason)) =>
                  store.markUnresolved(rowId, reason) *>
                    logger
                      .warn(
                        Map(
                          "source_event_id" -> message.sourceEventId,
                          "event_type" -> message.eventType,
                          "reason" -> reason
                        )
                      )(
                        s"Recipient unresolved ($reason) - no email sent"
                      )
                      .as(EmailSendOutcome.Unresolved(reason))
                case Right(RecipientResolution.Resolved(resolved)) =>
                  store
                    .markResolved(
                      rowId,
                      resolved.customerId,
                      resolved.recipientEmail
                    ) *>
                    transport.send(resolved).attempt.flatMap {
                      case Right(_) =>
                        store.markSent(rowId) *>
                          logger
                            .info(
                              Map(
                                "email_id" -> rowId.toString,
                                "source_event_id" -> message.sourceEventId,
                                "recipient" -> resolved.recipientEmail
                                  .getOrElse(resolved.recipientKey)
                              )
                            )(s"Recorded sent email (id=$rowId)")
                            .as(EmailSendOutcome.Sent)
                      case Left(error) => failed(error)
                    }
              }
          }
        }
    }
}
