package notificationservice

import cats.effect.Sync
import cats.syntax.all._
import org.typelevel.log4cats.StructuredLogger

import java.util.UUID

/** Everything the audit store needs to know about one email, independent of how
  * it is sent. `recipientKey` is the best identity the event carries today: the
  * customerId for order events, the orderId for payment/fulfilment events, and
  * the real email address only for `UserRegistered` (ADM-3 resolves the rest).
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
    body: String
) {

  def toNewSentEmail(id: UUID): NewSentEmail =
    NewSentEmail(
      id = id,
      sourceEventId = sourceEventId,
      eventType = eventType,
      reason = reason,
      recipientType = recipientType,
      recipientKey = recipientKey,
      customerId = customerId,
      orderId = orderId,
      subject = subject,
      body = body
    )
}

/** Sends a customer-facing email. No real provider is decided yet (per
  * `gluon/PLAN.md` Phase 5) — [[EmailClient.logging]] is the transport stub.
  * Consumers use [[EmailClient.audited]] so every attempt is recorded in
  * `sent_email` first (ADM-2).
  */
trait EmailClient[F[_]] {
  def send(message: EmailMessage): F[Unit]
}

object EmailClient {

  /** Logs the send as a structured line instead of calling a real provider.
    */
  def logging[F[_]](logger: StructuredLogger[F]): EmailClient[F] =
    new EmailClient[F] {
      def send(message: EmailMessage): F[Unit] =
        logger.info(
          Map(
            "to" -> message.recipientKey,
            "subject" -> message.subject,
            "body" -> message.body,
            "event_type" -> message.eventType,
            "source_event_id" -> message.sourceEventId
          )
        )(
          s"Sending email (stub - logged only) to=${message.recipientKey} subject=${message.subject}"
        )
    }

  /** Claims the event in `sent_email`, sends through `transport`, then marks
    * the row `sent` (or `failed`, rethrowing so the caller's DLQ guard retries
    * and eventually dead-letters). A duplicate whose row is already `sent` is
    * logged and not sent again; `pending`/`failed` rows are retried.
    */
  def audited[F[_]: Sync](
      store: SentEmailStore[F],
      transport: EmailClient[F],
      logger: StructuredLogger[F]
  ): EmailClient[F] =
    new EmailClient[F] {
      def send(message: EmailMessage): F[Unit] =
        Sync[F].delay(UUID.randomUUID()).flatMap { id =>
          store.claim(message.toNewSentEmail(id)).flatMap {
            case EmailClaim.Duplicate =>
              logger.info(
                Map(
                  "source_event_id" -> message.sourceEventId,
                  "event_type" -> message.eventType
                )
              )("Duplicate event - email already sent; skipping")
            case EmailClaim.Send(rowId) =>
              transport.send(message).attempt.flatMap {
                case Right(_) =>
                  store.markSent(rowId) *>
                    logger.info(
                      Map(
                        "email_id" -> rowId.toString,
                        "source_event_id" -> message.sourceEventId
                      )
                    )(s"Recorded sent email (id=$rowId)")
                case Left(error) =>
                  val detail =
                    Option(error.getMessage).getOrElse(error.getClass.getName)
                  store.markFailed(rowId, detail) *>
                    Sync[F].raiseError[Unit](error)
              }
          }
        }
    }
}
