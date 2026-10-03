package notificationservice

import cats.effect.Sync
import org.typelevel.log4cats.StructuredLogger

/** Stub for sending a customer-facing email. No real provider is decided yet
  * (per `gluon/PLAN.md` Phase 5) - `to` is `customerId` (a bare string, per
  * `gluon/docs/user-stories.md`'s "Auth/identity" open question), not a real
  * email address.
  */
trait EmailClient[F[_]] {
  def send(to: String, subject: String, body: String): F[Unit]
}

object EmailClient {

  /** Logs the send as a structured line instead of calling a real provider.
    */
  def logging[F[_]: Sync](logger: StructuredLogger[F]): EmailClient[F] =
    new EmailClient[F] {
      def send(to: String, subject: String, body: String): F[Unit] =
        logger.info(
          Map("to" -> to, "subject" -> subject, "body" -> body)
        )("Sending email (stub - logged only)")
    }
}
