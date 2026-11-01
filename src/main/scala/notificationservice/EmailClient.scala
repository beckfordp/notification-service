package notificationservice

import org.typelevel.log4cats.StructuredLogger

/** Stub for sending a customer-facing email. No real provider is decided yet
  * (per `gluon/PLAN.md` Phase 5). `to` is the best identity each event carries:
  * the customerId for order events, the orderId for fulfilment/payment events,
  * and the real email address only for `UserRegistered`.
  */
trait EmailClient[F[_]] {
  def send(to: String, subject: String, body: String): F[Unit]
}

object EmailClient {

  /** Logs the send as a structured line instead of calling a real provider.
    */
  def logging[F[_]](logger: StructuredLogger[F]): EmailClient[F] =
    new EmailClient[F] {
      def send(to: String, subject: String, body: String): F[Unit] =
        logger.info(
          Map("to" -> to, "subject" -> subject, "body" -> body)
        )(s"Sending email (stub - logged only) to=$to subject=$subject")
    }
}
