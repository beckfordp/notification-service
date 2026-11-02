package notificationservice

import cats.Applicative
import cats.Monad
import cats.syntax.all._

/** The result of trying to turn an event into a real addressee (ADM-3/ADR
  * 0025). `Unresolved` is terminal: the audit row is marked and no transport
  * call is made, because sending to a non-address is never acceptable.
  * Transient lookup failures are raised by the clients instead, so the caller's
  * DLQ guard retries them.
  */
sealed trait RecipientResolution

object RecipientResolution {
  final case class Resolved(message: EmailMessage) extends RecipientResolution
  final case class Unresolved(message: EmailMessage, reason: String)
      extends RecipientResolution
}

/** Resolves an [[EmailMessage]] to a real email address:
  *
  *   - an already-known address (`UserRegistered`) is used as-is;
  *   - `customerId` → user-service `GET /users/{id}`;
  *   - `orderId` only (payment/fulfilment) → order-service `GET /orders/{id}`,
  *     then the user lookup.
  */
trait RecipientResolver[F[_]] {
  def resolve(message: EmailMessage): F[RecipientResolution]
}

object RecipientResolver {

  /** Test/legacy resolver: trusts the message as-is, sending to `recipientKey`
    * exactly as before ADM-3.
    */
  def none[F[_]: Applicative]: RecipientResolver[F] =
    new RecipientResolver[F] {
      def resolve(message: EmailMessage): F[RecipientResolution] =
        Applicative[F].pure(RecipientResolution.Resolved(message))
    }

  def apply[F[_]: Monad](
      users: UserClient[F],
      orders: OrderClient[F]
  ): RecipientResolver[F] =
    new RecipientResolver[F] {
      def resolve(message: EmailMessage): F[RecipientResolution] =
        resolvedAddress(message) match {
          case Some(email) =>
            Monad[F].pure(
              RecipientResolution.Resolved(
                message.copy(recipientEmail = Some(email))
              )
            )
          case None =>
            customerIdFor(message).flatMap {
              case Left(reason) =>
                Monad[F].pure(RecipientResolution.Unresolved(message, reason))
              case Right((customerId, withCustomer)) =>
                users.getEmail(customerId).map {
                  case Some(email) =>
                    RecipientResolution.Resolved(
                      withCustomer.copy(
                        customerId = Some(customerId),
                        recipientEmail = Some(email)
                      )
                    )
                  case None =>
                    RecipientResolution.Unresolved(
                      withCustomer.copy(customerId = Some(customerId)),
                      s"unknown_customer:$customerId"
                    )
                }
            }
        }

      /** The address the message already carries: the resolved field, or the
        * event key when the event itself is an email (`UserRegistered`).
        */
      private def resolvedAddress(message: EmailMessage): Option[String] =
        message.recipientEmail.orElse(
          Option
            .when(message.recipientType == RecipientType.Email)(
              message.recipientKey
            )
        )

      /** The customer id, one order lookup away when the event carries only an
        * orderId.
        */
      private def customerIdFor(
          message: EmailMessage
      ): F[Either[String, (String, EmailMessage)]] =
        message.customerId match {
          case Some(customerId) =>
            Monad[F].pure(Right((customerId, message)))
          case None =>
            message.orderId match {
              case None =>
                Monad[F].pure(Left("no_customer_identity"))
              case Some(orderId) =>
                orders.getCustomerId(orderId).map {
                  case Some(customerId) => Right((customerId, message))
                  case None             => Left(s"unknown_order:$orderId")
                }
            }
        }
    }
}
