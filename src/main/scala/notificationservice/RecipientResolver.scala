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
  *     then the user lookup;
  *   - a customerId with no account (guest checkout) → the order's `guestEmail`
  *     (ADM-10), when the event has an orderId.
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
            customerFor(message).flatMap {
              case Left(reason) =>
                Monad[F].pure(RecipientResolution.Unresolved(message, reason))
              case Right((customerId, contact)) =>
                val withCustomer = message.copy(customerId = Some(customerId))
                users.getEmail(customerId).flatMap {
                  case Some(email) =>
                    Monad[F].pure(resolved(withCustomer, email))
                  case None =>
                    guestEmail(message, contact).map {
                      case Some(email) => resolved(withCustomer, email)
                      case None        =>
                        RecipientResolution.Unresolved(
                          withCustomer,
                          s"unknown_customer:$customerId"
                        )
                    }
                }
            }
        }

      private def resolved(
          message: EmailMessage,
          email: String
      ): RecipientResolution =
        RecipientResolution.Resolved(message.copy(recipientEmail = Some(email)))

      /** ADM-10: a customerId with no account is a guest checkout; the order
        * carries the address captured at checkout. Reuses the contact already
        * fetched for an orderId-only event.
        */
      private def guestEmail(
          message: EmailMessage,
          contact: Option[OrderContact]
      ): F[Option[String]] =
        contact match {
          case Some(known) => Monad[F].pure(known.guestEmail)
          case None        =>
            message.orderId.flatTraverse(orderId =>
              orders.getContact(orderId).map(_.flatMap(_.guestEmail))
            )
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
        * orderId (that lookup's contact is kept for the guest fallback).
        */
      private def customerFor(
          message: EmailMessage
      ): F[Either[String, (String, Option[OrderContact])]] =
        message.customerId match {
          case Some(customerId) =>
            Monad[F].pure(Right((customerId, None)))
          case None =>
            message.orderId match {
              case None =>
                Monad[F].pure(Left("no_customer_identity"))
              case Some(orderId) =>
                orders.getContact(orderId).map {
                  case Some(contact) =>
                    Right((contact.customerId, Some(contact)))
                  case None => Left(s"unknown_order:$orderId")
                }
            }
        }
    }
}
