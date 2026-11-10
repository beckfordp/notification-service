// SPDX-License-Identifier: MIT

package notificationservice

import cats.effect.IO
import munit.CatsEffectSuite

class RecipientResolverSuite extends CatsEffectSuite {

  private def message(
      customerId: Option[String] = None,
      orderId: Option[String] = None,
      recipientEmail: Option[String] = None
  ): EmailMessage =
    EmailMessage(
      sourceEventId = "e1",
      eventType = "OrderConfirmed",
      reason = None,
      recipientType = RecipientType.Customer,
      recipientKey = "key",
      customerId = customerId,
      orderId = orderId,
      subject = "subject",
      body = "body",
      recipientEmail = recipientEmail
    )

  private def usersWith(email: String => IO[Option[String]]): UserClient[IO] =
    new UserClient[IO] {
      def getEmail(customerId: String): IO[Option[String]] =
        email(customerId)
    }

  private def ordersWith(
      contact: String => IO[Option[OrderContact]]
  ): OrderClient[IO] =
    new OrderClient[IO] {
      def getContact(orderId: String): IO[Option[OrderContact]] =
        contact(orderId)
    }

  private val userMustNotBeCalled = usersWith(_ =>
    IO.raiseError(new AssertionError("user lookup must not be called"))
  )

  private val orderMustNotBeCalled = ordersWith(_ =>
    IO.raiseError(new AssertionError("order lookup must not be called"))
  )

  test("an already-known address short-circuits without lookups") {
    val known =
      message(customerId = Some("c1"), recipientEmail = Some("ada@example.com"))
    for {
      result <- RecipientResolver[IO](userMustNotBeCalled, orderMustNotBeCalled)
        .resolve(known)
    } yield assertEquals(result, RecipientResolution.Resolved(known))
  }

  test("an email-type message falls back to recipientKey without lookups") {
    val welcome = EmailMessage(
      sourceEventId = "e2",
      eventType = "UserRegistered",
      reason = None,
      recipientType = RecipientType.Email,
      recipientKey = "ada@example.com",
      customerId = Some("c1"),
      orderId = None,
      subject = "Welcome",
      body = "Welcome"
    )
    for {
      result <- RecipientResolver[IO](userMustNotBeCalled, orderMustNotBeCalled)
        .resolve(welcome)
    } yield assertEquals(
      result,
      RecipientResolution.Resolved(
        welcome.copy(recipientEmail = Some("ada@example.com"))
      )
    )
  }

  test("a customerId is resolved through user-service") {
    val original = message(customerId = Some("c1"))
    for {
      result <- RecipientResolver[IO](
        usersWith(id => IO.pure(Some(s"$id@example.com"))),
        orderMustNotBeCalled
      ).resolve(original)
    } yield assertEquals(
      result,
      RecipientResolution.Resolved(
        original.copy(recipientEmail = Some("c1@example.com"))
      )
    )
  }

  test(
    "an orderId-only event is resolved through order-service then user-service"
  ) {
    val original = message(orderId = Some("o1"))
    for {
      result <- RecipientResolver[IO](
        usersWith(_ => IO.pure(Some("ada@example.com"))),
        ordersWith(_ => IO.pure(Some(OrderContact("c1", None))))
      ).resolve(original)
    } yield assertEquals(
      result,
      RecipientResolution.Resolved(
        original.copy(
          customerId = Some("c1"),
          recipientEmail = Some("ada@example.com")
        )
      )
    )
  }

  test("an unknown order is terminal and is not followed by a user lookup") {
    val original = message(orderId = Some("o1"))
    for {
      result <- RecipientResolver[IO](
        userMustNotBeCalled,
        ordersWith(_ => IO.pure(None))
      ).resolve(original)
    } yield assertEquals(
      result,
      RecipientResolution.Unresolved(original, "unknown_order:o1")
    )
  }

  test("an unknown customer is terminal but keeps the customerId") {
    val original = message(customerId = Some("c1"))
    for {
      result <- RecipientResolver[IO](
        usersWith(_ => IO.pure(None)),
        orderMustNotBeCalled
      ).resolve(original)
    } yield assertEquals(
      result,
      RecipientResolution.Unresolved(original, "unknown_customer:c1")
    )
  }

  test("an order event for a guest falls back to the order's email (ADM-10)") {
    val original = message(customerId = Some("guest-1"), orderId = Some("o1"))
    for {
      result <- RecipientResolver[IO](
        usersWith(_ => IO.pure(None)),
        ordersWith(_ =>
          IO.pure(Some(OrderContact("guest-1", Some("guest@example.com"))))
        )
      ).resolve(original)
    } yield assertEquals(
      result,
      RecipientResolution.Resolved(
        original.copy(recipientEmail = Some("guest@example.com"))
      )
    )
  }

  test("an orderId-only guest event reuses the one order lookup (ADM-10)") {
    val original = message(orderId = Some("o1"))
    for {
      lookups <- cats.effect.Ref.of[IO, Int](0)
      result <- RecipientResolver[IO](
        usersWith(_ => IO.pure(None)),
        ordersWith(_ =>
          lookups
            .update(_ + 1)
            .as(Some(OrderContact("guest-1", Some("guest@example.com"))))
        )
      ).resolve(original)
      count <- lookups.get
    } yield {
      assertEquals(
        result,
        RecipientResolution.Resolved(
          original.copy(
            customerId = Some("guest-1"),
            recipientEmail = Some("guest@example.com")
          )
        )
      )
      assertEquals(count, 1)
    }
  }

  test("a guest order without an email stays terminal unresolved") {
    val original = message(customerId = Some("guest-1"), orderId = Some("o1"))
    for {
      result <- RecipientResolver[IO](
        usersWith(_ => IO.pure(None)),
        ordersWith(_ => IO.pure(Some(OrderContact("guest-1", None))))
      ).resolve(original)
    } yield assertEquals(
      result,
      RecipientResolution.Unresolved(original, "unknown_customer:guest-1")
    )
  }

  test("a signed-in customer never triggers the guest order lookup") {
    val original = message(customerId = Some("c1"), orderId = Some("o1"))
    for {
      result <- RecipientResolver[IO](
        usersWith(_ => IO.pure(Some("ada@example.com"))),
        orderMustNotBeCalled
      ).resolve(original)
    } yield assertEquals(
      result,
      RecipientResolution.Resolved(
        original.copy(recipientEmail = Some("ada@example.com"))
      )
    )
  }

  test("no identity at all is terminal") {
    val original = message()
    for {
      result <- RecipientResolver[IO](
        userMustNotBeCalled,
        orderMustNotBeCalled
      ).resolve(original)
    } yield assertEquals(
      result,
      RecipientResolution.Unresolved(original, "no_customer_identity")
    )
  }

  test("a transient lookup failure propagates for retry/DLQ") {
    for {
      result <- RecipientResolver[IO](
        usersWith(_ =>
          IO.raiseError(new RuntimeException("user-service down"))
        ),
        orderMustNotBeCalled
      ).resolve(message(customerId = Some("c1"))).attempt
    } yield assert(result.isLeft)
  }
}
