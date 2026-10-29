package notificationservice

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import java.time.Instant

class OrderEventsPropertySuite extends ScalaCheckSuite {

  private val at = Instant.parse("2026-01-01T00:00:00Z")

  private val nonEmptyId = Gen.identifier.suchThat(_.nonEmpty)

  property("every OrderConfirmed maps to a non-empty email naming the order") {
    forAll(nonEmptyId, nonEmptyId) { (orderId, customerId) =>
      val (subject, body) =
        OrderEventsConsumer.confirmedEmail(
          OrderConfirmedEvent(orderId, customerId, at)
        )
      subject.nonEmpty && body.nonEmpty && body.contains(orderId)
    }
  }

  property(
    "every OrderPaymentFailed maps to a non-empty email naming the order"
  ) {
    forAll(nonEmptyId, nonEmptyId, Gen.alphaNumStr) {
      (orderId, customerId, reason) =>
        val (subject, body) =
          OrderEventsConsumer.paymentFailedEmail(
            OrderPaymentFailedEvent(orderId, customerId, reason, at)
          )
        subject.nonEmpty && body.nonEmpty && body.contains(orderId)
    }
  }

  property("every OrderCancelled maps to a non-empty email naming the order") {
    forAll(nonEmptyId, nonEmptyId) { (orderId, customerId) =>
      val (subject, body) =
        OrderEventsConsumer.cancelledEmail(
          OrderCancelledEvent(orderId, customerId, at)
        )
      subject.nonEmpty && body.nonEmpty && body.contains(orderId)
    }
  }
}
