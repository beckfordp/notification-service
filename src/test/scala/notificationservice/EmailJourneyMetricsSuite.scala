// SPDX-License-Identifier: MIT

package notificationservice

import cats.effect.{IO, Ref}
import munit.CatsEffectSuite
import org.typelevel.log4cats.testing.StructuredTestingLogger
import purerest.metrics.Metrics

import scala.jdk.CollectionConverters._

class EmailJourneyMetricsSuite extends CatsEffectSuite {

  private def message(orderId: Option[String]) = EmailMessage(
    sourceEventId = "e1",
    eventType = "OrderConfirmed",
    reason = None,
    recipientType = RecipientType.Customer,
    recipientKey = "cust-1",
    customerId = Some("cust-1"),
    orderId = orderId,
    subject = "s",
    body = "b"
  )

  test("counts every send outcome by event type and logs order_notified") {
    Metrics.test[IO]("email-journey").use { tm =>
      val logger = StructuredTestingLogger.impl[IO]()
      for {
        results <- Ref.of[IO, List[IO[EmailSendOutcome]]](
          List(
            IO.pure(EmailSendOutcome.Sent),
            IO.pure(EmailSendOutcome.Sent),
            IO.pure(EmailSendOutcome.AlreadySent),
            IO.pure(EmailSendOutcome.Unresolved("no email")),
            IO.raiseError(new RuntimeException("smtp down"))
          )
        )
        underlying = new EmailClient[IO] {
          def send(m: EmailMessage): IO[EmailSendOutcome] =
            results.modify(rs => (rs.tail, rs.head)).flatten
        }
        client <- EmailJourneyMetrics
          .instrument[IO](underlying, tm.meter, logger)
        _ <- client.send(message(Some("order-1")))
        _ <- client.send(message(None))
        _ <- client.send(message(Some("order-1")))
        _ <- client.send(message(Some("order-1")))
        failed <- client.send(message(Some("order-1"))).attempt
        metrics <- tm.collectMetrics
        logged <- logger.logged
      } yield {
        assert(failed.isLeft)
        val points = metrics
          .find(_.getName == "notifications.emails")
          .get
          .getLongSumData
          .getPoints
          .asScala
          .map { p =>
            val a =
              p.getAttributes.asMap.asScala
                .map((k, v) => k.getKey -> v.toString)
            (a("event_type"), a("outcome")) -> p.getValue
          }
          .toMap
        assertEquals(
          points,
          Map(
            ("OrderConfirmed", "sent") -> 2L,
            ("OrderConfirmed", "duplicate") -> 1L,
            ("OrderConfirmed", "unresolved") -> 1L,
            ("OrderConfirmed", "error") -> 1L
          )
        )
        assertEquals(
          logged.toList
            .map(l => (l.ctx.get("milestone"), l.ctx.get("order_id"))),
          List((Some("order_notified"), Some("order-1")))
        )
      }
    }
  }
}
