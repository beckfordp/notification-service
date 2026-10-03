package notificationservice

import cats.effect.{IO, Ref}
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fs2.kafka._
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger

import java.time.Instant
import scala.concurrent.duration._

/** Ignored: Testcontainers/docker-java in this dev environment cannot reach
  * the local Docker daemon (every client strategy gets a degenerate, all-empty
  * `/info` response with a 400, even though `docker`/`docker compose`/`curl`
  * against the same socket all work fine) - a docker-java/Testcontainers
  * version-vs-Docker-Desktop-version compatibility issue, not a code problem.
  * Un-ignore once that's resolved; mirrors `OrderReservedConsumerSuite`'s
  * pattern exactly otherwise.
  */
class OrderStatusChangedConsumerSuite
    extends CatsEffectSuite
    with TestContainerForAll {

  override val containerDef: KafkaContainer.Def = KafkaContainer.Def()

  final case class SentEmail(to: String, subject: String, body: String)

  private def configFor(kafka: KafkaContainer): KafkaConfig =
    KafkaConfig(bootstrapServers = kafka.bootstrapServers)

  private def fakeEmailClient(ref: Ref[IO, List[SentEmail]]): EmailClient[IO] =
    new EmailClient[IO] {
      def send(to: String, subject: String, body: String): IO[Unit] =
        ref.update(_ :+ SentEmail(to, subject, body))
    }

  private def awaitEmails(
      ref: Ref[IO, List[SentEmail]],
      n: Int
  ): IO[List[SentEmail]] = {
    def loop: IO[List[SentEmail]] =
      ref.get.flatMap { emails =>
        if (emails.size >= n) IO.pure(emails)
        else IO.sleep(50.millis) *> loop
      }
    loop.timeout(15.seconds)
  }

  private def produce(
      config: KafkaConfig,
      key: String,
      json: String
  ): IO[Unit] = {
    val producerSettings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
    KafkaProducer
      .resource(producerSettings)
      .use(
        _.produceOne_(
          ProducerRecord(OrderStatusChangedConsumer.topic, key, json)
        ).flatten.void
      )
  }

  private def event(
      orderId: String,
      customerId: String,
      status: OrderStatusChanged
  ): OrderStatusChangedEvent =
    OrderStatusChangedEvent(
      orderId,
      customerId,
      status,
      Instant.parse("2026-01-01T00:00:00Z")
    )

  private def runAndAwaitOneEmail(
      kafka: KafkaContainer,
      ev: OrderStatusChangedEvent
  ): IO[SentEmail] = {
    val config = configFor(kafka)
    for {
      emailsRef <- Ref.of[IO, List[SentEmail]](Nil)
      _ <- produce(config, ev.orderId, ev.asJson.noSpaces)
      raced <- IO.race(
        OrderStatusChangedConsumer
          .run[IO](config, fakeEmailClient(emailsRef), NoOpLogger[IO])
          .compile
          .drain,
        awaitEmails(emailsRef, 1)
      )
      emails <- raced match {
        case Right(emails) => IO.pure(emails)
        case Left(_) =>
          IO.raiseError(
            new RuntimeException(
              "consumer stream completed before an email arrived"
            )
          )
      }
    } yield emails.head
  }

  test("a reservation_failed event sends the matching email".ignore) {
    withContainers { kafka =>
      val ev = event("order-1", "cust-1", OrderStatusChanged.ReservationFailed)
      runAndAwaitOneEmail(kafka, ev).map { email =>
        assertEquals(email.to, "cust-1")
        assert(email.subject.contains("could not be fulfilled"))
        assert(email.body.contains("order-1"))
      }
    }
  }

  test("a confirmed event sends the matching email".ignore) {
    withContainers { kafka =>
      val ev = event("order-2", "cust-2", OrderStatusChanged.Confirmed)
      runAndAwaitOneEmail(kafka, ev).map { email =>
        assertEquals(email.to, "cust-2")
        assert(email.subject.contains("confirmed"))
        assert(email.body.contains("order-2"))
      }
    }
  }

  test("a payment_failed event sends the matching email".ignore) {
    withContainers { kafka =>
      val ev = event("order-3", "cust-3", OrderStatusChanged.PaymentFailed)
      runAndAwaitOneEmail(kafka, ev).map { email =>
        assertEquals(email.to, "cust-3")
        assert(email.subject.contains("couldn't process your payment"))
        assert(email.body.contains("order-3"))
      }
    }
  }
}
