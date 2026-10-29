package notificationservice

import cats.effect.{IO, Ref}
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fs2.kafka._
import io.circe.Json
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.metrics.Meter
import purekafka.consumer.DeadLetter
import purekafka.resilience.{
  CircuitBreakerConfig,
  ResilienceConfig,
  RetryConfig
}

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._

/** Ignored: same cross-test isolation issue as the retired
  * `OrderStatusChangedConsumerSuite` - every test in this suite shares one
  * Kafka container and a fixed consumer group id with `Earliest` reset, so a
  * later test can consume an earlier test's leftover message. Fixing that is a
  * separate backlog item; the suite is rewritten for the per-outcome events
  * (TD-15) but stays `.ignore`'d. The pure email/decoding behaviour is covered
  * by `OrderEventsSuite` and the property suite.
  */
class OrderEventsConsumerSuite
    extends CatsEffectSuite
    with TestContainerForAll {

  private def noOpDeadLetter: IO[DeadLetter[IO, String, String]] = {
    import org.typelevel.otel4s.metrics.Meter.Implicits.noop
    DeadLetter.fromPublish[IO, String, String](
      RetryConfig(maxRetries = 0, baseDelay = 1.milli),
      ResilienceConfig(
        retry = RetryConfig(maxRetries = 0, baseDelay = 1.milli),
        circuitBreaker =
          CircuitBreakerConfig(failureThreshold = 100, resetTimeout = 1.second)
      ),
      (_, _, _) => IO.unit
    )(NoOpLogger[IO])(summon[Meter[IO]])
  }

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
          ProducerRecord(OrderEventsConsumer.topic, key, json)
        ).flatten.void
      )
  }

  private def envelopeJson(
      eventType: String,
      correlationId: String,
      payload: Json
  ): String =
    EventEnvelope(
      eventId = UUID.randomUUID().toString,
      eventType = eventType,
      timestamp = Instant.parse("2026-01-01T00:00:00Z"),
      source = "order-service",
      correlationId = correlationId,
      payload = payload
    ).asJson.noSpaces

  private def produceNullValue(config: KafkaConfig, key: String): IO[Unit] = {
    val producerSettings =
      ProducerSettings[IO, Array[Byte], Array[Byte]]
        .withBootstrapServers(config.bootstrapServers)
    KafkaProducer
      .resource(producerSettings)
      .use(
        _.produceOne_(
          ProducerRecord(
            OrderEventsConsumer.topic,
            key.getBytes("UTF-8"),
            null
          )
        ).flatten.void
      )
  }

  private def runAndAwaitOneEmail(
      kafka: KafkaContainer,
      eventType: String,
      orderId: String,
      payload: Json
  ): IO[SentEmail] = {
    val config = configFor(kafka)
    for {
      emailsRef <- Ref.of[IO, List[SentEmail]](Nil)
      deadLetter <- noOpDeadLetter
      _ <- produce(
        config,
        orderId,
        envelopeJson(eventType, orderId, payload)
      )
      raced <- IO.race(
        OrderEventsConsumer
          .run[IO](
            config,
            fakeEmailClient(emailsRef),
            NoOpLogger[IO],
            deadLetter
          )
          .compile
          .drain,
        awaitEmails(emailsRef, 1)
      )
      emails <- raced match {
        case Right(emails) => IO.pure(emails)
        case Left(_)       =>
          IO.raiseError(
            new RuntimeException(
              "consumer stream completed before an email arrived"
            )
          )
      }
    } yield emails.head
  }

  private val at = Instant.parse("2026-01-01T00:00:00Z")

  test("an OrderConfirmed event sends the matching email".ignore) {
    withContainers { kafka =>
      val ev = OrderConfirmedEvent("order-1", "cust-1", at)
      runAndAwaitOneEmail(kafka, "OrderConfirmed", ev.orderId, ev.asJson)
        .map { email =>
          assertEquals(email.to, "cust-1")
          assert(email.subject.contains("confirmed"))
          assert(email.body.contains("order-1"))
        }
    }
  }

  test("an OrderPaymentFailed event sends the matching email".ignore) {
    withContainers { kafka =>
      val ev =
        OrderPaymentFailedEvent("order-2", "cust-2", "capture_failed", at)
      runAndAwaitOneEmail(kafka, "OrderPaymentFailed", ev.orderId, ev.asJson)
        .map { email =>
          assertEquals(email.to, "cust-2")
          assert(email.subject.contains("couldn't process your payment"))
          assert(email.body.contains("order-2"))
        }
    }
  }

  test("an OrderCancelled event sends the matching email".ignore) {
    withContainers { kafka =>
      val ev = OrderCancelledEvent("order-3", "cust-3", at)
      runAndAwaitOneEmail(kafka, "OrderCancelled", ev.orderId, ev.asJson)
        .map { email =>
          assertEquals(email.to, "cust-3")
          assert(email.subject.contains("cancelled"))
          assert(email.body.contains("order-3"))
        }
    }
  }

  test(
    "a malformed envelope is logged and doesn't block a later good event".ignore
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      val goodEvent = OrderConfirmedEvent("order-4", "cust-4", at)
      for {
        _ <- produce(config, "bad-key", "not-json")
        email <- runAndAwaitOneEmail(
          kafka,
          "OrderConfirmed",
          goodEvent.orderId,
          goodEvent.asJson
        )
      } yield {
        assertEquals(email.to, "cust-4")
        assert(email.subject.contains("confirmed"))
      }
    }
  }

  test(
    "an envelope with an eventType other than the order lifecycle events is ignored".ignore
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      val goodEvent = OrderConfirmedEvent("order-6", "cust-6", at)
      for {
        _ <- produce(
          config,
          "order-reserved-6",
          envelopeJson(
            "OrderReserved",
            "order-reserved-6",
            Json.obj("orderId" -> Json.fromString("order-reserved-6"))
          )
        )
        email <- runAndAwaitOneEmail(
          kafka,
          "OrderConfirmed",
          goodEvent.orderId,
          goodEvent.asJson
        )
      } yield {
        assertEquals(email.to, "cust-6")
        assert(email.subject.contains("confirmed"))
      }
    }
  }

  test(
    "a null-valued record is logged and doesn't block a later good event".ignore
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      val goodEvent = OrderConfirmedEvent("order-5", "cust-5", at)
      for {
        _ <- produceNullValue(config, "null-value-key")
        email <- runAndAwaitOneEmail(
          kafka,
          "OrderConfirmed",
          goodEvent.orderId,
          goodEvent.asJson
        )
      } yield {
        assertEquals(email.to, "cust-5")
        assert(email.subject.contains("confirmed"))
      }
    }
  }
}
