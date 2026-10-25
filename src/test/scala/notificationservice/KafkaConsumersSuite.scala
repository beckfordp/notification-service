package notificationservice

import cats.effect.{IO, Ref}
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fs2.kafka._
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger
import purerest.metrics.Metrics

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Proves `KafkaConsumers.supervised`'s DLQ-routing property for
  * `OrderStatusChangedConsumer` (TD-2.4, ADR 0010).
  *
  * Unlike order-service's own `KafkaConsumersSuite` (which also proves a
  * *merge-isolation* property — wrapping two consumers individually, then
  * merging, so a crash in one doesn't restart the other), notification-service
  * has only one consumer: there's no merge, so no merge-isolation property to
  * prove here. `StreamSupervisor`'s restart-with-backoff behavior itself is
  * already proven by `purekafka`'s own `StreamSupervisorSuite` against a
  * synthetic `Stream[F, O]` — re-proving that generic mechanism again here,
  * through this thin single-consumer wrapper, would add no wiring-specific
  * value. Only the DLQ-routing test is adapted, mirroring payment-service's own
  * `KafkaConsumersSuite`. This test class gets its own fresh Testcontainers
  * Kafka instance (one per class, per `TestContainerForAll`) and has exactly
  * one test, so there's no risk of the cross-test message leakage found in the
  * *separate*, still-`.ignore`'d `OrderStatusChangedConsumerSuite` (which
  * shares one container across 6 tests) — no group-id override needed here.
  */
class KafkaConsumersSuite extends CatsEffectSuite with TestContainerForAll {

  override val containerDef: KafkaContainer.Def = KafkaContainer.Def()

  private def configFor(kafka: KafkaContainer): KafkaConfig =
    KafkaConfig(bootstrapServers = kafka.bootstrapServers)

  private def produce(
      config: KafkaConfig,
      topic: String,
      key: String,
      json: String
  ): IO[Unit] = {
    val producerSettings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
    KafkaProducer
      .resource(producerSettings)
      .use(_.produceOne_(ProducerRecord(topic, key, json)).flatten.void)
  }

  private def envelopeJson(
      eventType: String,
      correlationId: String,
      payload: io.circe.Json
  ): String =
    EventEnvelope(
      eventId = UUID.randomUUID().toString,
      eventType = eventType,
      timestamp = Instant.parse("2026-01-01T00:00:00Z"),
      source = "order-service",
      correlationId = correlationId,
      payload = payload
    ).asJson.noSpaces

  private def pollUntil[A](check: IO[Option[A]]): IO[A] =
    check.flatMap {
      case Some(a) => IO.pure(a)
      case None    => IO.sleep(200.millis) *> pollUntil(check)
    }

  final case class SentEmail(to: String, subject: String, body: String)

  private def awaitEmail(
      emailsRef: Ref[IO, List[SentEmail]]
  ): IO[SentEmail] =
    pollUntil(emailsRef.get.map(_.headOption)).timeout(20.seconds)

  /** Fails `send` every time for `failOnCustomerId` - never recovers. Used to
    * prove `DeadLetter` actually exhausts its retry budget and routes to the
    * DLQ, rather than eventually succeeding.
    */
  private def alwaysFailOn(
      failOnCustomerId: String,
      emailsRef: Ref[IO, List[SentEmail]]
  ): EmailClient[IO] = new EmailClient[IO] {
    def send(to: String, subject: String, body: String): IO[Unit] =
      if (to == failOnCustomerId)
        IO.raiseError(
          new RuntimeException("synthetic permanent email-send failure")
        )
      else emailsRef.update(_ :+ SentEmail(to, subject, body))
  }

  /** Consumes whatever's on `topic` right now (from the beginning) within a
    * short window, for asserting on DLQ contents directly - a plain, one-shot
    * read, not a long-running consumer group.
    */
  private def consumeOneFrom(
      config: KafkaConfig,
      topic: String
  ): IO[List[String]] = {
    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(s"dlq-check-${UUID.randomUUID()}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
    KafkaConsumer
      .stream(consumerSettings)
      .subscribeTo(topic)
      .records
      .map(_.record.value)
      .interruptAfter(5.seconds)
      .compile
      .toList
  }

  test(
    "a message that fails processing every time is routed to order-events-dlq after exhausting retries, and the stream keeps consuming later messages (TD-2.4)"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      Metrics.test[IO]("notification-kafka-consumers-dlq-suite").use {
        testMeter =>
          for {
            emailsRef <- Ref.of[IO, List[SentEmail]](Nil)
            emailClient = alwaysFailOn("cust-poison", emailsRef)
            poisonEvent = OrderStatusChangedEvent(
              orderId = "order-poison",
              customerId = "cust-poison",
              status = OrderStatusChanged.Confirmed,
              timestamp = Instant.parse("2026-01-01T00:00:00Z")
            )
            healthyEvent = OrderStatusChangedEvent(
              orderId = "order-healthy",
              customerId = "cust-healthy",
              status = OrderStatusChanged.Confirmed,
              timestamp = Instant.parse("2026-01-01T00:00:00Z")
            )
            _ <- produce(
              config,
              OrderStatusChangedConsumer.topic,
              poisonEvent.orderId,
              envelopeJson(
                "OrderStatusChanged",
                poisonEvent.orderId,
                poisonEvent.asJson
              )
            )
            _ <- produce(
              config,
              OrderStatusChangedConsumer.topic,
              healthyEvent.orderId,
              envelopeJson(
                "OrderStatusChanged",
                healthyEvent.orderId,
                healthyEvent.asJson
              )
            )
            healthyFinal <-
              KafkaConsumers
                .supervised[IO](
                  config,
                  emailClient,
                  NoOpLogger[IO],
                  testMeter.meter
                )
                .compile
                .drain
                .background
                .use { _ =>
                  // Proves the stream didn't get stuck/killed on the poison
                  // message - a later, healthy message on the same topic
                  // still gets processed and sends its email normally.
                  awaitEmail(emailsRef)
                }
            dlqMessages <- consumeOneFrom(
              config,
              s"${OrderStatusChangedConsumer.topic}-dlq"
            )
            metrics <- testMeter.collectMetrics
            dlqPublished = metrics
              .filter(_.getName == "purekafka.dlq.published")
              .flatMap(_.getLongSumData.getPoints.asScala)
              .map(_.getValue)
              .sum
          } yield {
            assertEquals(healthyFinal.to, "cust-healthy")
            assert(
              dlqMessages.exists(_.contains("order-poison")),
              s"expected a DLQ message mentioning order-poison, got $dlqMessages"
            )
            assert(
              dlqPublished >= 1L,
              s"expected at least 1 purekafka.dlq.published, got $dlqPublished"
            )
          }
      }
    }
  }
}
