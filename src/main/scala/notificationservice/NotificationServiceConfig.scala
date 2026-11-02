package notificationservice

import cats.effect.Sync
import org.http4s.Uri
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.error.CannotConvert
import purekafka.consumer.StreamSupervisorConfig
import purekafka.resilience.{
  CircuitBreakerConfig => KafkaCircuitBreakerConfig,
  ResilienceConfig => KafkaResilienceConfig,
  RetryConfig => KafkaRetryConfig
}
import purerest.resilience.{CircuitBreakerConfig, ResilienceConfig, RetryConfig}

import scala.concurrent.duration._

// Client base URLs are parsed/validated as `Uri` at config-load time, so no
// `Uri.unsafeFromString` can throw later during wiring (TD-15.1).
given ConfigReader[Uri] =
  ConfigReader[String].emap { raw =>
    Uri.fromString(raw) match {
      case Right(uri)    => Right(uri)
      case Left(failure) =>
        Left(CannotConvert(raw, "Uri", failure.message))
    }
  }

// purerest's resilience case classes don't derive ConfigReader themselves
// (the library has no PureConfig dependency) - instances are derived here,
// the same way order-service's OrderServiceConfig does.
given ConfigReader[RetryConfig] = ConfigReader.derived
given ConfigReader[CircuitBreakerConfig] = ConfigReader.derived
given ConfigReader[ResilienceConfig] = ConfigReader.derived

// purekafka's own resilience/consumer config case classes don't derive
// ConfigReader themselves (the library has no PureConfig dependency) -
// instances are derived here, mirroring order-service's OrderServiceConfig.
given ConfigReader[KafkaRetryConfig] = ConfigReader.derived
given ConfigReader[KafkaCircuitBreakerConfig] = ConfigReader.derived
given ConfigReader[KafkaResilienceConfig] = ConfigReader.derived
given ConfigReader[StreamSupervisorConfig] = ConfigReader.derived

// Defaults below are for direct Scala construction only (e.g. test helpers
// that build a KafkaConfig without caring about these three fields) -
// confirmed empirically (order-service's own OrderServiceConfig) that this
// version's ConfigReader.derived does *not* fall back to them when a key is
// missing from HOCON (KeyNotFound instead), so application.conf always
// specifies all three explicitly.
final case class KafkaConfig(
    bootstrapServers: String,
    // Message-processing retry before a message is routed to the DLQ - see
    // KafkaConsumers.scala's DeadLetter usage.
    dlqRetry: KafkaRetryConfig =
      KafkaRetryConfig(maxRetries = 3, baseDelay = 200.millis),
    // Guards the DLQ's own publish call. Defaults match order-service's/
    // payment-service's own KafkaConfig, for consistency across services.
    dlqPublishResilience: KafkaResilienceConfig = KafkaResilienceConfig(
      retry = KafkaRetryConfig(maxRetries = 3, baseDelay = 100.millis),
      circuitBreaker = KafkaCircuitBreakerConfig(
        failureThreshold = 5,
        resetTimeout = 30.seconds
      )
    ),
    // Restart-with-backoff for a crashed consumer stream (TD-2.3, ADR 0010) -
    // see KafkaConsumers.scala's StreamSupervisor usage.
    streamSupervisor: StreamSupervisorConfig =
      StreamSupervisorConfig(baseDelay = 1.second, maxDelay = 30.seconds)
) derives ConfigReader

final case class PostgresConfig(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String
) derives ConfigReader

final case class UserClientConfig(
    baseUrl: Uri,
    resilience: ResilienceConfig
) derives ConfigReader

final case class OrderClientConfig(
    baseUrl: Uri,
    resilience: ResilienceConfig
) derives ConfigReader

final case class NotificationServiceConfig(
    port: Int,
    metricsPort: Int,
    serviceName: String,
    asyncApiHtmlDir: String,
    postgres: PostgresConfig,
    kafka: KafkaConfig,
    userClient: UserClientConfig,
    orderClient: OrderClientConfig
) derives ConfigReader

object NotificationServiceConfig {
  def load[F[_]: Sync]: F[NotificationServiceConfig] =
    Sync[F].delay(ConfigSource.default.loadOrThrow[NotificationServiceConfig])
}
