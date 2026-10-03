package notificationservice

import cats.effect.Sync
import pureconfig.{ConfigReader, ConfigSource}

final case class KafkaConfig(
    bootstrapServers: String
) derives ConfigReader

final case class NotificationServiceConfig(
    port: Int,
    metricsPort: Int,
    serviceName: String,
    kafka: KafkaConfig
) derives ConfigReader

object NotificationServiceConfig {
  def load[F[_]: Sync]: F[NotificationServiceConfig] =
    Sync[F].delay(ConfigSource.default.loadOrThrow[NotificationServiceConfig])
}
