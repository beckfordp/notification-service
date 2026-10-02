package notificationservice

import cats.effect.Sync
import pureconfig.{ConfigReader, ConfigSource}

final case class PostgresConfig(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String
) derives ConfigReader

final case class NotificationServiceConfig(
    port: Int,
    metricsPort: Int,
    serviceName: String,
    postgres: PostgresConfig
) derives ConfigReader

object NotificationServiceConfig {
  def load[F[_]: Sync]: F[NotificationServiceConfig] =
    Sync[F].delay(ConfigSource.default.loadOrThrow[NotificationServiceConfig])
}
