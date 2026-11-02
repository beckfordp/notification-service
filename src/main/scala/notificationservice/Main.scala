package notificationservice

import cats.effect.{IO, IOApp, Resource}
import com.comcast.ip4s._
import org.http4s.ember.server.EmberServerBuilder

/** Acquires every dependency notification-service needs, builds the routes, and
  * serves them — a single flat `Resource` for-comprehension, one `<-` per
  * dependency, per the composition pattern documented in `purerest`'s
  * `docs/functional-design-patterns-scala-haskell-category-theory.md` ("Scaling
  * tagless final without Cake: bundling dependencies into `Resource`-built
  * modules"). Runs until the Kafka consumers fail (`KafkaConsumers.watched`),
  * then releases everything and exits non-zero so the pod restarts. Mirrors
  * order-service's/payment-service's/inventory-service's own `Main.scala`.
  */
object Main extends IOApp.Simple {

  val run: IO[Unit] = (for {
    config <- Resource.eval(NotificationServiceConfig.load[IO])
    port <- Resource.eval(
      IO.fromOption(Port.fromInt(config.port))(
        new IllegalArgumentException(
          s"Invalid notification-service port: ${config.port}"
        )
      )
    )
    _ <- Migrations.resource[IO](config.postgres)
    telemetry <- Telemetry.resource[IO](config.serviceName, config.metricsPort)
    _ <- Resource.eval(
      telemetry.logger.info(
        Map(
          "port" -> config.port.toString,
          "metrics_port" -> config.metricsPort.toString
        )
      )("notification-service starting")
    )
    emailStore <- SentEmailStore.postgres[IO](config.postgres)
    emailClient = EmailClient.audited[IO](
      emailStore,
      EmailClient.logging[IO](telemetry.logger),
      telemetry.logger
    )
    consumersFailed <- KafkaConsumers.resource[IO](
      config.kafka,
      emailClient,
      telemetry.logger,
      telemetry.meter
    )
    routes = AppRoutes.all[IO](
      telemetry,
      emailStore,
      AsyncApiChannels.channels,
      config.asyncApiHtmlDir
    )
    _ <- EmberServerBuilder
      .default[IO]
      .withHost(host"0.0.0.0")
      .withPort(port)
      .withHttpApp(routes.orNotFound)
      .build
  } yield consumersFailed).use(identity)
}
