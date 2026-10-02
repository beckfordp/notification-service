package notificationservice

import cats.effect.{IO, IOApp}
import com.comcast.ip4s._
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits._
import purerest.docs.Docs
import purerest.logging.Logging
import purerest.metrics.{Metrics, ServerMetrics}
import purerest.tracing.{ServerTracing, Tracing}

object Main extends IOApp.Simple {

  val run: IO[Unit] =
    for {
      config <- NotificationServiceConfig.load[IO]
      port <- IO.fromOption(Port.fromInt(config.port))(
        new IllegalArgumentException(
          s"Invalid notification-service port: ${config.port}"
        )
      )
      _ <- Migrations.run[IO](config.postgres)
      _ <- Tracing.console[IO](config.serviceName).use { tracer =>
        Metrics.oteljava[IO](config.serviceName, config.metricsPort).use {
          meter =>
            for {
              logger <- Logging.create[IO](tracer, config.serviceName)
              _ <- logger.info(
                Map(
                  "port" -> config.port.toString,
                  "metrics_port" -> config.metricsPort.toString
                )
              )("notification-service starting")
              _ <- NotificationStore.postgres[IO](config.postgres, meter).use {
                store =>
                  val docsRoutes = Docs.routes[IO](
                    "Notification Service",
                    "1.0",
                    List(
                      NotificationRoutes.serverEndpoint[IO](store, logger),
                      NotificationRoutes
                        .getNotificationServerEndpoint[IO](store, logger),
                      NotificationRoutes
                        .updateNotificationServerEndpoint[IO](store, logger),
                      NotificationRoutes
                        .replaceNotificationServerEndpoint[IO](store, logger),
                      NotificationRoutes
                        .deleteNotificationServerEndpoint[IO](store, logger),
                      HealthRoutes.healthServerEndpoint[IO],
                      HealthRoutes.readyServerEndpoint[IO](store)
                    )
                  )
                  val tracedRoutes =
                    ServerTracing.middleware(tracer)(docsRoutes)
                  val routes =
                    ServerMetrics.middleware[IO](meter)(tracedRoutes)
                  EmberServerBuilder
                    .default[IO]
                    .withHost(host"0.0.0.0")
                    .withPort(port)
                    .withHttpApp(routes.orNotFound)
                    .build
                    .useForever
              }
            } yield ()
        }
      }
    } yield ()
}
