package notificationservice

import cats.effect.{IO, IOApp, Ref}
import com.comcast.ip4s._
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits._
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
              readyRef <- Ref.of[IO, Boolean](true)
              emailClient = EmailClient.logging[IO](logger)
              _ <- OrderStatusChangedConsumer
                .run[IO](config.kafka, emailClient, logger, readyRef)
                .compile
                .drain
                .background
                .use { _ =>
                  val healthRoutes = HealthRoutes.routes[IO](readyRef)
                  val tracedRoutes =
                    ServerTracing.middleware(tracer)(healthRoutes)
                  val routes = ServerMetrics.middleware[IO](meter)(tracedRoutes)
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
