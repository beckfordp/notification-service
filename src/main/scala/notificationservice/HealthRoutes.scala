// SPDX-License-Identifier: MIT

package notificationservice

import cats.{Applicative, Monad}
import cats.effect.Async
import cats.syntax.all._
import org.http4s.HttpRoutes
import sttp.model.StatusCode
import sttp.tapir._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter

/** `/health/ready` is gated on the sent-email audit store (ADM-2/ADM-6): with
  * Postgres unreachable no email can be claimed, so the pod must not take
  * traffic. Kafka consumers are not part of readiness - `purekafka`'s
  * `StreamSupervisor` (TD-2.3, ADR 0010) restarts them with backoff instead of
  * leaving them permanently dead. See `KafkaConsumers.scala`.
  */
object HealthRoutes {

  private val healthEndpoint: PublicEndpoint[Unit, Unit, Unit, Any] =
    endpoint.get
      .in("health")
      .out(statusCode(StatusCode.Ok))
      .summary("Liveness check")
      .description("200 while the process is up. Checks no dependencies.")
      .tag("health")

  private val readyEndpoint: PublicEndpoint[Unit, Unit, Unit, Any] =
    endpoint.get
      .in("health" / "ready")
      .out(statusCode(StatusCode.Ok))
      .errorOut(
        statusCode(StatusCode.ServiceUnavailable)
          .description("Postgres is unreachable.")
      )
      .summary("Readiness check")
      .description(
        "200 when the sent-email audit store (Postgres) is reachable, 503 " +
          "otherwise. Kafka consumers are supervised with restart-with-" +
          "backoff, so they do not gate readiness."
      )
      .tag("health")

  def healthServerEndpoint[F[_]: Applicative]: ServerEndpoint[Any, F] =
    healthEndpoint.serverLogicSuccess[F](_ => Applicative[F].unit)

  def readyServerEndpoint[F[_]: Monad](
      store: SentEmailStore[F]
  ): ServerEndpoint[Any, F] =
    readyEndpoint.serverLogic[F] { _ =>
      store.ping.map(ready => if (ready) Right(()) else Left(()))
    }

  def routes[F[_]: Async](store: SentEmailStore[F]): HttpRoutes[F] =
    Http4sServerInterpreter[F]().toRoutes(
      List(healthServerEndpoint[F], readyServerEndpoint(store))
    )
}
