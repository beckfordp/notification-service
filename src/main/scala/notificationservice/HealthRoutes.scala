package notificationservice

import cats.Applicative
import cats.effect.Async
import org.http4s.HttpRoutes
import sttp.model.StatusCode
import sttp.tapir._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter

/** `/health/ready` always returns 200, same as `/health` - this service has no
  * DB/store to gate readiness on, and the one thing that previously drove it
  * (`OrderStatusChangedConsumer`'s hand-rolled `ready` ref, flipped to `false`
  * on a stream error with no way back) is gone: `purekafka`'s
  * `StreamSupervisor` (TD-2.3, ADR 0010) now restarts that stream with backoff
  * instead of leaving it permanently dead, so there's no "stuck forever" state
  * left to signal. See `KafkaConsumers.scala`.
  */
object HealthRoutes {

  private val healthEndpoint: PublicEndpoint[Unit, Unit, Unit, Any] =
    endpoint.get.in("health").out(statusCode(StatusCode.Ok))

  private val readyEndpoint: PublicEndpoint[Unit, Unit, Unit, Any] =
    endpoint.get.in("health" / "ready").out(statusCode(StatusCode.Ok))

  def healthServerEndpoint[F[_]: Applicative]: ServerEndpoint[Any, F] =
    healthEndpoint.serverLogicSuccess[F](_ => Applicative[F].unit)

  def readyServerEndpoint[F[_]: Applicative]: ServerEndpoint[Any, F] =
    readyEndpoint.serverLogicSuccess[F](_ => Applicative[F].unit)

  def routes[F[_]: Async]: HttpRoutes[F] =
    Http4sServerInterpreter[F]().toRoutes(
      List(healthServerEndpoint[F], readyServerEndpoint[F])
    )
}
