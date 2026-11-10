// SPDX-License-Identifier: MIT

package notificationservice

import cats.effect.{Async, Concurrent}
import cats.syntax.all._
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.{Method, Request, Status, Uri}
import purerest.resilience.Resilience

/** The customerId → email lookup (ADM-3/ADR 0025), calling user-service's
  * `GET /users/{id}` (contract in `gluon/docs/system-design.md`). `client` is
  * expected to be wrapped with `purerest.resilience.Resilience.middleware` by
  * the caller — this type only knows the one endpoint.
  */
trait UserClient[F[_]] {

  /** `None` when user-service says the id is unknown or malformed (400/404): a
    * permanently unresolved recipient, never retried. Any other unexpected
    * response or transport error raises, so the caller's DLQ guard retries.
    */
  def getEmail(customerId: String): F[Option[String]]
}

object UserClient {

  private final case class UserBody(email: String)

  private object UserBody {
    implicit val decoder: Decoder[UserBody] = deriveDecoder
  }

  def resilient[F[_]: Async](
      httpClient: Client[F],
      config: UserClientConfig,
      telemetry: Telemetry[F]
  ): UserClient[F] =
    apply(
      Resilience.middleware[F](config.resilience)(telemetry.logger)(
        telemetry.meter
      )(httpClient),
      config.baseUrl
    )

  def apply[F[_]: Concurrent](
      client: Client[F],
      baseUri: Uri
  ): UserClient[F] =
    new UserClient[F] {
      def getEmail(customerId: String): F[Option[String]] =
        client
          .run(Request[F](Method.GET, baseUri / "users" / customerId))
          .use { response =>
            response.status match {
              case Status.Ok =>
                response.as[UserBody].map(body => Some(body.email))
              case Status.BadRequest | Status.NotFound =>
                Concurrent[F].pure(None)
              case other =>
                Concurrent[F].raiseError(
                  new RuntimeException(
                    s"Unexpected response from user-service for $customerId: $other"
                  )
                )
            }
          }
    }
}
