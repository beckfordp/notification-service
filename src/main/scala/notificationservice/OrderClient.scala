package notificationservice

import cats.effect.{Async, Concurrent}
import cats.syntax.all._
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.{Method, Request, Status, Uri}
import purerest.resilience.Resilience

/** Who an order belongs to: its customerId and, for a guest checkout (ADM-10),
  * the email captured at checkout.
  */
final case class OrderContact(customerId: String, guestEmail: Option[String])

/** The orderId → customer lookup (ADM-3/ADR 0025), calling order-service's
  * existing `GET /orders/{id}` (contract in `gluon/docs/system-design.md`).
  * Payment/fulfilment events carry only the orderId, so this is the first hop
  * of their recipient resolution; a guest order's email is the fallback when
  * the customerId has no account (ADM-10). Resilience is wrapped by the caller.
  */
trait OrderClient[F[_]] {

  /** `None` when order-service says the order is unknown or malformed
    * (400/404): a permanently unresolved recipient, never retried. Any other
    * unexpected response or transport error raises, so the caller retries.
    */
  def getContact(orderId: String): F[Option[OrderContact]]
}

object OrderClient {

  private final case class OrderBody(
      customerId: String,
      guestEmail: Option[String]
  )

  private object OrderBody {
    implicit val decoder: Decoder[OrderBody] = deriveDecoder
  }

  def resilient[F[_]: Async](
      httpClient: Client[F],
      config: OrderClientConfig,
      telemetry: Telemetry[F]
  ): OrderClient[F] =
    apply(
      Resilience.middleware[F](config.resilience)(telemetry.logger)(
        telemetry.meter
      )(httpClient),
      config.baseUrl
    )

  def apply[F[_]: Concurrent](
      client: Client[F],
      baseUri: Uri
  ): OrderClient[F] =
    new OrderClient[F] {
      def getContact(orderId: String): F[Option[OrderContact]] =
        client
          .run(Request[F](Method.GET, baseUri / "orders" / orderId))
          .use { response =>
            response.status match {
              case Status.Ok =>
                response
                  .as[OrderBody]
                  .map(body =>
                    Some(OrderContact(body.customerId, body.guestEmail))
                  )
              case Status.BadRequest | Status.NotFound =>
                Concurrent[F].pure(None)
              case other =>
                Concurrent[F].raiseError(
                  new RuntimeException(
                    s"Unexpected response from order-service for $orderId: $other"
                  )
                )
            }
          }
    }
}
