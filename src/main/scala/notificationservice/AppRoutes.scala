package notificationservice

import cats.effect.Async
import org.http4s.HttpRoutes
import purerest.metrics.ServerMetrics
import purerest.tracing.ServerTracing

/** Assembles notification-service's full set of `HttpRoutes[F]` and applies
  * this service's full middleware stack (server tracing, server metrics, CORS),
  * in one call. Extracted out of `Main` per the `Resource`-built-module
  * composition pattern documented in `purerest`'s
  * `docs/functional-design-patterns-scala-haskell-category-theory.md` ("Scaling
  * tagless final without Cake") — `Main` should just acquire dependencies and
  * call this once, not inline the route construction itself. Mirrors
  * order-service's/payment-service's/inventory-service's own `AppRoutes.scala`.
  *
  * Unlike those three, there are no business HTTP routes here at all - this
  * service is a pure Kafka-to-email relay (no CRUD API, no `purerest.docs.Docs`
  * OpenAPI docs) - so this is just `HealthRoutes` for now (AsyncAPI routes
  * added once registered, see `AsyncApiChannels.scala`).
  */
object AppRoutes {

  def all[F[_]: Async](telemetry: Telemetry[F]): HttpRoutes[F] = {
    val healthRoutes = HealthRoutes.routes[F]
    val tracedRoutes = ServerTracing.middleware(telemetry.tracer)(healthRoutes)
    val meteredRoutes =
      ServerMetrics.middleware[F](telemetry.meter)(tracedRoutes)
    Cors.middleware[F](meteredRoutes)
  }
}
