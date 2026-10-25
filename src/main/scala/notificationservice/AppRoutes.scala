package notificationservice

import cats.effect.Async
import cats.syntax.semigroupk._
import org.http4s.HttpRoutes
import org.http4s.server.staticcontent.{FileService, fileService}
import purekafka.docs.{AsyncApiChannel, Docs => AsyncApiDocs}
import purerest.metrics.ServerMetrics
import purerest.tracing.ServerTracing

/** Assembles notification-service's full set of `HttpRoutes[F]` — health routes
  * and AsyncAPI docs (raw spec + rendered static HTML) — and applies this
  * service's full middleware stack (server tracing, server metrics, CORS), in
  * one call. Extracted out of `Main` per the `Resource`-built-module
  * composition pattern documented in `purerest`'s
  * `docs/functional-design-patterns-scala-haskell-category-theory.md` ("Scaling
  * tagless final without Cake") — `Main` should just acquire dependencies and
  * call this once, not inline the route construction itself. Mirrors
  * order-service's/payment-service's/inventory-service's own `AppRoutes.scala`.
  *
  * Unlike those three, there are no business HTTP routes here at all - this
  * service is a pure Kafka-to-email relay (no CRUD API, no `purerest.docs.Docs`
  * OpenAPI docs).
  */
object AppRoutes {

  def all[F[_]: Async](
      telemetry: Telemetry[F],
      channels: List[AsyncApiChannel[?]]
  ): HttpRoutes[F] = {
    val healthRoutes = HealthRoutes.routes[F]
    val asyncApiRoutes =
      AsyncApiDocs.routes[F]("Notification Service", "1.0", channels)
    // Serves the sbt-purekafka-docs plugin's generated static HTML at
    // /docs/asyncapi/* - sibling to asyncApiRoutes' raw-spec route, same
    // split as the other services' own Swagger /docs vs /docs/docs.yaml.
    // notification-service runs as a plain `sbt run` JVM process (no docker
    // bind mount), so this reads straight from the local filesystem.
    val staticAsyncApiDocsDir =
      sys.env.getOrElse("ASYNCAPI_HTML_DIR", "target/asyncapi-docs")
    val staticAsyncApiRoutes = fileService[F](
      FileService.Config(
        systemPath = staticAsyncApiDocsDir,
        pathPrefix = "/docs/asyncapi"
      )
    )
    val tracedRoutes = ServerTracing.middleware(telemetry.tracer)(
      asyncApiRoutes <+> staticAsyncApiRoutes <+> healthRoutes
    )
    val meteredRoutes =
      ServerMetrics.middleware[F](telemetry.meter)(tracedRoutes)
    Cors.middleware[F](meteredRoutes)
  }
}
