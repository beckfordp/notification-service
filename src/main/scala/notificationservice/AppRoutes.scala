package notificationservice

import cats.effect.Async
import cats.syntax.semigroupk._
import org.http4s.HttpRoutes
import org.http4s.server.staticcontent.{FileService, fileService}
import purekafka.docs.{AsyncApiChannel, Docs => AsyncApiDocs}
import purerest.docs.Docs
import purerest.metrics.ServerMetrics
import purerest.tracing.ServerTracing

/** Assembles notification-service's full set of `HttpRoutes[F]` — the two
  * health endpoints plus their OpenAPI/Swagger docs, AsyncAPI docs (raw spec +
  * rendered static HTML) — and applies this service's full middleware stack
  * (server tracing, server metrics, CORS), in one call. Extracted out of `Main`
  * per the `Resource`-built-module composition pattern documented in
  * `purerest`'s `docs/functional-design-patterns-scala-haskell-category-
  * theory.md` ("Scaling tagless final without Cake") — `Main` should just
  * acquire dependencies and call this once, not inline the route construction
  * itself. Mirrors order-service's/payment-service's/inventory-service's own
  * `AppRoutes.scala`.
  *
  * Unlike those three, there are no business HTTP routes here beyond `/health`,
  * `/health/ready` and ADM-2's read-only `GET /emails` - this service is a
  * Kafka-to-email relay with an audit read API. All of them are still served
  * through `purerest.docs.Docs.routes` so the TD-20 docs convention (and its
  * coverage test) applies to every served endpoint.
  */
object AppRoutes {

  def all[F[_]: Async](
      telemetry: Telemetry[F],
      emailStore: SentEmailStore[F],
      channels: List[AsyncApiChannel[?]],
      staticAsyncApiDocsDir: String
  ): HttpRoutes[F] = {
    val docsRoutes = Docs.routes[F](
      "Notification Service",
      "1.0",
      List(
        HealthRoutes.healthServerEndpoint[F],
        HealthRoutes.readyServerEndpoint[F](emailStore),
        EmailRoutes.serverEndpoint[F](emailStore, telemetry.logger)
      )
    )
    val asyncApiRoutes =
      AsyncApiDocs.routes[F]("Notification Service", "1.0", channels)
    // Serves the sbt-purekafka-docs plugin's generated static HTML at
    // /docs/asyncapi/* - sibling to asyncApiRoutes' raw-spec route, same
    // split as the other services' own Swagger /docs vs /docs/docs.yaml.
    // notification-service runs as a plain `sbt run` JVM process (no docker
    // bind mount), so this reads straight from the local filesystem.
    val staticAsyncApiRoutes = fileService[F](
      FileService.Config(
        systemPath = staticAsyncApiDocsDir,
        pathPrefix = "/docs/asyncapi"
      )
    )
    // asyncApiRoutes/staticAsyncApiRoutes must come before docsRoutes:
    // docsRoutes' Swagger UI bundle serves a wildcard /docs/{resource} route
    // for its own static assets, which returns its own real 404 (not a
    // http4s fallthrough `None`) for any /docs/* path it doesn't recognize —
    // including /docs/asyncapi.json — short-circuiting `<+>` before
    // asyncApiRoutes ever sees the request (same ordering as order-service).
    val tracedRoutes = ServerTracing.middleware(telemetry.tracer)(
      asyncApiRoutes <+> staticAsyncApiRoutes <+> docsRoutes
    )
    val meteredRoutes =
      ServerMetrics.middleware[F](telemetry.meter)(tracedRoutes)
    Cors.middleware[F](meteredRoutes)
  }
}
