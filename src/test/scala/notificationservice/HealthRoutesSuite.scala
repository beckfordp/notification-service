package notificationservice

import cats.effect.{IO, Ref}
import munit.CatsEffectSuite
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}

class HealthRoutesSuite extends CatsEffectSuite {

  test("GET /health returns 200") {
    for {
      ready <- Ref.of[IO, Boolean](true)
      routes = HealthRoutes.routes[IO](ready)
      response <- routes.orNotFound.run(Request[IO](Method.GET, uri"/health"))
    } yield assertEquals(response.status, Status.Ok)
  }

  test("GET /health/ready returns 200 when the ready ref is true") {
    for {
      ready <- Ref.of[IO, Boolean](true)
      routes = HealthRoutes.routes[IO](ready)
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/health/ready")
      )
    } yield assertEquals(response.status, Status.Ok)
  }

  test("GET /health/ready returns 503 when the ready ref is false") {
    for {
      ready <- Ref.of[IO, Boolean](false)
      routes = HealthRoutes.routes[IO](ready)
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/health/ready")
      )
    } yield assertEquals(response.status, Status.ServiceUnavailable)
  }
}
