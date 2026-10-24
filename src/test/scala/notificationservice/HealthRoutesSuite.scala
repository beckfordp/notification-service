package notificationservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}

class HealthRoutesSuite extends CatsEffectSuite {

  test("GET /health returns 200") {
    val routes = HealthRoutes.routes[IO]
    routes.orNotFound
      .run(Request[IO](Method.GET, uri"/health"))
      .map(response => assertEquals(response.status, Status.Ok))
  }

  test("GET /health/ready returns 200") {
    val routes = HealthRoutes.routes[IO]
    routes.orNotFound
      .run(Request[IO](Method.GET, uri"/health/ready"))
      .map(response => assertEquals(response.status, Status.Ok))
  }
}
