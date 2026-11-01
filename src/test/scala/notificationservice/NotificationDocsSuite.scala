package notificationservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}
import purerest.docs.Docs
import sttp.tapir.server.ServerEndpoint

class NotificationDocsSuite extends CatsEffectSuite {

  /** The exact endpoints `AppRoutes` passes to `Docs.routes` (TD-20.9).
    */
  private def endpoints: List[ServerEndpoint[Any, IO]] =
    List(
      HealthRoutes.healthServerEndpoint[IO],
      HealthRoutes.readyServerEndpoint[IO]
    )

  test("the health endpoints are served and documented via purerest.docs") {
    val routes = Docs.routes[IO]("Notification Service", "1.0", endpoints)
    for {
      health <- routes.orNotFound.run(Request[IO](Method.GET, uri"/health"))
      ready <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/health/ready")
      )
      docsResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/docs.yaml")
      )
      docsBody <- docsResponse.bodyText.compile.string
    } yield {
      assertEquals(health.status, Status.Ok)
      assertEquals(ready.status, Status.Ok)
      assertEquals(docsResponse.status, Status.Ok)
      assert(clue(docsBody).contains("/health"))
    }
  }

  test("every endpoint is documented") {
    assertEquals(
      Docs.undocumented(endpoints.map(_.endpoint)).map(_.render),
      Nil
    )
  }
}
