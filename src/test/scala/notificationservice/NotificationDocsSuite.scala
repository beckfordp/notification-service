// SPDX-License-Identifier: MIT

package notificationservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}
import org.typelevel.log4cats.testing.StructuredTestingLogger
import purerest.docs.Docs
import sttp.tapir.server.ServerEndpoint

import java.util.UUID

class NotificationDocsSuite extends CatsEffectSuite {

  private val store = new SentEmailStore[IO] {
    def claim(email: NewSentEmail): IO[EmailClaim] =
      IO.pure(EmailClaim.Send(email.id))
    def markSent(id: UUID): IO[Unit] = IO.unit
    def markFailed(id: UUID, error: String): IO[Unit] = IO.unit
    def markResolved(
        id: UUID,
        customerId: Option[String],
        recipientEmail: Option[String]
    ): IO[Unit] = IO.unit
    def markUnresolved(id: UUID, reason: String): IO[Unit] = IO.unit
    def get(sourceEventId: String): IO[Option[SentEmailRow]] = IO.pure(None)
    def list(
        customerId: Option[String],
        orderId: Option[String]
    ): IO[List[SentEmailRow]] = IO.pure(Nil)
    def ping: IO[Boolean] = IO.pure(true)
  }

  /** The exact endpoints `AppRoutes` passes to `Docs.routes` (TD-20.9). */
  private def endpoints: List[ServerEndpoint[Any, IO]] =
    List(
      HealthRoutes.healthServerEndpoint[IO],
      HealthRoutes.readyServerEndpoint[IO](store),
      EmailRoutes.serverEndpoint[IO](
        store,
        StructuredTestingLogger.impl[IO]()
      )
    )

  test("the health and email endpoints are served and documented") {
    val routes = Docs.routes[IO]("Notification Service", "1.0", endpoints)
    for {
      health <- routes.orNotFound.run(Request[IO](Method.GET, uri"/health"))
      ready <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/health/ready")
      )
      emails <- routes.orNotFound.run(Request[IO](Method.GET, uri"/emails"))
      docsResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/docs.yaml")
      )
      docsBody <- docsResponse.bodyText.compile.string
    } yield {
      assertEquals(health.status, Status.Ok)
      assertEquals(ready.status, Status.Ok)
      assertEquals(emails.status, Status.Ok)
      assertEquals(docsResponse.status, Status.Ok)
      assert(clue(docsBody).contains("/health"))
      assert(clue(docsBody).contains("/emails"))
    }
  }

  test("every endpoint is documented") {
    assertEquals(
      Docs.undocumented(endpoints.map(_.endpoint)).map(_.render),
      Nil
    )
  }
}
