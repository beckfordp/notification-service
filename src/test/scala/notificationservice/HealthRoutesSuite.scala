// SPDX-License-Identifier: MIT

package notificationservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}

import java.util.UUID

class HealthRoutesSuite extends CatsEffectSuite {

  private def store(reachable: Boolean): SentEmailStore[IO] =
    new SentEmailStore[IO] {
      def claim(email: NewSentEmail): IO[EmailClaim] =
        IO.raiseError(new NotImplementedError)
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
      def ping: IO[Boolean] = IO.pure(reachable)
    }

  test("GET /health returns 200 without checking Postgres") {
    HealthRoutes
      .routes[IO](store(reachable = false))
      .orNotFound
      .run(Request[IO](Method.GET, uri"/health"))
      .map(response => assertEquals(response.status, Status.Ok))
  }

  test("GET /health/ready returns 200 when Postgres is reachable") {
    HealthRoutes
      .routes[IO](store(reachable = true))
      .orNotFound
      .run(Request[IO](Method.GET, uri"/health/ready"))
      .map(response => assertEquals(response.status, Status.Ok))
  }

  test("GET /health/ready returns 503 when Postgres is unreachable") {
    HealthRoutes
      .routes[IO](store(reachable = false))
      .orNotFound
      .run(Request[IO](Method.GET, uri"/health/ready"))
      .map(response => assertEquals(response.status, Status.ServiceUnavailable))
  }
}
