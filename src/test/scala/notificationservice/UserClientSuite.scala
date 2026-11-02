package notificationservice

import cats.effect.{IO, Ref, Resource}
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.client.Client
import org.http4s.{Request, Response, Status, Uri}

class UserClientSuite extends CatsEffectSuite {

  private val baseUri = Uri.unsafeFromString("http://user-service")

  private def stubClient(
      behavior: Request[IO] => Response[IO]
  ): Client[IO] =
    Client[IO](req => Resource.eval(IO(behavior(req))))

  private val userJson =
    io.circe.Json.obj(
      "userId" -> io.circe.Json.fromString("cust-1"),
      "email" -> io.circe.Json.fromString("ada@example.com")
    )

  test("getEmail returns the address and targets GET /users/{id}") {
    for {
      seen <- Ref.of[IO, List[String]](Nil)
      stub = Client[IO] { req =>
        Resource.eval(
          seen
            .update(_ :+ req.uri.path.renderString)
            .as(Response[IO](Status.Ok).withEntity(userJson))
        )
      }
      result <- UserClient[IO](stub, baseUri).getEmail("cust-1")
      paths <- seen.get
    } yield {
      assertEquals(result, Some("ada@example.com"))
      assertEquals(paths, List("/users/cust-1"))
    }
  }

  test("getEmail returns None for 404 and 400") {
    for {
      notFound <- UserClient[IO](
        stubClient(_ => Response[IO](Status.NotFound)),
        baseUri
      ).getEmail("cust-1")
      malformed <- UserClient[IO](
        stubClient(_ => Response[IO](Status.BadRequest)),
        baseUri
      ).getEmail("not-a-uuid")
    } yield {
      assertEquals(notFound, None)
      assertEquals(malformed, None)
    }
  }

  test("getEmail raises on an unexpected status") {
    for {
      result <- UserClient[IO](
        stubClient(_ => Response[IO](Status.InternalServerError)),
        baseUri
      ).getEmail("cust-1").attempt
    } yield assert(result.isLeft)
  }
}
