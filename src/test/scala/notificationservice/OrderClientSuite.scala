package notificationservice

import cats.effect.{IO, Ref, Resource}
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.client.Client
import org.http4s.{Request, Response, Status, Uri}

class OrderClientSuite extends CatsEffectSuite {

  private val baseUri = Uri.unsafeFromString("http://order-service")

  private def stubClient(
      behavior: Request[IO] => Response[IO]
  ): Client[IO] =
    Client[IO](req => Resource.eval(IO(behavior(req))))

  private val orderJson =
    io.circe.Json.obj(
      "id" -> io.circe.Json.fromString("order-1"),
      "customerId" -> io.circe.Json.fromString("cust-1")
    )

  test("getCustomerId returns the id and targets GET /orders/{id}") {
    for {
      seen <- Ref.of[IO, List[String]](Nil)
      stub = Client[IO] { req =>
        Resource.eval(
          seen
            .update(_ :+ req.uri.path.renderString)
            .as(Response[IO](Status.Ok).withEntity(orderJson))
        )
      }
      result <- OrderClient[IO](stub, baseUri).getCustomerId("order-1")
      paths <- seen.get
    } yield {
      assertEquals(result, Some("cust-1"))
      assertEquals(paths, List("/orders/order-1"))
    }
  }

  test("getCustomerId returns None for 404 and 400") {
    for {
      notFound <- OrderClient[IO](
        stubClient(_ => Response[IO](Status.NotFound)),
        baseUri
      ).getCustomerId("order-1")
      malformed <- OrderClient[IO](
        stubClient(_ => Response[IO](Status.BadRequest)),
        baseUri
      ).getCustomerId("not-a-uuid")
    } yield {
      assertEquals(notFound, None)
      assertEquals(malformed, None)
    }
  }

  test("getCustomerId raises on an unexpected status") {
    for {
      result <- OrderClient[IO](
        stubClient(_ => Response[IO](Status.InternalServerError)),
        baseUri
      ).getCustomerId("order-1").attempt
    } yield assert(result.isLeft)
  }
}
