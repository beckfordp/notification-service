package notificationservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.circe._
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}
import purekafka.docs.{Docs => AsyncApiDocs}

class NotificationAsyncApiDocsSuite extends CatsEffectSuite {

  test("GET /docs/asyncapi.json returns 200 with the registered event type") {
    val routes =
      AsyncApiDocs.routes[IO](
        "Notification Service",
        "1.0",
        AsyncApiChannels.channels
      )
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/asyncapi.json")
      )
      body <- response.bodyText.compile.string
    } yield {
      assertEquals(response.status, Status.Ok)
      assert(clue(body).contains("OrderConfirmed"))
      assert(clue(body).contains("OrderPaymentFailed"))
      assert(clue(body).contains("OrderCancelled"))
      assert(!clue(body).contains("OrderStatusChanged"))
      assert(clue(body).contains("UserRegistered"))
      assert(clue(body).contains("DispatchConfirmed"))
      assert(clue(body).contains("DispatchFailed"))
    }
  }

  test(
    "GET /docs/asyncapi.json: order-events advertises the 3 per-outcome messages as oneOf"
  ) {
    val routes =
      AsyncApiDocs.routes[IO](
        "Notification Service",
        "1.0",
        AsyncApiChannels.channels
      )
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/asyncapi.json")
      )
      json <- response.as[io.circe.Json]
    } yield {
      val message = json.hcursor
        .downField("channels")
        .downField("order-events")
        .downField("subscribe")
        .downField("message")

      val messages = message.downField("oneOf").values.toList.flatten
      val names = messages
        .flatMap(_.hcursor.downField("name").as[String].toOption)
        .toSet

      assertEquals(
        names,
        Set("OrderConfirmed", "OrderPaymentFailed", "OrderCancelled")
      )
    }
  }
}
