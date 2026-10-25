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
      assert(clue(body).contains("OrderStatusChanged"))
    }
  }

  test(
    "GET /docs/asyncapi.json: OrderStatusChanged's status field documents its fixed set of values as a JSON Schema enum"
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
      // Only one event type on this topic - unlike order-service's
      // multiplexed topics, no "oneOf" wrapper, just a single message
      // directly under subscribe.
      val statusPayload = json.hcursor
        .downField("channels")
        .downField("order-events")
        .downField("subscribe")
        .downField("message")
        .downField("payload")
        .downField("properties")
        .downField("status")

      val enumValues = statusPayload
        .downField("enum")
        .values
        .toList
        .flatten
        .flatMap(_.asString)

      assertEquals(
        enumValues.toSet,
        Set("reservation_failed", "confirmed", "payment_failed")
      )
    }
  }
}
