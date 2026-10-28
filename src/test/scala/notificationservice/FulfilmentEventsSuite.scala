package notificationservice

import io.circe.parser.decode
import io.circe.syntax._
import munit.CatsEffectSuite

class FulfilmentEventsSuite extends CatsEffectSuite {

  private val item = DispatchItem("sku-1", "Watch", 2, 1000)

  test("decodes DispatchConfirmed with the item snapshot") {
    val event = DispatchConfirmedEvent("order-1", "GLN-1", List(item))

    assertEquals(
      decode[DispatchConfirmedEvent](event.asJson.noSpaces),
      Right(event)
    )
  }

  test("decodes DispatchFailed with the reason") {
    val event = DispatchFailedEvent("order-1", "damaged")

    assertEquals(
      decode[DispatchFailedEvent](event.asJson.noSpaces),
      Right(event)
    )
  }

  test("shippedEmail names the order and tracking number") {
    val (subject, body) =
      FulfilmentEventConsumer.shippedEmail(
        DispatchConfirmedEvent("order-1", "GLN-1", List(item))
      )

    assertEquals(subject, "Your order has shipped")
    assert(body.contains("order-1"))
    assert(body.contains("GLN-1"))
  }

  test("failedEmail names the order and reason") {
    val (subject, body) =
      FulfilmentEventConsumer.failedEmail(
        DispatchFailedEvent("order-1", "damaged")
      )

    assertEquals(subject, "We couldn't ship your order")
    assert(body.contains("order-1"))
    assert(body.contains("damaged"))
  }
}
