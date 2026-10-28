package notificationservice

import io.circe.parser.decode
import io.circe.syntax._
import munit.CatsEffectSuite

class UserRegisteredEventSuite extends CatsEffectSuite {

  test("decodes userId and email") {
    val result = decode[UserRegisteredEvent](
      """{"userId":"user-1","email":"ada@example.com"}"""
    )

    assertEquals(
      result,
      Right(UserRegisteredEvent("user-1", "ada@example.com"))
    )
  }

  test("fails to decode a payload missing email") {
    val result = decode[UserRegisteredEvent]("""{"userId":"user-1"}""")

    assert(result.isLeft, s"expected a decode failure, got $result")
  }

  test("round-trips through encode/decode") {
    val event = UserRegisteredEvent("user-1", "ada@example.com")

    assertEquals(
      decode[UserRegisteredEvent](event.asJson.noSpaces),
      Right(event)
    )
  }

  test("emailFor addresses the event's email with the welcome subject") {
    val (subject, body) =
      UserRegisteredConsumer.emailFor(
        UserRegisteredEvent("user-1", "ada@example.com")
      )

    assertEquals(subject, "Welcome to gshop")
    assert(body.contains("ada@example.com"))
  }
}
