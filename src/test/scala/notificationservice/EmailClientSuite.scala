package notificationservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.typelevel.log4cats.testing.StructuredTestingLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger.INFO

class EmailClientSuite extends CatsEffectSuite {

  test("EmailClient.logging logs the recipient and subject on send") {
    for {
      testLogger <- IO.pure(StructuredTestingLogger.impl[IO]())
      client = EmailClient.logging[IO](testLogger)
      _ <- client.send(
        to = "cust-1",
        subject = "Your order is confirmed",
        body = "Order order-1 has been confirmed."
      )
      logged <- testLogger.logged
    } yield {
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.ctx.get("to").contains("cust-1") &&
            m.ctx.get("subject").contains("Your order is confirmed")
        ),
        s"expected an INFO line logging the recipient and subject, got: $infos"
      )
    }
  }
}
