// SPDX-License-Identifier: MIT

package notificationservice

import cats.effect.IO
import fs2.Stream
import munit.CatsEffectSuite
import org.typelevel.log4cats.testing.StructuredTestingLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger.ERROR

import java.util.concurrent.TimeoutException
import scala.concurrent.duration._

/** `KafkaConsumers.watched`: the consumers' background fiber must never die
  * silently - its failure or completion is logged and fails the watcher, so
  * `Main` exits and the pod restarts.
  */
class KafkaConsumersWatchedSuite extends CatsEffectSuite {

  test("a failed consumer stream is logged and fails the watcher") {
    val logger = StructuredTestingLogger.impl[IO]()
    val boom = new RuntimeException("dlq producer unavailable")
    for {
      result <- KafkaConsumers
        .watched[IO](Stream.raiseError[IO](boom), logger)
        .use(identity)
        .attempt
      logged <- logger.logged
    } yield {
      assertEquals(result, Left(boom))
      assert(
        logged.exists {
          case e: ERROR => e.throwOpt.contains(boom); case _ => false
        },
        logged
      )
    }
  }

  test(
    "a consumer stream that completes fails the watcher with ConsumersStopped"
  ) {
    val logger = StructuredTestingLogger.impl[IO]()
    for {
      result <- KafkaConsumers
        .watched[IO](Stream.empty, logger)
        .use(identity)
        .attempt
      logged <- logger.logged
    } yield {
      assertEquals(result, Left(KafkaConsumers.ConsumersStopped))
      assert(logged.exists { case _: ERROR => true; case _ => false }, logged)
    }
  }

  test("a running consumer stream keeps the watcher waiting") {
    val logger = StructuredTestingLogger.impl[IO]()
    KafkaConsumers
      .watched[IO](Stream.never[IO], logger)
      .use(identity)
      .timeout(200.millis)
      .attempt
      .map(r =>
        assert(
          r.left.exists { case _: TimeoutException => true; case _ => false },
          r
        )
      )
  }
}
