package notificationservice

import cats.effect.{IO, Ref}
import io.circe.Json
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.testing.StructuredTestingLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger.{ERROR, INFO}

import java.time.Instant

class EnvelopeHandlerSuite extends CatsEffectSuite {

  private val topic = "test-events"

  private def envelope(eventType: String, payload: Json): String =
    EventEnvelope(
      eventId = "e1",
      eventType = eventType,
      timestamp = Instant.parse("2026-01-01T00:00:00Z"),
      source = "test",
      correlationId = "c1",
      payload = payload
    ).asJson.noSpaces

  private val known = envelope(
    "UserRegistered",
    Json.obj("userId" -> "u1".asJson, "email" -> "a@example.com".asJson)
  )

  private final case class Fixture(
      handled: Ref[IO, List[(String, UserRegisteredEvent)]],
      dlq: TestDeadLetter,
      logger: StructuredTestingLogger[IO],
      run: (String, Option[String]) => IO[Unit]
  )

  private def fixture(fail: Boolean = false): IO[Fixture] =
    for {
      handled <- Ref.of[IO, List[(String, UserRegisteredEvent)]](Nil)
      dlq <- TestDeadLetter.create
      logger = StructuredTestingLogger.impl[IO]()
      routes = List(
        EnvelopeRoute.of[IO, UserRegisteredEvent]("UserRegistered") {
          (eventId, e) =>
            if (fail) IO.raiseError(new RuntimeException("boom"))
            else handled.update(_ :+ (eventId -> e))
        }
      )
    } yield Fixture(
      handled,
      dlq,
      logger,
      EnvelopeHandler.handle[IO](topic, routes, logger, dlq.deadLetter)
    )

  test(
    "a known event type is decoded and handled with the envelope eventId"
  ) {
    for {
      f <- fixture()
      _ <- f.run("k", Some(known))
      handled <- f.handled.get
      routed <- f.dlq.routed.get
    } yield {
      assertEquals(
        handled,
        List("e1" -> UserRegisteredEvent("u1", "a@example.com"))
      )
      assertEquals(routed, Nil)
    }
  }

  test(
    "a handler failure goes through the dead-letter guard with the raw value"
  ) {
    for {
      f <- fixture(fail = true)
      _ <- f.run("k", Some(known))
      routed <- f.dlq.routed.get
    } yield assertEquals(routed, List((topic, "k", known)))
  }

  test("an undecodable envelope is dead-lettered raw and logged without it") {
    for {
      f <- fixture()
      _ <- f.run("k", Some("not json"))
      routed <- f.dlq.routed.get
      logged <- f.logger.logged
    } yield {
      assertEquals(routed, List((topic, "k", "not json")))
      val errors = logged.collect { case m: ERROR => m }
      assert(errors.exists(_.message.contains("envelope")), logged)
      assert(logged.forall(!_.message.contains("not json")), logged)
    }
  }

  test("a known event type with a bad payload is dead-lettered raw") {
    val bad = envelope("UserRegistered", Json.obj("userId" -> "u1".asJson))
    for {
      f <- fixture()
      _ <- f.run("k", Some(bad))
      routed <- f.dlq.routed.get
      handled <- f.handled.get
    } yield {
      assertEquals(routed, List((topic, "k", bad)))
      assertEquals(handled, Nil)
    }
  }

  test("an unknown event type is logged with its type and skipped") {
    for {
      f <- fixture()
      _ <- f.run("k", Some(envelope("SomethingElse", Json.obj())))
      routed <- f.dlq.routed.get
      handled <- f.handled.get
      logged <- f.logger.logged
    } yield {
      assertEquals(routed, Nil)
      assertEquals(handled, Nil)
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(_.ctx.get("event_type").contains("SomethingElse")),
        logged
      )
    }
  }

  test("a null value is logged and skipped") {
    for {
      f <- fixture()
      _ <- f.run("k", None)
      routed <- f.dlq.routed.get
      logged <- f.logger.logged
    } yield {
      assertEquals(routed, Nil)
      assert(logged.exists(_.message.contains("null value")), logged)
    }
  }
}
