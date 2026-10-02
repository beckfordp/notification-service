package notificationservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger.{
  ERROR,
  INFO,
  WARN
}
import purerest.tracing.{ServerTracing, Tracing}

class NotificationRoutesSuite extends CatsEffectSuite {

  private def failingStore(error: Throwable): NotificationStore[IO] =
    new NotificationStore[IO] {
      def create(/* codegen:fields:CREATE_PARAMS */): IO[Notification] =
        IO.raiseError(error)
      def get(id: String): IO[Option[Notification]] = IO.pure(None)
      def update(
          id: String
          /* codegen:fields:UPDATE_PARAMS */
      ): IO[Option[Notification]] =
        IO.raiseError(error)
      def delete(id: String): IO[Boolean] = IO.raiseError(error)
      def ping: IO[Boolean] = IO.raiseError(error)
    }

  test("POST /notifications returns 201 with the created entity") {
    for {
      store <- NotificationStore.inMemory[IO]
      routes = NotificationRoutes.routes[IO](store, NoOpLogger[IO])
      request = Request[IO](Method.POST, uri"/notifications")
        .withEntity(CreateNotificationRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */))
      response <- routes.orNotFound.run(request)
      entity <- response.as[NotificationResponse]
    } yield {
      assertEquals(response.status, Status.Created)
      assert(entity.id.nonEmpty)
    }
  }

  test("GET /notifications/{id} returns 200 with the persisted entity") {
    for {
      store <- NotificationStore.inMemory[IO]
      routes = NotificationRoutes.routes[IO](store, NoOpLogger[IO])
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/notifications").withEntity(
          CreateNotificationRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      created <- postResponse.as[NotificationResponse]
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/notifications" / created.id)
      )
      fetched <- getResponse.as[NotificationResponse]
    } yield {
      assertEquals(getResponse.status, Status.Ok)
      assertEquals(fetched, created)
    }
  }

  test(
    "GET /notifications/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- NotificationStore.inMemory[IO]
      routes = NotificationRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/notifications" / "unknown-id")
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "POST /notifications logs a received-request line and a completed line with structured context"
  ) {
    for {
      store <- NotificationStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = NotificationRoutes.routes[IO](store, testLogger)
      request = Request[IO](Method.POST, uri"/notifications")
        .withEntity(CreateNotificationRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */))
      response <- routes.orNotFound.run(request)
      entity <- response.as[NotificationResponse]
      logged <- testLogger.logged
    } yield {
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("POST")
        ),
        s"expected a received-request INFO line with method context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("notification_id").contains(entity.id)
        ),
        s"expected a completed INFO line with notification_id context, got: $infos"
      )
    }
  }

  test(
    "POST /notifications logs an ERROR with the raised throwable when persisting the entity fails"
  ) {
    val boom = new RuntimeException("boom")
    for {
      testLogger <- IO.pure(StructuredTestingLogger.impl[IO]())
      routes = NotificationRoutes.routes[IO](failingStore(boom), testLogger)
      request = Request[IO](Method.POST, uri"/notifications")
        .withEntity(CreateNotificationRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */))
      response <- routes.orNotFound.run(request)
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.InternalServerError)
      val errors = logged.collect { case m: ERROR => m }
      assert(
        errors.exists(m => m.throwOpt.contains(boom)),
        s"expected an ERROR line with the raised throwable, got: $errors"
      )
    }
  }

  test(
    "GET /notifications/{id} logs a received-request line and a completed line for a found entity"
  ) {
    for {
      store <- NotificationStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = NotificationRoutes.routes[IO](store, testLogger)
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/notifications").withEntity(
          CreateNotificationRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      created <- postResponse.as[NotificationResponse]
      _ <- testLogger.logged // drain POST's own log lines before the GET
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/notifications" / created.id)
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(getResponse.status, Status.Ok)
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("GET") &&
            m.ctx.get("notification_id").contains(created.id)
        ),
        s"expected a received-request INFO line with method/id context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("notification_id").contains(created.id)
        ),
        s"expected a completed INFO line with id context, got: $infos"
      )
    }
  }

  test(
    "GET /notifications/{id} logs a WARN for an unknown id"
  ) {
    for {
      store <- NotificationStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = NotificationRoutes.routes[IO](store, testLogger)
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/notifications" / "unknown-id")
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("notification_id").contains("unknown-id")
        ),
        s"expected a 'not found' WARN line with id context, got: $warns"
      )
    }
  }

  test("PATCH /notifications/{id} returns 200 with the updated entity") {
    for {
      store <- NotificationStore.inMemory[IO]
      routes = NotificationRoutes.routes[IO](store, NoOpLogger[IO])
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/notifications").withEntity(
          CreateNotificationRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      created <- postResponse.as[NotificationResponse]
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/notifications" / created.id)
          .withEntity(UpdateNotificationRequest(/* codegen:fields:TEST_UPDATE_REQUEST_ARGS */))
      )
      updated <- patchResponse.as[NotificationResponse]
    } yield {
      assertEquals(patchResponse.status, Status.Ok)
      assertEquals(updated.id, created.id)
    }
  }

  test(
    "PATCH /notifications/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- NotificationStore.inMemory[IO]
      routes = NotificationRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/notifications" / "unknown-id")
          .withEntity(UpdateNotificationRequest(/* codegen:fields:TEST_UPDATE_REQUEST_ARGS */))
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "PATCH /notifications/{id} logs a received-request line and a completed line for a found entity"
  ) {
    for {
      store <- NotificationStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = NotificationRoutes.routes[IO](store, testLogger)
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/notifications").withEntity(
          CreateNotificationRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      created <- postResponse.as[NotificationResponse]
      _ <- testLogger.logged // drain POST's own log lines before the PATCH
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/notifications" / created.id)
          .withEntity(UpdateNotificationRequest(/* codegen:fields:TEST_UPDATE_REQUEST_ARGS */))
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(patchResponse.status, Status.Ok)
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("PATCH") &&
            m.ctx.get("notification_id").contains(created.id)
        ),
        s"expected a received-request INFO line with method/id context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("notification_id").contains(created.id)
        ),
        s"expected a completed INFO line with id context, got: $infos"
      )
    }
  }

  test("PATCH /notifications/{id} logs a WARN for an unknown id") {
    for {
      store <- NotificationStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = NotificationRoutes.routes[IO](store, testLogger)
      response <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/notifications" / "unknown-id")
          .withEntity(UpdateNotificationRequest(/* codegen:fields:TEST_UPDATE_REQUEST_ARGS */))
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("notification_id").contains("unknown-id")
        ),
        s"expected a 'not found' WARN line with id context, got: $warns"
      )
    }
  }

  test(
    "DELETE /notifications/{id} returns 204, and a subsequent GET returns 404"
  ) {
    for {
      store <- NotificationStore.inMemory[IO]
      routes = NotificationRoutes.routes[IO](store, NoOpLogger[IO])
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/notifications").withEntity(
          CreateNotificationRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      created <- postResponse.as[NotificationResponse]
      deleteResponse <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/notifications" / created.id)
      )
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/notifications" / created.id)
      )
    } yield {
      assertEquals(deleteResponse.status, Status.NoContent)
      assertEquals(getResponse.status, Status.NotFound)
    }
  }

  test(
    "DELETE /notifications/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- NotificationStore.inMemory[IO]
      routes = NotificationRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/notifications" / "unknown-id")
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "DELETE /notifications/{id} logs a received-request line and a completed line for a found entity"
  ) {
    for {
      store <- NotificationStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = NotificationRoutes.routes[IO](store, testLogger)
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/notifications").withEntity(
          CreateNotificationRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      created <- postResponse.as[NotificationResponse]
      _ <- testLogger.logged // drain POST's own log lines before the DELETE
      deleteResponse <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/notifications" / created.id)
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(deleteResponse.status, Status.NoContent)
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("DELETE") &&
            m.ctx.get("notification_id").contains(created.id)
        ),
        s"expected a received-request INFO line with method/id context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("notification_id").contains(created.id)
        ),
        s"expected a completed INFO line with id context, got: $infos"
      )
    }
  }

  test("DELETE /notifications/{id} logs a WARN for an unknown id") {
    for {
      store <- NotificationStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = NotificationRoutes.routes[IO](store, testLogger)
      response <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/notifications" / "unknown-id")
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("notification_id").contains("unknown-id")
        ),
        s"expected a 'not found' WARN line with id context, got: $warns"
      )
    }
  }

  test("PUT /notifications/{id} returns 200 with the replaced entity") {
    for {
      store <- NotificationStore.inMemory[IO]
      routes = NotificationRoutes.routes[IO](store, NoOpLogger[IO])
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/notifications").withEntity(
          CreateNotificationRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      created <- postResponse.as[NotificationResponse]
      putResponse <- routes.orNotFound.run(
        Request[IO](Method.PUT, uri"/notifications" / created.id)
          .withEntity(UpdateNotificationRequest(/* codegen:fields:TEST_UPDATE_REQUEST_ARGS */))
      )
      replaced <- putResponse.as[NotificationResponse]
    } yield {
      assertEquals(putResponse.status, Status.Ok)
      assertEquals(replaced.id, created.id)
    }
  }

  test(
    "PUT /notifications/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- NotificationStore.inMemory[IO]
      routes = NotificationRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.PUT, uri"/notifications" / "unknown-id")
          .withEntity(UpdateNotificationRequest(/* codegen:fields:TEST_UPDATE_REQUEST_ARGS */))
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "wrapped routes (with tracing middleware) record a span for a handled request"
  ) {
    Tracing.test[IO]("notification-service-test").use { testTracer =>
      for {
        store <- NotificationStore.inMemory[IO]
        routes = ServerTracing.middleware(testTracer.tracer)(
          NotificationRoutes.routes[IO](store, NoOpLogger[IO])
        )
        request = Request[IO](Method.POST, uri"/notifications")
          .withEntity(CreateNotificationRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */))
        response <- routes.orNotFound.run(request)
        spans <- testTracer.finishedSpans
      } yield {
        assertEquals(response.status, Status.Created)
        assertEquals(spans.map(_.getName), List("POST /notifications"))
      }
    }
  }
}
