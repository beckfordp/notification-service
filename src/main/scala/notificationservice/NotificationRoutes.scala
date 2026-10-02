package notificationservice

import cats.effect.Async
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.http4s.HttpRoutes
import org.typelevel.log4cats.StructuredLogger
import sttp.model.StatusCode
import sttp.tapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter

final case class CreateNotificationRequest( /* codegen:fields:CREATE_PARAMS */ )

object CreateNotificationRequest {
  implicit val codec: Codec[CreateNotificationRequest] = deriveCodec
}

final case class UpdateNotificationRequest( /* codegen:fields:UPDATE_PARAMS */ )

object UpdateNotificationRequest {
  implicit val codec: Codec[UpdateNotificationRequest] = deriveCodec
}

final case class NotificationResponse(
    id: String,
    // codegen:fields:CASE_CLASS_FIELD
    createdAt: java.time.Instant,
    updatedAt: java.time.Instant
)

object NotificationResponse {
  implicit val codec: Codec[NotificationResponse] = deriveCodec

  def apply(entity: Notification): NotificationResponse =
    NotificationResponse(
      entity.id,
      /* codegen:fields:RESPONSE_APPLY_ARGS */
      entity.createdAt,
      entity.updatedAt
    )
}

final case class ErrorResponse(error: String)

object ErrorResponse {
  implicit val codec: Codec[ErrorResponse] = deriveCodec
}

object NotificationRoutes {

  private val createNotificationEndpoint: PublicEndpoint[
    CreateNotificationRequest,
    Unit,
    NotificationResponse,
    Any
  ] =
    endpoint.post
      .in("notifications")
      .in(jsonBody[CreateNotificationRequest])
      .out(statusCode(StatusCode.Created))
      .out(jsonBody[NotificationResponse])

  private val notFoundOutput: EndpointOutput[NotificationError] =
    statusCode(StatusCode.NotFound)
      .and(jsonBody[ErrorResponse])
      .map[NotificationError](_ => NotificationNotFound)(_ =>
        ErrorResponse("Notification not found")
      )

  private val getNotificationEndpoint: PublicEndpoint[
    String,
    NotificationError,
    NotificationResponse,
    Any
  ] =
    endpoint.get
      .in("notifications" / path[String]("id"))
      .out(jsonBody[NotificationResponse])
      .errorOut(notFoundOutput)

  private val updateNotificationEndpoint: PublicEndpoint[
    (String, UpdateNotificationRequest),
    NotificationError,
    NotificationResponse,
    Any
  ] =
    endpoint.patch
      .in("notifications" / path[String]("id"))
      .in(jsonBody[UpdateNotificationRequest])
      .out(jsonBody[NotificationResponse])
      .errorOut(notFoundOutput)

  private val replaceNotificationEndpoint: PublicEndpoint[
    (String, UpdateNotificationRequest),
    NotificationError,
    NotificationResponse,
    Any
  ] =
    endpoint.put
      .in("notifications" / path[String]("id"))
      .in(jsonBody[UpdateNotificationRequest])
      .out(jsonBody[NotificationResponse])
      .errorOut(notFoundOutput)

  private val deleteNotificationEndpoint
      : PublicEndpoint[String, NotificationError, Unit, Any] =
    endpoint.delete
      .in("notifications" / path[String]("id"))
      .out(statusCode(StatusCode.NoContent))
      .errorOut(notFoundOutput)

  def serverEndpoint[F[_]: Async](
      store: NotificationStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    createNotificationEndpoint.serverLogicSuccess[F] { req =>
      for {
        _ <- logger.info(
          Map(
            "method" -> "POST",
            "path" -> "/notifications"
          )
        )("Received request")
        entity <- store
          .create( /* codegen:fields:CREATE_CALL_ARGS */ )
          .onError { case error =>
            logger.error(Map.empty, error)("Persisting the notification failed")
          }
        _ <- logger.info(
          Map("notification_id" -> entity.id)
        )("Request completed")
      } yield NotificationResponse(entity)
    }

  def getNotificationServerEndpoint[F[_]: Async](
      store: NotificationStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    getNotificationEndpoint.serverLogic[F] { id =>
      for {
        _ <- logger.info(
          Map(
            "method" -> "GET",
            "path" -> s"/notifications/$id",
            "notification_id" -> id
          )
        )(
          "Received request"
        )
        result <- store.get(id).flatMap {
          case Some(entity) =>
            logger
              .info(Map("notification_id" -> id))("Request completed")
              .as(Right(NotificationResponse(entity)))
          case None =>
            logger
              .warn(Map("notification_id" -> id))("Notification not found")
              .as(Left(NotificationNotFound))
        }
      } yield result
    }

  /** Shared handler for `PATCH` (partial update) and `PUT` (full replace) —
    * both call `NotificationStore.update` with the same required update body;
    * only the logged HTTP method differs.
    */
  private def updateLogic[F[_]: Async](
      store: NotificationStore[F],
      logger: StructuredLogger[F],
      httpMethod: String
  )(
      id: String,
      req: UpdateNotificationRequest
  ): F[Either[NotificationError, NotificationResponse]] =
    for {
      _ <- logger.info(
        Map(
          "method" -> httpMethod,
          "path" -> s"/notifications/$id",
          "notification_id" -> id
        )
      )("Received request")
      result <- store
        .update(id /* codegen:fields:UPDATE_CALL_ARGS */ )
        .flatMap {
          case Some(entity) =>
            logger
              .info(Map("notification_id" -> id))("Request completed")
              .as(Right(NotificationResponse(entity)))
          case None =>
            logger
              .warn(Map("notification_id" -> id))("Notification not found")
              .as(Left(NotificationNotFound))
        }
    } yield result

  def updateNotificationServerEndpoint[F[_]: Async](
      store: NotificationStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    updateNotificationEndpoint.serverLogic[F] { case (id, req) =>
      updateLogic(store, logger, "PATCH")(id, req)
    }

  def replaceNotificationServerEndpoint[F[_]: Async](
      store: NotificationStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    replaceNotificationEndpoint.serverLogic[F] { case (id, req) =>
      updateLogic(store, logger, "PUT")(id, req)
    }

  def deleteNotificationServerEndpoint[F[_]: Async](
      store: NotificationStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    deleteNotificationEndpoint.serverLogic[F] { id =>
      for {
        _ <- logger.info(
          Map(
            "method" -> "DELETE",
            "path" -> s"/notifications/$id",
            "notification_id" -> id
          )
        )("Received request")
        result <- store.delete(id).flatMap {
          case true =>
            logger
              .info(Map("notification_id" -> id))("Request completed")
              .as(Right(()))
          case false =>
            logger
              .warn(Map("notification_id" -> id))("Notification not found")
              .as(Left(NotificationNotFound))
        }
      } yield result
    }

  def routes[F[_]: Async](
      store: NotificationStore[F],
      logger: StructuredLogger[F]
  ): HttpRoutes[F] =
    Http4sServerInterpreter[F]().toRoutes(
      List(
        serverEndpoint(store, logger),
        getNotificationServerEndpoint(store, logger),
        updateNotificationServerEndpoint(store, logger),
        replaceNotificationServerEndpoint(store, logger),
        deleteNotificationServerEndpoint(store, logger)
      )
    )
}
