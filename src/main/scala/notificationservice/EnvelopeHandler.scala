package notificationservice

import cats.MonadThrow
import cats.syntax.all._
import io.circe.{Decoder, Json}
import io.circe.parser.decode
import org.typelevel.log4cats.StructuredLogger
import purekafka.consumer.DeadLetter

/** One `eventType` a consumer acts on: how to decode its payload and what to do
  * with it.
  */
final case class EnvelopeRoute[F[_]](
    eventType: String,
    run: Json => Decoder.Result[F[Unit]]
)

object EnvelopeRoute {
  def of[F[_], A: Decoder](eventType: String)(
      handle: A => F[Unit]
  ): EnvelopeRoute[F] =
    EnvelopeRoute(eventType, _.as[A].map(handle))
}

/** The per-record decision every consumer shares (defined once, not per
  * consumer):
  *   - null value: logged and skipped (a `DeadLetter[String, String]` can't
  *     carry it);
  *   - undecodable envelope, or a routed event type whose payload doesn't
  *     decode: logged without the raw payload, then routed raw to `<topic>-dlq`
  *     so it isn't silently lost;
  *   - event type with no route: logged and skipped (these topics carry other
  *     services' events by design);
  *   - routed event: its handler runs inside `DeadLetter.guard`.
  *
  * Offset commits stay with the caller's stream.
  */
object EnvelopeHandler {

  def handle[F[_]: MonadThrow](
      topic: String,
      routes: List[EnvelopeRoute[F]],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String]
  )(key: String, value: Option[String]): F[Unit] = {
    val byType = routes.map(r => r.eventType -> r).toMap

    def deadLetterRaw(raw: String, error: Throwable, what: String): F[Unit] =
      logger.error(
        Map("topic" -> topic, "bytes" -> raw.length.toString),
        error
      )(s"Failed to decode $what") *>
        deadLetter.guard(topic, key, raw)(MonadThrow[F].raiseError(error))

    value match {
      case None =>
        logger.error(Map("topic" -> topic))(
          s"Received $topic record with a null value - skipping"
        )
      case Some(raw) =>
        decode[EventEnvelope](raw) match {
          case Left(error)     => deadLetterRaw(raw, error, s"$topic envelope")
          case Right(envelope) =>
            byType.get(envelope.eventType) match {
              case None =>
                logger.info(
                  Map("topic" -> topic, "event_type" -> envelope.eventType)
                )(s"Ignoring unrelated $topic event type")
              case Some(route) =>
                route.run(envelope.payload) match {
                  case Left(error) =>
                    deadLetterRaw(raw, error, s"${route.eventType} payload")
                  case Right(action) =>
                    deadLetter.guard(topic, key, raw)(action)
                }
            }
        }
    }
  }
}
