package notificationservice

import cats.effect.{Async, Concurrent, Outcome, Resource}
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import fs2.kafka.ProducerSettings
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.metrics.Meter
import purekafka.consumer.{DeadLetter, StreamSupervisor}

/** Wires every consumer with `purekafka`'s `StreamSupervisor`
  * (restart-with-backoff) and `DeadLetter` (bounded-retry-then-DLQ) -
  * TD-2.3/TD-2.4, ADR 0010. Each consumer is wrapped individually, then the
  * streams are merged, so a crash/backoff in one doesn't restart the others.
  * Extracted out of `Main.scala`'s own `val run` so this wiring is directly
  * testable, not just exercised incidentally by running the whole service.
  * `config.streamSupervisor`/`config.dlqRetry`/ `config.dlqPublishResilience`
  * are all `application.conf`-driven (see
  * `NotificationServiceConfig.KafkaConfig`) rather than hardcoded here.
  */
object KafkaConsumers {

  def supervised[F[_]: Async](
      config: KafkaConfig,
      emailClient: EmailClient[F],
      logger: StructuredLogger[F],
      meter: Meter[F]
  ): Stream[F, Unit] = {
    def dlqProducerSettings: ProducerSettings[F, String, String] =
      ProducerSettings[F, String, String]
        .withBootstrapServers(config.bootstrapServers)

    for {
      orderDeadLetter <- Stream.resource(
        DeadLetter.resource[F, String, String](
          config.dlqRetry,
          config.dlqPublishResilience,
          dlqProducerSettings
        )(logger)(meter)
      )
      userDeadLetter <- Stream.resource(
        DeadLetter.resource[F, String, String](
          config.dlqRetry,
          config.dlqPublishResilience,
          dlqProducerSettings
        )(logger)(meter)
      )
      fulfilmentDeadLetter <- Stream.resource(
        DeadLetter.resource[F, String, String](
          config.dlqRetry,
          config.dlqPublishResilience,
          dlqProducerSettings
        )(logger)(meter)
      )
      paymentDeadLetter <- Stream.resource(
        DeadLetter.resource[F, String, String](
          config.dlqRetry,
          config.dlqPublishResilience,
          dlqProducerSettings
        )(logger)(meter)
      )
      merged <- StreamSupervisor
        .supervise[F, Unit](config.streamSupervisor)(logger)(meter)(
          Async[F].pure(
            OrderEventsConsumer
              .run[F](config, emailClient, logger, orderDeadLetter)
          )
        )
        .merge(
          StreamSupervisor.supervise[F, Unit](config.streamSupervisor)(
            logger
          )(meter)(
            Async[F].pure(
              UserRegisteredConsumer
                .run[F](config, emailClient, logger, userDeadLetter)
            )
          )
        )
        .merge(
          StreamSupervisor.supervise[F, Unit](config.streamSupervisor)(
            logger
          )(meter)(
            Async[F].pure(
              FulfilmentEventConsumer
                .run[F](config, emailClient, logger, fulfilmentDeadLetter)
            )
          )
        )
        .merge(
          StreamSupervisor.supervise[F, Unit](config.streamSupervisor)(
            logger
          )(meter)(
            Async[F].pure(
              PaymentEventConsumer
                .run[F](config, emailClient, logger, paymentDeadLetter)
            )
          )
        )
    } yield merged
  }

  /** Raised by [[watched]] when the merged consumer stream completes: it is
    * meant to run for the life of the service.
    */
  case object ConsumersStopped
      extends RuntimeException("Kafka consumer streams stopped unexpectedly")

  /** Runs `stream` on a background fiber and yields a watcher that only
    * completes by failing: if the stream errors (e.g. a DLQ producer can't be
    * acquired - outside `StreamSupervisor`'s restart loop) or ends, the cause
    * is logged and raised. `Main` waits on it, so the service exits and the pod
    * restarts instead of serving a healthy `/health` with no consumers. The
    * fiber is cancelled when the resource is released.
    */
  def watched[F[_]: Concurrent](
      stream: Stream[F, Unit],
      logger: StructuredLogger[F]
  ): Resource[F, F[Unit]] =
    stream.compile.drain.background.map(_.flatMap {
      case Outcome.Succeeded(_) =>
        logger.error(Map.empty)("Kafka consumers stopped") *>
          Concurrent[F].raiseError[Unit](ConsumersStopped)
      case Outcome.Errored(error) =>
        logger.error(Map.empty, error)("Kafka consumers failed") *>
          Concurrent[F].raiseError[Unit](error)
      case Outcome.Canceled() => Concurrent[F].never[Unit]
    })

  /** [[supervised]] under [[watched]], so `Main` wires this in as one
    * `Resource` line, the same shape as every other dependency, with no fs2
    * streaming mechanics visible there.
    */
  def resource[F[_]: Async](
      config: KafkaConfig,
      emailClient: EmailClient[F],
      logger: StructuredLogger[F],
      meter: Meter[F]
  ): Resource[F, F[Unit]] =
    watched[F](supervised[F](config, emailClient, logger, meter), logger)
}
