package notificationservice

import cats.effect.{IO, Ref}
import purekafka.consumer.DeadLetter

/** Broker-free `DeadLetter`: runs `process` once, and on failure records the
  * raw `(topic, key, value)` it would have routed to `<topic>-dlq`.
  */
final case class TestDeadLetter(
    deadLetter: DeadLetter[IO, String, String],
    routed: Ref[IO, List[(String, String, String)]]
)

object TestDeadLetter {
  def create: IO[TestDeadLetter] =
    Ref.of[IO, List[(String, String, String)]](Nil).map { ref =>
      TestDeadLetter(
        new DeadLetter[IO, String, String] {
          def guard(topic: String, key: String, value: String)(
              process: IO[Unit]
          ): IO[Unit] =
            process.handleErrorWith(_ => ref.update(_ :+ (topic, key, value)))
        },
        ref
      )
    }
}

/** In-memory `EmailClient` recording every send. */
final case class SentEmail(to: String, subject: String, body: String)

object RecordingEmailClient {
  def create: IO[(EmailClient[IO], Ref[IO, List[SentEmail]])] =
    Ref.of[IO, List[SentEmail]](Nil).map { ref =>
      (
        new EmailClient[IO] {
          def send(message: EmailMessage): IO[EmailSendOutcome] =
            ref
              .update(
                _ :+ SentEmail(
                  message.recipientEmail.getOrElse(message.recipientKey),
                  message.subject,
                  message.body
                )
              )
              .map(_ => EmailSendOutcome.Sent)
        },
        ref
      )
    }
}
