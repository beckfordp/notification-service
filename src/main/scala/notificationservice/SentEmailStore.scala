package notificationservice

import cats.effect.std.Console
import cats.effect.{Async, Resource, Sync}
import cats.syntax.all._
import fs2.io.net.Network
import org.typelevel.otel4s.metrics.Meter
import purerest.metrics.PoolMetrics
import skunk.codec.all._
import skunk.implicits._
import skunk.{Codec, Session}

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID

/** Who an email is addressed to. Mirrors the Admin console mock's
  * `customer | email | order` recipient discriminator.
  */
sealed trait RecipientType { def asString: String }

object RecipientType {
  case object Customer extends RecipientType {
    val asString: String = "customer"
  }
  case object Email extends RecipientType { val asString: String = "email" }
  case object Order extends RecipientType { val asString: String = "order" }

  val all: List[RecipientType] = List(Customer, Email, Order)

  def fromString(raw: String): Either[String, RecipientType] =
    all.find(_.asString == raw).toRight(s"Unknown recipient type: $raw")
}

/** Send state of one audit row. */
sealed trait EmailStatus { def asString: String }

object EmailStatus {
  case object Pending extends EmailStatus { val asString: String = "pending" }
  case object Sent extends EmailStatus { val asString: String = "sent" }
  case object Failed extends EmailStatus { val asString: String = "failed" }

  /** Terminal: no email address could be resolved for this event (ADM-3). Never
    * retried, never sent.
    */
  case object Unresolved extends EmailStatus {
    val asString: String = "unresolved"
  }

  val all: List[EmailStatus] = List(Pending, Sent, Failed, Unresolved)

  def fromString(raw: String): Either[String, EmailStatus] =
    all.find(_.asString == raw).toRight(s"Unknown email status: $raw")
}

/** A new audit row to insert as `pending`, before the send is attempted. */
final case class NewSentEmail(
    id: UUID,
    sourceEventId: String,
    eventType: String,
    reason: Option[String],
    recipientType: RecipientType,
    recipientKey: String,
    recipientEmail: Option[String],
    customerId: Option[String],
    orderId: Option[String],
    subject: String,
    body: String
)

final case class SentEmailRow(
    id: UUID,
    sourceEventId: String,
    eventType: String,
    reason: Option[String],
    recipientType: RecipientType,
    recipientKey: String,
    recipientEmail: Option[String],
    customerId: Option[String],
    orderId: Option[String],
    subject: String,
    body: String,
    status: EmailStatus,
    createdAt: Instant,
    sentAt: Option[Instant],
    failedAt: Option[Instant],
    error: Option[String]
)

/** Result of claiming an event for sending. `Send` carries the row id to mark
  * afterwards; `Duplicate` means the event was already sent and must not be
  * emailed again.
  */
sealed trait EmailClaim

object EmailClaim {
  final case class Send(id: UUID) extends EmailClaim
  case object Duplicate extends EmailClaim
}

trait SentEmailStore[F[_]] {

  /** Atomically claims `sourceEventId` for sending. A new row is inserted
    * `pending`; an existing `pending`/`failed` row is reset to `pending` and
    * reused (so a redelivery can finish an interrupted send); an existing
    * `sent` row yields `Duplicate` and no write.
    */
  def claim(email: NewSentEmail): F[EmailClaim]

  def markSent(id: UUID): F[Unit]

  def markFailed(id: UUID, error: String): F[Unit]

  /** Records the resolved identity before the send is attempted (ADM-3), so
    * attribution survives a subsequent send failure.
    */
  def markResolved(
      id: UUID,
      customerId: Option[String],
      recipientEmail: Option[String]
  ): F[Unit]

  /** Terminal resolution failure: no address exists for this event. */
  def markUnresolved(id: UUID, reason: String): F[Unit]

  def get(sourceEventId: String): F[Option[SentEmailRow]]

  /** Newest first; both filters optional and ANDed. */
  def list(
      customerId: Option[String],
      orderId: Option[String]
  ): F[List[SentEmailRow]]

  /** `true` when Postgres answers; gates `/health/ready` (ADM-6). */
  def ping: F[Boolean]
}

object SentEmailStore {

  def postgres[F[_]: Async: Console: Network](
      config: PostgresConfig,
      meter: Meter[F]
  ): Resource[F, SentEmailStore[F]] = {
    import org.typelevel.otel4s.metrics.Meter.Implicits.noop
    import org.typelevel.otel4s.trace.Tracer.Implicits.noop
    Session
      .Builder[F]
      .withHost(config.host)
      .withPort(config.port)
      .withUserAndPassword(config.user, config.password)
      .withDatabase(config.database)
      .pooled(max = 10)
      .evalMap(PoolMetrics.instrument(meter, "notification")(_))
      .map(pool => new PostgresSentEmailStore[F](pool))
  }

  // Interpolated by name: `$text.opt` would bind `.opt` as SQL text, not as
  // a codec modifier, so optional parameters use this codec directly.
  private val optionalText: Codec[Option[String]] = text.opt

  private val recipientTypeCodec: Codec[RecipientType] =
    text.eimap(RecipientType.fromString)(_.asString)

  private val emailStatusCodec: Codec[EmailStatus] =
    text.eimap(EmailStatus.fromString)(_.asString)

  private val instantCodec: Codec[Instant] =
    timestamptz.eimap(odt => Right(odt.toInstant))(instant =>
      OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)
    )

  private val sentEmailRow: Codec[
    (
        UUID,
        String,
        String,
        Option[String],
        RecipientType,
        String,
        Option[String],
        Option[String],
        Option[String],
        String,
        String,
        EmailStatus,
        Instant,
        Option[Instant],
        Option[Instant],
        Option[String]
    )
  ] =
    uuid *: text *: text *: text.opt *: recipientTypeCodec *: text *:
      text.opt *: text.opt *: text.opt *: text *: text *: emailStatusCodec *:
      instantCodec *: instantCodec.opt *: instantCodec.opt *: text.opt

  private def toSentEmailRow(
      row: (
          UUID,
          String,
          String,
          Option[String],
          RecipientType,
          String,
          Option[String],
          Option[String],
          Option[String],
          String,
          String,
          EmailStatus,
          Instant,
          Option[Instant],
          Option[Instant],
          Option[String]
      )
  ): SentEmailRow = {
    val (
      id,
      sourceEventId,
      eventType,
      reason,
      recipientType,
      recipientKey,
      recipientEmail,
      customerId,
      orderId,
      subject,
      body,
      status,
      createdAt,
      sentAt,
      failedAt,
      error
    ) = row
    SentEmailRow(
      id,
      sourceEventId,
      eventType,
      reason,
      recipientType,
      recipientKey,
      recipientEmail,
      customerId,
      orderId,
      subject,
      body,
      status,
      createdAt,
      sentAt,
      failedAt,
      error
    )
  }

  private val claimEmail: skunk.Query[
    (
        UUID,
        String,
        String,
        Option[String],
        RecipientType,
        String,
        Option[String],
        Option[String],
        Option[String],
        String,
        String
    ),
    UUID
  ] =
    sql"""
      insert into sent_email
        (id, source_event_id, event_type, reason, recipient_type,
         recipient_key, recipient_email, customer_id, order_id, subject,
         body, status)
      values
        ($uuid, $text, $text, $optionalText, $recipientTypeCodec,
         $text, $optionalText, $optionalText, $optionalText, $text, $text,
         'pending')
      on conflict (source_event_id) do update
        set status = 'pending',
            sent_at = null,
            failed_at = null,
            error = null
        where sent_email.status not in ('sent', 'unresolved')
      returning id
    """.query(uuid)

  private val markSentQuery: skunk.Command[UUID] =
    sql"""
      update sent_email
      set status = 'sent', sent_at = now(), failed_at = null, error = null
      where id = $uuid
    """.command

  private val markFailedQuery: skunk.Command[(String, UUID)] =
    sql"""
      update sent_email
      set status = 'failed', failed_at = now(), error = $text
      where id = $uuid
    """.command

  private val markResolvedQuery: skunk.Command[
    (Option[String], Option[String], UUID)
  ] =
    sql"""
      update sent_email
      set customer_id = coalesce($optionalText, customer_id),
          recipient_email = coalesce($optionalText, recipient_email)
      where id = $uuid
    """.command

  private val markUnresolvedQuery: skunk.Command[(String, UUID)] =
    sql"""
      update sent_email
      set status = 'unresolved', error = $text, failed_at = null
      where id = $uuid
    """.command

  private val selectBySourceEventId: skunk.Query[String, SentEmailRow] =
    sql"""
      select id, source_event_id, event_type, reason, recipient_type,
             recipient_key, recipient_email, customer_id, order_id, subject,
             body, status, created_at, sent_at, failed_at, error
      from sent_email
      where source_event_id = $text
    """.query(sentEmailRow).map(toSentEmailRow)

  private val pingQuery: skunk.Query[skunk.Void, Int] =
    sql"SELECT 1".query(int4)

  private val listEmails: skunk.Query[
    (Option[String], Option[String], Option[String], Option[String]),
    SentEmailRow
  ] =
    sql"""
      select id, source_event_id, event_type, reason, recipient_type,
             recipient_key, recipient_email, customer_id, order_id, subject,
             body, status, created_at, sent_at, failed_at, error
      from sent_email
      where ($optionalText is null or customer_id = $optionalText)
        and ($optionalText is null or order_id = $optionalText)
      order by created_at desc, id
    """.query(sentEmailRow).map(toSentEmailRow)

  private final class PostgresSentEmailStore[F[_]: Sync](
      pool: Resource[F, Session[F]]
  ) extends SentEmailStore[F] {

    def claim(email: NewSentEmail): F[EmailClaim] =
      pool
        .use { session =>
          session
            .prepare(claimEmail)
            .flatMap(
              _.option(
                (
                  email.id,
                  email.sourceEventId,
                  email.eventType,
                  email.reason,
                  email.recipientType,
                  email.recipientKey,
                  email.recipientEmail,
                  email.customerId,
                  email.orderId,
                  email.subject,
                  email.body
                )
              )
            )
        }
        .map {
          case Some(id) => EmailClaim.Send(id)
          case None     => EmailClaim.Duplicate
        }

    def markSent(id: UUID): F[Unit] =
      pool.use(_.prepare(markSentQuery).flatMap(_.execute(id))).void

    def markFailed(id: UUID, error: String): F[Unit] =
      pool
        .use(_.prepare(markFailedQuery).flatMap(_.execute((error, id))))
        .void

    def markResolved(
        id: UUID,
        customerId: Option[String],
        recipientEmail: Option[String]
    ): F[Unit] =
      pool
        .use(
          _.prepare(markResolvedQuery)
            .flatMap(_.execute((customerId, recipientEmail, id)))
        )
        .void

    def markUnresolved(id: UUID, reason: String): F[Unit] =
      pool
        .use(
          _.prepare(markUnresolvedQuery).flatMap(_.execute((reason, id)))
        )
        .void

    def ping: F[Boolean] =
      pool.use(_.unique(pingQuery)).attempt.map(_.isRight)

    def get(sourceEventId: String): F[Option[SentEmailRow]] =
      pool.use(
        _.prepare(selectBySourceEventId).flatMap(_.option(sourceEventId))
      )

    def list(
        customerId: Option[String],
        orderId: Option[String]
    ): F[List[SentEmailRow]] =
      pool.use(
        _.prepare(listEmails).flatMap(
          _.stream(
            (customerId, customerId, orderId, orderId),
            64
          ).compile.toList
        )
      )
  }
}
