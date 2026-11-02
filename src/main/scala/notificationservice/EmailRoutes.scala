package notificationservice

import cats.Applicative
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.typelevel.log4cats.StructuredLogger
import sttp.tapir._
import sttp.tapir.Schema.annotations.description
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.ServerEndpoint

import java.time.Instant

/** Read model for the Admin console's Customers tab (ADM-2). */
@description("One recorded email send attempt.")
final case class SentEmailResponse(
    @description("Audit row id (UUID).")
    id: String,
    @description(
      "Envelope eventId that triggered the email; the idempotency key."
    )
    sourceEventId: String,
    @description("Event type, e.g. OrderConfirmed or PaymentCaptured.")
    eventType: String,
    @description("Cancellation or failure reason carried by the event, if any.")
    reason: Option[String],
    @description("Recipient discriminator: customer, email or order.")
    recipientType: String,
    @description(
      "Event identity: customerId, email address or orderId. Provenance, not necessarily the destination."
    )
    recipientKey: String,
    @description(
      "Resolved destination email address, when known (ADM-3). Null for unresolved or legacy rows."
    )
    recipientEmail: Option[String],
    @description("Customer the email is attributed to, when known.")
    customerId: Option[String],
    @description("Order the email concerns, when the event carries one.")
    orderId: Option[String],
    @description("Email subject.")
    subject: String,
    @description("Email body.")
    body: String,
    @description(
      "Send state: pending, sent, failed (retryable) or unresolved (no address found - terminal)."
    )
    status: String,
    @description("When the send was attempted.")
    createdAt: Instant,
    @description("When the send succeeded, if it did.")
    sentAt: Option[Instant],
    @description("When the send failed, if it did.")
    failedAt: Option[Instant],
    @description("Failure detail, if the send failed.")
    error: Option[String]
)

object SentEmailResponse {
  implicit val codec: Codec[SentEmailResponse] = deriveCodec

  def apply(row: SentEmailRow): SentEmailResponse =
    SentEmailResponse(
      id = row.id.toString,
      sourceEventId = row.sourceEventId,
      eventType = row.eventType,
      reason = row.reason,
      recipientType = row.recipientType.asString,
      recipientKey = row.recipientKey,
      recipientEmail = row.recipientEmail,
      customerId = row.customerId,
      orderId = row.orderId,
      subject = row.subject,
      body = row.body,
      status = row.status.asString,
      createdAt = row.createdAt,
      sentAt = row.sentAt,
      failedAt = row.failedAt,
      error = row.error
    )
}

/** `GET /emails?customerId=&orderId=` — the sent-email audit history (ADM-2).
  * Both filters are optional and ANDed; results are newest first.
  */
object EmailRoutes {

  private val tag = "emails"

  private val exampleSentEmail: SentEmailResponse =
    SentEmailResponse(
      id = "5c8d2e1f-3a4b-4c6d-9e7f-0a1b2c3d4e5f",
      sourceEventId = "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d",
      eventType = "OrderConfirmed",
      reason = None,
      recipientType = "customer",
      recipientKey = "7c9e6679-7425-40de-944b-e07fc1f90ae7",
      recipientEmail = Some("ada@example.com"),
      customerId = Some("7c9e6679-7425-40de-944b-e07fc1f90ae7"),
      orderId = Some("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f"),
      subject = "Your order is confirmed",
      body =
        "Good news - order 3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f is confirmed.",
      status = "sent",
      createdAt = Instant.parse("2026-11-02T10:15:30Z"),
      sentAt = Some(Instant.parse("2026-11-02T10:15:31Z")),
      failedAt = None,
      error = None
    )

  private val listEmailsEndpoint: PublicEndpoint[
    (Option[String], Option[String]),
    Unit,
    List[SentEmailResponse],
    Any
  ] =
    endpoint.get
      .in("emails")
      .in(
        query[Option[String]]("customerId")
          .description("Only emails attributed to this customer id.")
      )
      .in(
        query[Option[String]]("orderId")
          .description("Only emails for this order id.")
      )
      .out(
        jsonBody[List[SentEmailResponse]].example(List(exampleSentEmail))
      )
      .summary("List sent emails")
      .description(
        "The email audit history, newest first. Both filters are optional and combined with AND."
      )
      .tag(tag)

  def serverEndpoint[F[_]: Applicative](
      store: SentEmailStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    listEmailsEndpoint.serverLogicSuccess[F] { case (customerId, orderId) =>
      logger.info(
        Map(
          "method" -> "GET",
          "path" -> "/emails",
          "customer_id" -> customerId.getOrElse(""),
          "order_id" -> orderId.getOrElse("")
        )
      )("Received request") *>
        store.list(customerId, orderId).map(_.map(SentEmailResponse.apply))
    }
}
