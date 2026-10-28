package notificationservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema

/** `user-events: UserRegistered` payload (US-16.3 producer, US-16.4 consumer).
  * Contract: `gluon/docs/system-design.md`'s "Payload contracts".
  */
final case class UserRegisteredEvent(userId: String, email: String)

object UserRegisteredEvent {
  implicit val codec: Codec[UserRegisteredEvent] = deriveCodec
  implicit val schema: Schema[UserRegisteredEvent] = Schema.derived
}
