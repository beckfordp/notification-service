// SPDX-License-Identifier: MIT

package notificationservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

/** `user-events: UserRegistered` payload (US-16.3 producer, US-16.4 consumer).
  * Contract: `gluon/docs/system-design.md`'s "Payload contracts".
  *
  * Descriptions/examples are copied from the producer's artifact in gluon's
  * `registry/` (TD-19); nothing checks they stay current.
  */
@description("New user signed up: POST /auth/signup succeeded.")
final case class UserRegisteredEvent(
    @description("User UUID. Also the partition key.")
    @encodedExample("5b6c7d8e-9f0a-4b1c-8d2e-3f4a5b6c7d8e")
    userId: String,
    @description("User's email address.")
    @encodedExample("jane@example.com")
    email: String
)

object UserRegisteredEvent {
  implicit val codec: Codec[UserRegisteredEvent] = deriveCodec
  implicit val schema: Schema[UserRegisteredEvent] = Schema.derived
}
