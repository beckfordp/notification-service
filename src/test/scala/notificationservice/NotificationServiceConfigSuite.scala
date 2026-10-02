package notificationservice

import cats.effect.IO
import munit.CatsEffectSuite
import pureconfig.ConfigSource

class NotificationServiceConfigSuite extends CatsEffectSuite {

  private val validHocon =
    """
      |port = 8080
      |metrics-port = 9090
      |service-name = "notification-service"
      |postgres {
      |  host = "localhost"
      |  port = 5432
      |  database = "notification"
      |  user = "notification"
      |  password = "notification"
      |}
      |""".stripMargin

  test("loads a fully-specified config") {
    val result =
      ConfigSource.string(validHocon).load[NotificationServiceConfig]
    assertEquals(
      result,
      Right(
        NotificationServiceConfig(
          port = 8080,
          metricsPort = 9090,
          serviceName = "notification-service",
          postgres = PostgresConfig(
            host = "localhost",
            port = 5432,
            database = "notification",
            user = "notification",
            password = "notification"
          )
        )
      )
    )
  }

  test("fails to load when a required field is missing") {
    val missingPassword =
      """
        |port = 8080
        |metrics-port = 9090
        |postgres {
        |  host = "localhost"
        |  port = 5432
        |  database = "notification"
        |  user = "notification"
        |}
        |""".stripMargin

    assert(
      ConfigSource
        .string(missingPassword)
        .load[NotificationServiceConfig]
        .isLeft
    )
  }

  test("load[F] reads the shipped application.conf defaults") {
    NotificationServiceConfig.load[IO].map { config =>
      assertEquals(config.port, 8080)
      assertEquals(config.metricsPort, 9090)
      assertEquals(config.serviceName, "notification-service")
      assertEquals(
        config.postgres,
        PostgresConfig(
          "localhost",
          5432,
          "notification",
          "notification",
          "notification"
        )
      )
    }
  }
}
