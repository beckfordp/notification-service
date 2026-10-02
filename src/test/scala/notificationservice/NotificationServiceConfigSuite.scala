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
      |""".stripMargin

  test("loads a fully-specified config, with no postgres block") {
    val result =
      ConfigSource.string(validHocon).load[NotificationServiceConfig]
    assertEquals(
      result,
      Right(
        NotificationServiceConfig(
          port = 8080,
          metricsPort = 9090,
          serviceName = "notification-service"
        )
      )
    )
  }

  test("fails to load when a required field is missing") {
    val missingServiceName =
      """
        |port = 8080
        |metrics-port = 9090
        |""".stripMargin

    assert(
      ConfigSource
        .string(missingServiceName)
        .load[NotificationServiceConfig]
        .isLeft
    )
  }

  test("load[F] reads the shipped application.conf defaults") {
    NotificationServiceConfig.load[IO].map { config =>
      assertEquals(config.port, 8080)
      assertEquals(config.metricsPort, 9090)
      assertEquals(config.serviceName, "notification-service")
    }
  }
}
