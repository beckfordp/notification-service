// SPDX-License-Identifier: MIT

package notificationservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.Uri
import pureconfig.ConfigSource
import purerest.resilience.{CircuitBreakerConfig, ResilienceConfig, RetryConfig}

import scala.concurrent.duration._

class NotificationServiceConfigSuite extends CatsEffectSuite {

  private val validHocon =
    """
      |port = 8080
      |metrics-port = 9090
      |service-name = "notification-service"
      |async-api-html-dir = "target/asyncapi-docs"
      |postgres {
      |  host = "localhost"
      |  port = 5432
      |  database = "notification"
      |  user = "notification"
      |  password = "notification"
      |}
      |kafka {
      |  bootstrap-servers = "localhost:9092"
      |  dlq-retry {
      |    max-retries = 3
      |    base-delay = 200ms
      |  }
      |  dlq-publish-resilience {
      |    retry {
      |      max-retries = 3
      |      base-delay = 100ms
      |    }
      |    circuit-breaker {
      |      failure-threshold = 5
      |      reset-timeout = 30s
      |    }
      |  }
      |  stream-supervisor {
      |    base-delay = 1s
      |    max-delay = 30s
      |  }
      |}
      |user-client {
      |  base-url = "http://localhost:8084"
      |  resilience {
      |    retry {
      |      max-retries = 3
      |      base-delay = 100ms
      |    }
      |    circuit-breaker {
      |      failure-threshold = 5
      |      reset-timeout = 30s
      |    }
      |  }
      |}
      |order-client {
      |  base-url = "http://localhost:8083"
      |  resilience {
      |    retry {
      |      max-retries = 3
      |      base-delay = 100ms
      |    }
      |    circuit-breaker {
      |      failure-threshold = 5
      |      reset-timeout = 30s
      |    }
      |  }
      |}
      |""".stripMargin

  private val expectedResilience = ResilienceConfig(
    retry = RetryConfig(maxRetries = 3, baseDelay = 100.millis),
    circuitBreaker =
      CircuitBreakerConfig(failureThreshold = 5, resetTimeout = 30.seconds)
  )

  test("loads a fully-specified config, with a kafka block") {
    val result =
      ConfigSource.string(validHocon).load[NotificationServiceConfig]
    assertEquals(
      result,
      Right(
        NotificationServiceConfig(
          port = 8080,
          metricsPort = 9090,
          serviceName = "notification-service",
          asyncApiHtmlDir = "target/asyncapi-docs",
          postgres = PostgresConfig(
            host = "localhost",
            port = 5432,
            database = "notification",
            user = "notification",
            password = "notification"
          ),
          kafka = KafkaConfig(bootstrapServers = "localhost:9092"),
          userClient = UserClientConfig(
            Uri.unsafeFromString("http://localhost:8084"),
            expectedResilience
          ),
          orderClient = OrderClientConfig(
            Uri.unsafeFromString("http://localhost:8083"),
            expectedResilience
          )
        )
      )
    )
  }

  test("fails to load when a required field is missing") {
    val missingKafka =
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

    assert(
      ConfigSource
        .string(missingKafka)
        .load[NotificationServiceConfig]
        .isLeft
    )
  }

  test("fails to load when the postgres password is missing") {
    val missingPassword = validHocon.replace(
      "  password = \"notification\"\n",
      ""
    )

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
      assertEquals(config.asyncApiHtmlDir, "target/asyncapi-docs")
      assertEquals(
        config.postgres,
        PostgresConfig(
          host = "localhost",
          port = 5432,
          database = "notification",
          user = "notification",
          password = "notification"
        )
      )
      assertEquals(
        config.kafka,
        KafkaConfig(bootstrapServers = "localhost:9092")
      )
      assertEquals(
        config.userClient.baseUrl,
        Uri.unsafeFromString("http://localhost:8084")
      )
      assertEquals(
        config.orderClient.baseUrl,
        Uri.unsafeFromString("http://localhost:8083")
      )
    }
  }
}
