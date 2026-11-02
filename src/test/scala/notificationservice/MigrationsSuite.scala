package notificationservice

import cats.effect.IO
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.CatsEffectSuite
import org.testcontainers.utility.DockerImageName

import java.sql.{Connection, DriverManager, SQLException}
import java.util.UUID

class MigrationsSuite extends CatsEffectSuite with TestContainerForAll {

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(dockerImageName =
      DockerImageName.parse("postgres:16-alpine")
    )

  private def configFor(postgres: PostgreSQLContainer): PostgresConfig =
    PostgresConfig(
      host = postgres.host,
      port = postgres.mappedPort(5432),
      database = postgres.databaseName,
      user = postgres.username,
      password = postgres.password
    )

  private def withConnection[A](
      postgres: PostgreSQLContainer
  )(body: Connection => A): A = {
    val conn = DriverManager.getConnection(
      postgres.jdbcUrl,
      postgres.username,
      postgres.password
    )
    try body(conn)
    finally conn.close()
  }

  private def strings(
      conn: Connection,
      query: String
  ): List[String] = {
    val rs = conn.createStatement().executeQuery(query)
    try Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toList
    finally rs.close()
  }

  private def insertRow(
      conn: Connection,
      eventId: String,
      status: String = "pending",
      recipientType: String = "customer"
  ): Unit = {
    val stmt = conn.prepareStatement(
      """insert into sent_email
        |  (id, source_event_id, event_type, recipient_type, recipient_key,
        |   subject, body, status)
        |values (?::uuid, ?, 'OrderConfirmed', ?, 'customer-1',
        |        'subject', 'body', ?)""".stripMargin
    )
    try {
      stmt.setString(1, UUID.randomUUID().toString)
      stmt.setString(2, eventId)
      stmt.setString(3, recipientType)
      stmt.setString(4, status)
      stmt.executeUpdate()
      ()
    } finally stmt.close()
  }

  test("migrations create the sent_email table with its columns") {
    withContainers { postgres =>
      Migrations.run[IO](configFor(postgres)).map { _ =>
        val columns = withConnection(postgres) { conn =>
          strings(
            conn,
            """select column_name from information_schema.columns
              |where table_name = 'sent_email'
              |order by ordinal_position""".stripMargin
          )
        }
        assertEquals(
          columns,
          List(
            "id",
            "source_event_id",
            "event_type",
            "reason",
            "recipient_type",
            "recipient_key",
            "customer_id",
            "order_id",
            "subject",
            "body",
            "status",
            "created_at",
            "sent_at",
            "failed_at",
            "error"
          )
        )
      }
    }
  }

  test("migrations add the filter indexes") {
    withContainers { postgres =>
      Migrations.run[IO](configFor(postgres)).map { _ =>
        val indexes = withConnection(postgres) { conn =>
          strings(
            conn,
            "select indexname from pg_indexes where tablename = 'sent_email'"
          )
        }
        assert(
          clue(indexes).contains("sent_email_customer_id_created_at_idx")
        )
        assert(clue(indexes).contains("sent_email_order_id_created_at_idx"))
      }
    }
  }

  test("source_event_id is unique") {
    withContainers { postgres =>
      Migrations.run[IO](configFor(postgres)).map { _ =>
        withConnection(postgres) { conn =>
          insertRow(conn, "event-1")
          val duplicate = intercept[SQLException](insertRow(conn, "event-1"))
          assertEquals(duplicate.getSQLState, "23505")
        }
      }
    }
  }

  test("recipient_type and status are checked") {
    withContainers { postgres =>
      Migrations.run[IO](configFor(postgres)).map { _ =>
        withConnection(postgres) { conn =>
          val badRecipient = intercept[SQLException](
            insertRow(conn, "event-2", recipientType = "bogus")
          )
          assertEquals(badRecipient.getSQLState, "23514")
          val badStatus = intercept[SQLException](
            insertRow(conn, "event-3", status = "bogus")
          )
          assertEquals(badStatus.getSQLState, "23514")
        }
      }
    }
  }
}
