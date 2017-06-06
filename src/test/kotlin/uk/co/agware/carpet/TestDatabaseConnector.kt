package uk.co.agware.carpet

import com.nhaarman.mockito_kotlin.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.*
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import uk.co.agware.carpet.database.DefaultDatabaseConnector
import uk.co.agware.carpet.database.toMD5
import uk.co.agware.carpet.exception.MagicCarpetDatabaseException
import uk.co.agware.carpet.exception.MagicCarpetParseException
import uk.co.agware.carpet.extensions.assertEqualsIgnoreIndent
import uk.co.agware.carpet.stubs.ResultsSetStub
import java.sql.*
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@RunWith(JUnitPlatform::class)
class TestDatabaseConnector: Spek(spek)
private val spek: Dsl.() -> Unit = {

  describe("A Database Connection") {

      var statement = mock<Statement>()
      var preparedStatement = mock<PreparedStatement>()
      var connection = mock<Connection>()
      var metaData = mock<DatabaseMetaData>()
      var subject = DefaultDatabaseConnector(connection)

      beforeEachTest {
          statement = mock<Statement>()
          preparedStatement = mock<PreparedStatement>()
          connection = mock<Connection>()
          metaData = mock<DatabaseMetaData>()
          whenever(connection.createStatement()).thenReturn(statement)
          whenever(connection.prepareStatement(any())).thenReturn(preparedStatement)
          whenever(connection.metaData).thenReturn(metaData)
          whenever(preparedStatement.execute()).thenReturn(true)
          whenever(statement.execute(any())).thenReturn(true)
          whenever(preparedStatement.executeQuery()).thenReturn(ResultsSetStub(true))

          subject = DefaultDatabaseConnector(connection)
      }

      given("a DatabaseConnector") {

          it("Should execute the statement on the database") {
              val select = "SELECT * FROM Table"
              subject.executeStatement(select)
              verify(statement).execute(select)
          }

          it("Should commit the change to the database") {
              subject.commit()
              verify(connection).commit()
          }

          it("Should rollback the database changes") {
              subject.rollBack()
              verify(connection).rollback()
          }

          it("Should close the connection to the database") {
              subject.close()
              verify(connection).close()
          }

          on("checking a version exists") {
              val expectedStatement = """SELECT * FROM change_set
                                         WHERE version = ?"""
              val statementCaptor = argumentCaptor<String>()
              val version = "1.0.0"

              subject.versionExists(version)

              it("should prepare the statement") {
                  verify(connection).prepareStatement(statementCaptor.capture())
                  assertEqualsIgnoreIndent(expectedStatement, statementCaptor.value)
              }

              it("should correctly set the first parameter to the version") {
                  verify(preparedStatement).setString(1, version)
              }

              it("should execute the query") {
                  verify(preparedStatement).executeQuery()
              }
          }

          on("checking a task exists") {
              val expectedStatement = """SELECT * FROM change_set
                                         WHERE version = ?
                                             AND task = ?
                                      """
              val statementCaptor = argumentCaptor<String>()
              val version = "1.0.0"
              val taskName = "Task Name"

              subject.taskExists(version, taskName)

              it("should create a PreparedStatement") {
                  verify(connection).prepareStatement(statementCaptor.capture())
                  assertEqualsIgnoreIndent(expectedStatement, statementCaptor.value)
              }

              it("should correctly set the first parameter as the version") {
                  verify(preparedStatement).setString(1, version)
              }

              it("should correctly set the second parameter as the task name") {
                  verify(preparedStatement).setString(2, taskName)
              }

              it("should execute the query") {
                  verify(preparedStatement).executeQuery()
              }
          }
      }

      given("A task to record") {
          val expectedStatement = """INSERT INTO change_set
                                     (version, task, applied, hash)
                                     VALUES (?, ?, ?, ?)"""
          val version = "1.0.0"
          val task = "Select from DB"
          val query = "SELECT * FROM Table"

          on("recording the task") {

              val statementCaptor = argumentCaptor<String>()

              subject.recordTask(version, task, query)

              it("should create a PreparedStatement") {
                  verify(connection).prepareStatement(statementCaptor.capture())
              }

              it("should set the correct parameters on the statement") {
                  assertEqualsIgnoreIndent(expectedStatement, statementCaptor.value)
                  verify(preparedStatement).setString(1, version)
                  verify(preparedStatement).setString(2, task)
                  verify(preparedStatement).setDate(eq(3), any())
                  verify(preparedStatement).setString(4, query.toMD5())
              }

              it("Should execute the statement") {
                  verify(preparedStatement).execute()
              }
          }
      }

      given("A database with no change_set table or hash column") {

          val statementCaptor = argumentCaptor<String>()

          beforeEachTest {
              whenever(metaData.getTables(null, null, "change_set", null))
                      .thenReturn(ResultsSetStub(false))
              whenever(metaData.getColumns(null, null, "change_set", "hash"))
                      .thenReturn(ResultsSetStub(false))
          }

          on("checking the existence of the change set table") {

              subject.checkChangeSetTable(true)

              it("should check the metadata for the tables existence") {
                  verify(metaData).getTables(null, null, "change_set", null)
              }

              it("should execute create and update statements") {
                  verify(statement, times(2)).execute(statementCaptor.capture())
              }

              it("should create the change set table") {
                  val expectedStatement = """CREATE TABLE change_set (
                                                version VARCHAR(255),
                                                task VARCHAR(255),
                                                applied DATE
                                             )"""
                  assertEqualsIgnoreIndent(expectedStatement, statementCaptor.allValues[0])
              }

              it("should check the metadata for the hash columns existence") {
                  verify(metaData).getColumns(null, null, "change_set", "hash")
              }

              it("should create the hash column") {
                  val expectedStatement = "ALTER TABLE change_set ADD COLUMN hash VARCHAR(64)"
                  assertEquals(expectedStatement, statementCaptor.allValues[1])
              }

              it("should commit the changes to the database") {
                  verify(connection, times(2)).commit()
              }
          }
      }

      given("a database with a change_set table but no hash column") {

          beforeEachTest {
              whenever(metaData.getTables(null, null, "change_set", null))
                      .thenReturn(ResultsSetStub(true))
              whenever(metaData.getColumns(null, null, "change_set", "hash"))
                      .thenReturn(ResultsSetStub(false))
          }

          on("checking the existence of the change set table") {

              subject.checkChangeSetTable(true)

              it("Should check for the change set table") {
                  verify(metaData).getTables(null, null, "change_set", null)
              }

              it("Should check for the hash columns existence in the table") {
                  verify(metaData).getColumns(null, null, "change_set", "hash")
              }

              it("Should update the table schema for the hash column") {
                  val expectedStatement = "ALTER TABLE change_set ADD COLUMN hash VARCHAR(64)"
                  verify(statement).execute(check {
                      assertEquals(expectedStatement, it)
                  })
              }

              it("should commit the changes to the database") {
                  verify(connection).commit()
              }
          }
      }

      given("A Database with a change_set table with a hash column") {

          beforeEachTest {
              whenever(metaData.getTables(null, null, "change_set", null)).thenReturn(ResultsSetStub(true))
              whenever(metaData.getColumns(null, null, "change_set", "hash")).thenReturn(ResultsSetStub(true))
          }

          on("checking the existence of the change set table") {

              subject.checkChangeSetTable(true)

              it("Should check for the change set table") {
                  verify(metaData).getTables(null, null, "change_set", null)
              }

              it("Should check for the hash columns existence in the table") {
                  verify(metaData).getColumns(null, null, "change_set", "hash")
              }

              it("Should not create the table or update the table schema for the hash column") {
                  verify(statement, never()).execute(any())
              }

              it("should not commit the changes to the database") {
                  verify(connection, never()).commit()
              }
          }
      }

      given("A change set table with a matching change set") {

          val query = "SELECT * FROM Table"

          beforeEachTest {
              whenever(preparedStatement.executeQuery())
                      .thenReturn(ResultsSetStub(hasNext = true, hash = query.toMD5()))
          }

          on("checking a change sets hash column matches") {
              val expectedStatement = """SELECT * FROM change_set
                                         WHERE version = ?
                                             AND task = ?
                                      """
              val statementCaptor = argumentCaptor<String>()
              val version = "1.0.0"
              val taskName = "Task Name"
              val result = subject.taskHashMatches(version, taskName, query)

              it("Should prepare the statement") {
                  verify(connection).prepareStatement(statementCaptor.capture())
                  assertEqualsIgnoreIndent(expectedStatement, statementCaptor.value)
              }

              it("Should record populate the statement with the value of the version") {
                  verify(preparedStatement).setString(1, version)
              }

              it("Should record populate the statement with the value of the task name") {
                  verify(preparedStatement).setString(2, taskName)
              }

              it("Should execute the statement") {
                  verify(preparedStatement).executeQuery()
              }

              it("Should return true") {
                  assertTrue(result)
              }
          }
      }

      given("A change set table with a matching change set that has null hash") {

          beforeEachTest {
              whenever(preparedStatement.executeQuery()).thenReturn(ResultsSetStub())
          }

          on("checking a change sets hash column doesn't match") {
              val expectedStatement = """SELECT * FROM change_set
                                         WHERE version = ?
                                             AND task = ?
                                      """
              val statementCaptor = argumentCaptor<String>()
              val version = "1.0.0"
              val taskName = "Task Name"
              val query = "SELECT * FROM Table"
              val result = subject.taskHashMatches(version, taskName, query)

              it("Should prepare the statement") {
                  verify(connection).prepareStatement(statementCaptor.capture())
                  assertEqualsIgnoreIndent(expectedStatement, statementCaptor.value)
              }

              it("Should record populate the statement with the value of the version") {
                  verify(preparedStatement).setString(1, version)
              }

              it("Should record populate the statement with the value of the task name") {
                  verify(preparedStatement).setString(2, taskName)
              }

              it("Should execute the statement") {
                  verify(preparedStatement).executeQuery()
              }

              it("Should return false") {
                  assertFalse(result)
              }
          }
      }

      given("A change set table with a matching change set that has an incorrect hash") {

          beforeEachTest {
              whenever(preparedStatement.executeQuery()).thenReturn(ResultsSetStub(hash = "this.is.incorrect"))
          }

          on("checking a change sets hash column doesn't match") {
              val expectedStatement = """SELECT * FROM change_set
                                         WHERE version = ?
                                             AND task = ?
                                      """
              val statementCaptor = argumentCaptor<String>()
              val version = "1.0.0"
              val taskName = "Task Name"
              val query = "SELECT * FROM Table"

              it("Should fail to match the hashes") {
                  assertFailsWith<MagicCarpetParseException> {
                      subject.taskHashMatches(version, taskName, query)
                  }
              }

              it("Should prepare the statement") {
                  verify(connection).prepareStatement(statementCaptor.capture())
                  assertEqualsIgnoreIndent(expectedStatement, statementCaptor.value)
              }

              it("Should record populate the statement with the value of the version") {
                  verify(preparedStatement).setString(1, version)
              }

              it("Should record populate the statement with the value of the task name") {
                  verify(preparedStatement).setString(2, taskName)
              }

              it("Should execute the statement and fail with an exception") {
                  verify(preparedStatement).executeQuery()
              }
          }
      }

      given("A task hash to update") {
          val expectedStatement = """UPDATE change_set
                                     SET hash = ?
                                     WHERE version = ?
                                         AND task = ?
                                         AND hash IS NULL
                                  """
          val version = "1.0.0"
          val task = "Create DB"
          val query = "SELECT * FROM Table"

          on("updating the task hash") {

              val statementCaptor = argumentCaptor<String>()

              subject.updateTaskHash(version, task, query)

              it("Should prepare the statement") {
                  verify(connection).prepareStatement(statementCaptor.capture())
              }

              it("Should set the statement values") {
                  assertEqualsIgnoreIndent(expectedStatement, statementCaptor.value)
                  verify(preparedStatement).setString(1, query.toMD5())
                  verify(preparedStatement).setString(2, version)
                  verify(preparedStatement).setString(3, task)
              }

              it("Should execute the statement") {
                  verify(preparedStatement).executeUpdate()
              }

              it("Should commit NOT the changes") {
                  verify(connection, times(0)).commit()
              }
          }
      }

      given("a failing database connection") {

          val version = "1.0.0"
          val task = "Create DB"
          val query = "SELECT * FROM Table"

          beforeEachTest {
              whenever(connection.commit()).thenThrow(SQLException())
              whenever(connection.close()).thenThrow(SQLException())
              whenever(connection.rollback()).thenThrow(SQLException())
              whenever(preparedStatement.executeQuery()).thenThrow(SQLException())
              whenever(preparedStatement.executeUpdate()).thenThrow(SQLException())
              whenever(preparedStatement.execute()).thenThrow(SQLException())
              whenever(statement.execute(any())).thenThrow(SQLException())
          }

          it("Should fail to commit the changes") {
              assertFailsWith<MagicCarpetDatabaseException> {
                  subject.commit()
              }
          }

          it("Should fail to Close the connection") {
              assertFailsWith<MagicCarpetDatabaseException> {
                  subject.close()
              }
          }

          it("Should fail to execute a statement") {
              assertFailsWith<MagicCarpetDatabaseException> {
                  subject.executeStatement("SELECT * FROM Table")
              }
          }

          it("Should fail to record a task") {
              assertFailsWith<MagicCarpetDatabaseException> {
                  subject.recordTask(version, task, query)
              }
          }

          it("Should fail to check a version exists") {
              assertFailsWith<MagicCarpetDatabaseException> {
                  subject.versionExists(version)
              }
          }

          it("Should fail to check a task exists") {
              assertFailsWith<MagicCarpetDatabaseException> {
                  subject.taskExists(version, task)
              }
          }

          it("Should fail to update a task hash") {
              assertFailsWith<MagicCarpetDatabaseException> {
                  subject.updateTaskHash(version, task, query)
              }
          }

          it("Should fail to find a task hash that matches") {
              assertFailsWith<MagicCarpetDatabaseException> {
                  subject.taskHashMatches(version, task, query)
              }
          }

          it("Should fail to roll back changes") {
              assertFailsWith<MagicCarpetDatabaseException> {
                  subject.rollBack()
              }
          }
      }
  }
}
