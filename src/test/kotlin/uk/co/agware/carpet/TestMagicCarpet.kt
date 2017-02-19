package uk.co.agware.carpet

import com.nhaarman.mockito_kotlin.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import uk.co.agware.carpet.database.DefaultDatabaseConnector
import uk.co.agware.carpet.exception.MagicCarpetParseException
import uk.co.agware.carpet.stubs.ResultsSetStub
import java.nio.file.Paths
import java.sql.DatabaseMetaData
import java.sql.PreparedStatement
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@RunWith(JUnitPlatform::class)
class TestMagicCarpet: Spek({

  describe("MagicCarpet") {

    var connector = mock<DefaultDatabaseConnector>()
    var metaData: DatabaseMetaData
    var preparedStatement: PreparedStatement
    var subject = MagicCarpet(connector)

    beforeEachTest {
      connector = mock<DefaultDatabaseConnector>()
      metaData = mock<DatabaseMetaData>()
      preparedStatement = mock<PreparedStatement>()

      whenever(preparedStatement.execute()).thenReturn(true)
      whenever(metaData.getTables(null, null, "change_set", null))
              .thenReturn(ResultsSetStub(true))
      whenever(metaData.getColumns(null, null, "change_set", "hash"))
              .thenReturn(ResultsSetStub(true))
    }

    given("A base xml file path") {

      val path = Paths.get("src/test/files/ChangeSet.xml")

      beforeEachTest {
        subject = MagicCarpet(connector, basePath = path)
        whenever(connector.versionExists(any())).thenReturn(false)
      }

      it("Should read the xml changes") {

        subject.parseChanges()

        assertEquals(2, subject.changes.size)
        assertEquals(1, subject.changes[0].tasks.size)
      }


      on("Executing changes") {

        val statementCaptor = argumentCaptor<String>()
        subject.run()

        it("should check the change set table exists") {
          verify(connector).checkChangeSetTable(any())
        }

        it("should check the version exists in the database") {
          verify(connector, times(2)).versionExists(any())
        }

        it("should perform the tasks") {
          verify(connector, times(5)).executeStatement(statementCaptor.capture())
        }

        it("should execute all statements") {
          assertEquals(5, statementCaptor.allValues.size)
          assertTrue {
            statementCaptor.allValues.contains("create table test(version integer, test date)")
            statementCaptor.allValues.contains("alter table test add column another varchar(64)")
            statementCaptor.allValues.contains("create table second(version varchar(64))")
            statementCaptor.allValues.contains("SELECT * FROM Table")
            statementCaptor.allValues.contains("SELECT * FROM Other_Table")
          }
        }

        it("should store all tasks in the database") {
          verify(connector, times(2)).recordTask(any(), any(), any())
        }
      }
    }

    given("A base json file path") {

      val path = Paths.get("src/test/files/ChangeSet.json")

      beforeEachTest {
        subject = MagicCarpet(connector, basePath = path)
        whenever(connector.versionExists(any())).thenReturn(false)
      }

      it("Should read the json changes") {
        subject.parseChanges()
        assertEquals(2, subject.changes.size)
        assertEquals(1, subject.changes[0].tasks.size)
      }

      on("Executing changes") {

        val statementCaptor = argumentCaptor<String>()
        subject.run()

        it("should check the change set table exists") {
          verify(connector).checkChangeSetTable(any())
        }

        it("should check the version exists in the database") {
          verify(connector, times(2)).versionExists(any())
        }

        it("should perform the tasks") {
          verify(connector, times(5)).executeStatement(statementCaptor.capture())
        }

        it("should execute all statements") {
          assertEquals(5, statementCaptor.allValues.size)
          assertTrue {
            statementCaptor.allValues.contains("create table test(version integer, test date)")
            statementCaptor.allValues.contains("alter table test add column another varchar(64)")
            statementCaptor.allValues.contains("create table second(version varchar(64))")
            statementCaptor.allValues.contains("SELECT * FROM Table")
            statementCaptor.allValues.contains("SELECT * FROM Other_Table")
          }
        }

        it("should store all tasks in the database") {
          verify(connector, times(2)).recordTask(any(), any(), any())
        }
      }
    }

    given("A directory structure") {
      val path = Paths.get("src/test/files/nest")

      beforeEachTest {
        subject = MagicCarpet(connector, basePath = path)
        whenever(connector.versionExists(any())).thenReturn(false)
      }

      it("Should read the xml changes") {
        subject.parseChanges()
        assertEquals(3, subject.changes.size)
        assertEquals(1, subject.changes[0].tasks.size)
      }

      on("Executing changes") {

        val statementCaptor = argumentCaptor<String>()
        subject.run()

        it("should check the change set table exists") {
          verify(connector).checkChangeSetTable(any())
        }

        it("should check the version exists in the database") {
          verify(connector, times(3)).versionExists(any())
        }

        it("should perform the tasks") {
          verify(connector, times(6)).executeStatement(statementCaptor.capture())
        }

        it("should execute all statements") {
          assertEquals(6, statementCaptor.allValues.size)
          assertTrue {
            statementCaptor.allValues.contains("create table test(version integer, test date)")
            statementCaptor.allValues.contains("alter table test add column another varchar(64)")
            statementCaptor.allValues.contains("create table second(version varchar(64))")
            statementCaptor.allValues.contains("create table third(version varchar(64))")
            statementCaptor.allValues.contains("SELECT * FROM Table")
            statementCaptor.allValues.contains("SELECT * FROM Other_Table")
          }
        }

        it("should store all tasks in the database") {
          verify(connector, times(6)).recordTask(any(), any(), any())
        }
      }
    }

    given("A File that doesn't exist") {

      val path = Paths.get("this/does/not/exist")

      beforeEachTest {
        subject = MagicCarpet(connector, basePath = path)
        whenever(connector.versionExists(any())).thenReturn(false)
      }

      it("should fail to parse the changes") {
        assertFailsWith<MagicCarpetParseException>{
          subject.parseChanges()
        }

      }
    }

    given("A nested changeSet that uses a file that doesn't exist") {

      val path = Paths.get("src/test/files/nofile_changeset")

      beforeEachTest {
        subject = MagicCarpet(connector, basePath = path)
        whenever(connector.versionExists(any())).thenReturn(false)
      }

      it("should fail to parse the changes") {
        assertFailsWith<MagicCarpetParseException>{
          subject.parseChanges()
        }

      }
    }

    given("An invalid changeSet.xml") {

      val path = Paths.get("src/test/files/invalid_xml")

      beforeEachTest {
        subject = MagicCarpet(connector, basePath = path)
        whenever(connector.versionExists(any())).thenReturn(false)
      }

      it("should fail to parse the changes") {
        assertFailsWith<MagicCarpetParseException>{
          subject.parseChanges()
        }

      }
    }

    given("An invalid changeSet.json") {

      val path = Paths.get("src/test/files/invalid_json")

      beforeEachTest {
        subject = MagicCarpet(connector, basePath = path)
        whenever(connector.versionExists(any())).thenReturn(false)
      }

      it("should fail to parse the changes") {
        assertFailsWith<MagicCarpetParseException>{
          subject.parseChanges()
        }

      }
    }

    given("A base json file path and developer mode on") {

      val path = Paths.get("src/test/files/ChangeSet.json")

      beforeEachTest {
        subject = MagicCarpet(connector, devMode = true, basePath = path)
        whenever(connector.versionExists(any())).thenReturn(false)
      }

      it("Should not read the json changes") {
        subject.parseChanges()
        assertEquals(0, subject.changes.size)
      }

      on("Executing changes") {

        val statementCaptor = argumentCaptor<String>()
        subject.executeChanges()

        it("should not check the change set table exists") {
          verify(connector, never()).checkChangeSetTable(any())
        }

        it("should not check the version exists in the database") {
          verify(connector, never()).versionExists(any())
        }

        it("should not perform the tasks") {
          verify(connector, never()).executeStatement(statementCaptor.capture())
          assertEquals(0, statementCaptor.allValues.size)

        }

        it("should not record the tasks") {
          verify(connector, never()).recordTask(any(), any(), any())
        }
      }
    }

    given("A base json file path with changes that have already been run") {

      val path = Paths.get("src/test/files/ChangeSet.json")

      beforeEachTest {
        subject = MagicCarpet(connector, basePath = path)
        whenever(connector.versionExists(any())).thenReturn(true)
        whenever(connector.taskExists(any(), any())).thenReturn(true)
        whenever(connector.taskHashMatches(any(), any(), any())).thenReturn(false)
      }

      it("Should read the json changes") {
        subject.parseChanges()
        assertEquals(2, subject.changes.size)
        assertEquals(1, subject.changes[0].tasks.size)
      }

      on("Executing changes") {

        subject.run()

        it("should check the change set table exists") {
          verify(connector).checkChangeSetTable(true)
        }

        it("should check the version exists in the database") {
          verify(connector).versionExists("1.0.0")
          verify(connector).versionExists("1.0.1")
        }

        it("should check the task exists"){
          verify(connector).taskExists("1.0.0", "Create Tables")
          verify(connector).taskExists("1.0.1", "Alter table")
        }

        it("should update the task hash"){
          verify(connector).updateTaskHash("1.0.0", "Create Tables",
                                           "create table test(version integer, test date);" +
                                           " alter table test add column another varchar(64);" +
                                           " create table second(version varchar(64))")
          verify(connector).updateTaskHash("1.0.1", "Alter table",
                                           "SELECT * FROM Table;\r\nSELECT * FROM Other_Table")
        }

        it("should not perform the tasks") {
            val statementCaptor = argumentCaptor<String>()
            verify(connector, never()).executeStatement(statementCaptor.capture())
            assertEquals(0, statementCaptor.allValues.size)
        }

        it("should not record the tasks") {
          verify(connector, never()).recordTask(any(), any(), any())
        }

      }
    }

    given("A base json file path with some changes that have already been run") {

      val path = Paths.get("src/test/files/partially_completed/ChangeSet.json")
      val createTablesStatement = "create table test(version integer, test date);" +
              " alter table test add column another varchar(64);" +
              " create table second(version varchar(64))"
      val createTables2Statement = "create table test2(version integer, test date);" +
              " alter table test2 add column another varchar(64);" +
              " create table second2(version varchar(64))"
      val alterTableStatement = "SELECT * FROM Table;\r\nSELECT * FROM Other_Table"

      beforeEachTest {
        subject = MagicCarpet(connector, basePath = path)
        whenever(connector.versionExists("1.0.0")).thenReturn(true)
        whenever(connector.versionExists("1.0.1")).thenReturn(false)
        whenever(connector.taskExists("1.0.0", "Create Tables")).thenReturn(true)
        whenever(connector.taskExists("1.0.0", "Create Tables 2")).thenReturn(false)
        whenever(connector.taskExists("1.0.1", "Alter table")).thenReturn(false)
        whenever(connector.taskHashMatches(any(), any(), any())).thenReturn(false)
      }

      it("Should read the json changes") {
        subject.parseChanges()
        assertEquals(2, subject.changes.size)
        assertEquals(2, subject.changes[0].tasks.size)
        assertEquals(1, subject.changes[1].tasks.size)
      }

      on("Executing parsed changes") {

        subject.run()

        it("should check the change set table exists") {
          verify(connector).checkChangeSetTable(true)
        }

        it("should check the version exists in the database") {
          verify(connector).versionExists("1.0.0")
          verify(connector).versionExists("1.0.1")
          assertTrue(connector.versionExists("1.0.0"))
          assertFalse(connector.versionExists("1.0.1"))
        }

        it("should check the task exists"){
          verify(connector).taskExists("1.0.0", "Create Tables")
          verify(connector).taskExists("1.0.0", "Create Tables 2")
          assertTrue(connector.taskExists("1.0.0", "Create Tables"))
          assertFalse(connector.taskExists("1.0.0", "Create Tables 2"))
        }

        it("should not check if a task exists when the change doesnt exist"){
          verify(connector, never()).taskExists("1.0.1", "Alter table")
        }

        it("should update the task hash if the task exists"){
          verify(connector).updateTaskHash("1.0.0", "Create Tables", createTablesStatement)
        }

        it("should not update the task hash if the task doesnt exist"){
          verify(connector, never()).updateTaskHash("1.0.0", "Create Tables 2", createTables2Statement)
          verify(connector, never()).updateTaskHash("1.0.1", "Alter table", alterTableStatement)
        }

        it("should not execute an existing task") {
          verify(connector, never()).executeStatement("create table test(version integer, test date)")
          verify(connector, never()).executeStatement("alter table test add column another varchar(64)")
          verify(connector, never()).executeStatement("create table second(version varchar(64))")
        }

        it("should execute any new task") {
          verify(connector).executeStatement("create table test2(version integer, test date)")
          verify(connector).executeStatement("alter table test2 add column another varchar(64)")
          verify(connector).executeStatement("create table second2(version varchar(64))")
          verify(connector).executeStatement("SELECT * FROM Table")
          verify(connector).executeStatement("SELECT * FROM Other_Table")
        }

        it("should record the tasks if they have not been recorded before") {
          verify(connector).recordTask("1.0.0", "Create Tables 2", createTables2Statement)
          verify(connector).recordTask("1.0.1", "Alter table", alterTableStatement)
        }

        it("should not record the tasks if they have been recorded before") {
          verify(connector, never()).recordTask("1.0.0", "Create Tables", createTablesStatement)
        }

      }
    }

  }
})
