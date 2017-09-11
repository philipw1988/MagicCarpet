package uk.co.agware.carpet

import com.nhaarman.mockito_kotlin.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.*
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import uk.co.agware.carpet.database.DefaultDatabaseConnector
import uk.co.agware.carpet.exception.MagicCarpetParseException
import uk.co.agware.carpet.stubs.ResultsSetStub
import java.io.ByteArrayInputStream
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DatabaseMetaData
import java.sql.PreparedStatement
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

@RunWith(JUnitPlatform::class)
class TestMagicCarpet : Spek(spek)

private val spek: Dsl.() -> Unit = {

	describe("MagicCarpet") {

		var connector = mock<DefaultDatabaseConnector>()
		var metaData: DatabaseMetaData
		var preparedStatement: PreparedStatement
		var carpet = MagicCarpet(connector)

		beforeEachTest {
			connector = mock()
			metaData = mock()
			preparedStatement = mock()
			whenever(preparedStatement.execute()).thenReturn(true)
			whenever(metaData.getTables(null, null, "change_set", null)).thenReturn(ResultsSetStub(true))
			whenever(metaData.getColumns(null, null, "change_set", "hash")).thenReturn(ResultsSetStub(true))
		}

		given("A base xml file path") {

			val path = Paths.get("src/test/resources/files/ChangeSet.xml")
			val bytes = Files.readAllBytes(path)

			beforeEachTest {
				carpet = MagicCarpet(connector)
				whenever(connector.versionExists(any())).thenReturn(false)
			}

			it("Should read the xml changes") {
				val input = ByteArrayInputStream(bytes)
				val changes = carpet.parseChanges(input)
				assertEquals(2, changes.size)
				assertEquals(1, changes[0].tasks.size)
			}

			on("Executing changes") {

				val input = ByteArrayInputStream(bytes)
				val statementCaptor = argumentCaptor<String>()
				carpet.run(input)

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

			val baseDir = "/files/nest"

			beforeEachTest {
				carpet = MagicCarpet(connector)
				whenever(connector.versionExists(any())).thenReturn(false)
			}

			it("Should read the xml changes") {
				val changes = carpet.parseChanges(baseDir)
				assertEquals(3, changes.size)
				assertEquals(1, changes[0].tasks.size)
			}

			on("Executing changes") {

				val statementCaptor = argumentCaptor<String>()
				carpet.run(baseDir)

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

		given("A nested changeSet that uses a file that doesn't exist") {
			val path = Paths.get("src/test/resources/files/nofile_changeset/ChangeSet.xml")
			val bytes = Files.readAllBytes(path)

			beforeEachTest {
				carpet = MagicCarpet(connector)
				whenever(connector.versionExists(any())).thenReturn(false)
			}

			it("should fail to parse the changes") {
				val input = ByteArrayInputStream(bytes)
				assertFailsWith<MagicCarpetParseException> {
					carpet.parseChanges(input)
				}

			}
		}

		given("An invalid changeSet.xml") {
			val path = Paths.get("src/test/resources/files/invalid_xml/ChangeSet.xml")
			val bytes = Files.readAllBytes(path)

			beforeEachTest {
				carpet = MagicCarpet(connector)
				whenever(connector.versionExists(any())).thenReturn(false)
			}

			it("should fail to parse the changes") {
				val input = ByteArrayInputStream(bytes)
				assertFailsWith<MagicCarpetParseException> {
					carpet.parseChanges(input)
				}
			}
		}
	}
}
