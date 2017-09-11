package uk.co.agware.carpet

import com.nhaarman.mockito_kotlin.argumentCaptor
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.*
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import uk.co.agware.carpet.change.tasks.FileTask
import uk.co.agware.carpet.database.DatabaseConnector
import uk.co.agware.carpet.exception.MagicCarpetParseException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

@RunWith(JUnitPlatform::class)
class TestFileTask : Spek(spek)

private val spek: Dsl.() -> Unit = {

	describe("A FileTask Object") {

		var connection = mock<DatabaseConnector>()

		beforeEachTest {
			connection = mock()
		}

		given("A file on the classpath") {
			val task = FileTask("Test Task", 1, "classpath:classpathTest.sql", ",")

			on("performing the task") {

				task.performTask(connection)

				it("should execute each task statement") {
					val statements = argumentCaptor<String>()
					verify(connection, times(2))
						.executeStatement(statements.capture())
					assertEquals(2, statements.allValues.size)
					assertTrue(statements.allValues.contains("SELECT * FROM Classpath"))
					assertTrue(statements.allValues.contains("SELECT * FROM class_path"))
				}
			}

		}

		given("A file system file") {
			val subject = FileTask("Test Task", 1, "/files/test.sql")

			on("performing the task") {

				subject.performTask(connection)

				it("should execute each statement in the task") {
					val statements = argumentCaptor<String>()
					verify(connection, times(2))
						.executeStatement(statements.capture())
					assertEquals(2, statements.allValues.size)
					assertTrue(statements.allValues.contains("SELECT * FROM Table"))
					assertTrue(statements.allValues.contains("SELECT * FROM Other_Table"))
				}
			}
		}

		it("should fail with a MagicCarpetParseException") {
			assertFailsWith<MagicCarpetParseException> {
				FileTask("A Failing Task", 2, "this/does/not/exist")
			}
		}

		it("should fail with a MagicCarpetParseException") {
			assertFailsWith<MagicCarpetParseException> {
				FileTask("Test Task", 1, "/files/empty.sql")
			}
		}

		it("should fail with a MagicCarpetParseException") {
			assertFailsWith<MagicCarpetParseException> {
				FileTask("A Failing Task", 2, "classpath:this.does.not.exist")
			}
		}
	}
}
