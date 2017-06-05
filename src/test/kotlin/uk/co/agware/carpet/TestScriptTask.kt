package uk.co.agware.carpet

import com.nhaarman.mockito_kotlin.argumentCaptor
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.*
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import uk.co.agware.carpet.change.tasks.ScriptTask
import uk.co.agware.carpet.database.DatabaseConnector
import uk.co.agware.carpet.exception.MagicCarpetParseException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

@RunWith(JUnitPlatform::class)
class TestScriptTask: Spek(spek)
private val spek: Dsl.() -> Unit = {

    describe("A ScriptTask object") {

        var connection = mock<DatabaseConnector>()

        beforeEachTest {
            connection = mock<DatabaseConnector>()
        }

        given("A Script Task with a default delimiter") {
            val subject = ScriptTask("Test Task", 1, "SELECT * FROM Table; SELECT * FROM Other_Table")

            on("Performing the task") {

                subject.performTask(connection)

                it("should execute each statement") {
                    val statement = argumentCaptor<String>()
                    verify(connection,times(2)).executeStatement(statement.capture())
                    assertEquals(2, statement.allValues.size)
                    assertTrue(statement.allValues.contains("SELECT * FROM Table"))
                    assertTrue(statement.allValues.contains("SELECT * FROM Other_Table"))
                }
            }
        }

        given("a script task with a custom delimiter") {
            val subject = ScriptTask("Test Task", 1, "SELECT * FROM Table, SELECT * FROM Other_Table", ",")

            on("Performing the task") {

                subject.performTask(connection)

                it("should execute each statement") {
                    val statement = argumentCaptor<String>()
                    verify(connection,times(2)).executeStatement(statement.capture())
                    assertEquals(2, statement.allValues.size)
                    assertTrue(statement.allValues.contains("SELECT * FROM Table"))
                    assertTrue(statement.allValues.contains("SELECT * FROM Other_Table"))
                }
            }
        }

        it("should fail with a MagicCarpetParseException") {
            assertFailsWith<MagicCarpetParseException> {
                ScriptTask("Test Task", 1, "")
            }
        }
    }

}
