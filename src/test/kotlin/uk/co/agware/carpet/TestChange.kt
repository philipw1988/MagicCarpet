package uk.co.agware.carpet

import org.jetbrains.spek.api.SubjectSpek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import uk.co.agware.carpet.change.Change
import uk.co.agware.carpet.exception.MagicCarpetParseException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

@RunWith(JUnitPlatform::class)
class TestChange: SubjectSpek<Change>({

    describe("A Change object") {

        subject { Change("1.0.0") }

        it("should equal a Change with the same version") {
            assertEquals(subject, Change("1.0.0"))
        }

        it("should not equal a Change with a different version") {
            assertNotEquals(subject, Change("2.0.0"))
        }

        it("should fail when a supplied version does not match SemVer") {
            assertFailsWith<MagicCarpetParseException> {
                Change("1")
            }
        }

        it("should fail when a supplied version contains invalid characters") {
            assertFailsWith<MagicCarpetParseException> {
                Change("1.d.0")
            }
        }

        it("should order changes using the version number") {
            assertTrue { subject < Change("1.1.0") }
        }
    }
 })
