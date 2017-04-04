package uk.co.agware.carpet.extensions

import kotlin.test.assertTrue

fun assertEqualsIgnoreIndent(expected: String, actual: String) {
	val result = expected.replace(Regex("\\s+"), "") == actual.replace(Regex("\\s+"), "")
	assertTrue(result, "expected $actual to be $expected")
}
