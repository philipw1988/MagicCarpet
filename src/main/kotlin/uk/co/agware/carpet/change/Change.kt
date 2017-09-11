package uk.co.agware.carpet.change;

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import uk.co.agware.carpet.MagicCarpet
import uk.co.agware.carpet.change.tasks.Task
import uk.co.agware.carpet.exception.MagicCarpetParseException


/**
 * Create a Change with a list of tasks to execute on the database
 *
 * @property version the version of the change
 * @property tasks List of tasks for the change
 * @constructor create the change with tasks
 * Created by Simon on 29/12/2016.
 */
open class Change @JsonCreator constructor(@JsonProperty("version") val version: String,
										   @JsonProperty("tasks") val tasks: List<Task> = listOf()) : Comparable<Change> {

	init {
		if (!MagicCarpet.VERSION_TEST.matches(version)) {
			throw MagicCarpetParseException("Version $version does not match the SemVer pattern")
		}
	}

	/**
	 * Build the version number by converting the string
	 * n.n.n to an integer value.
	 *
	 * For example 1.1.0 will become 1.1 and 1.1.1 would become 1.11 thus
	 * making 1.1.0 smaller than 1.1.1
	 */
	protected fun buildVersionValue(version: String): Double {
		return version.split(".")
			.map(String::toDouble)
			.reduceIndexed { i, acc, next ->
				acc + (next / Math.pow(10.0, i.toDouble()))
			}
	}

	/**
	 * Compares the version strings of both Change objects by
	 * first converting them to decimals that represent their
	 * SemVer value.
	 * Will sort the values in ascending order
	 */
	override fun compareTo(other: Change): Int {
		val thisVersionValue = buildVersionValue(this.version)
		val oVersionValue = buildVersionValue(other.version)
		return thisVersionValue.compareTo(oVersionValue)
	}

	override fun equals(other: Any?): Boolean {
		if (other !is Change) return false
		return this.version == other.version
	}

	override fun hashCode(): Int {
		return this.version.hashCode()
	}

	override fun toString(): String {
		return "Change(version='$version', tasks=$tasks)"
	}
}
