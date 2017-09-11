package uk.co.agware.carpet.change.tasks

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo
import uk.co.agware.carpet.database.DatabaseConnector

@JsonTypeInfo(
	use = JsonTypeInfo.Id.NAME,
	include = JsonTypeInfo.As.PROPERTY,
	property = "type")
@JsonSubTypes(
	Type(value = FileTask::class, name = "FileTask"),
	Type(value = ScriptTask::class, name = "ScriptTask")
			 )
interface Task : Comparable<Task> {
	var taskName: String
	var taskOrder: Int
	val query: String

	/**
	 * Execute the task on the database
	 * @param databaseConnector the connection to the database
	 */
	fun performTask(databaseConnector: DatabaseConnector)

	override fun compareTo(other: Task): Int {
		return this.taskOrder.compareTo(other.taskOrder)
	}
}
