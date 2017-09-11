package uk.co.agware.carpet.database

/**
 * Set up a database connection
 * Controls interaction with the database.
 *
 * Created by Simon on 29/12/2016.
 */
interface DatabaseConnector : AutoCloseable {

	/**
	 * Commit Changes to the database
	 * @return Boolean
	 */
	fun commit()

	/**
	 * close the connection to the database
	 * @return Boolean
	 */
	override fun close()

	/**
	 * Insert a change into the database changes table
	 * Inserts version, task name and hash of query run
	 * @param version The change version
	 * @param taskName the name of the task executed
	 * @param query the query to insert
	 * @return Boolean
	 */
	fun recordTask(version: String, taskName: String, query: String)

	/**
	 * execute a statement on the database
	 * @param sql SQL statement to execute
	 * @return Boolean
	 */
	fun executeStatement(sql: String)

	/**
	 * Create the changes table in the database if it does not exist and *createTable* is true
	 * @param createTable Create the changes table if it does not exist
	 * @return Boolean
	 */
	fun checkChangeSetTable(createTable: Boolean)

	/**
	 * Checks if there are any existing tasks for this version number, allows us to skip doing any
	 * checks on the tasks before deploying them
	 */
	fun versionExists(version: String): Boolean

	/**
	 * Check if a change task exists in the database using the version, task name and hash of query
	 * @param version The Change Version
	 * @param taskName The Name of the task
	 * @param query The query to check.
	 * @return Boolean
	 */
	fun taskExists(version: String, taskName: String): Boolean

	/**
	 * Verifies that the Hash for the supplied task is correct
	 */
	fun taskHashMatches(version: String, taskName: String, query: String): Boolean

	/**
	 * Update a tasks table with new query hash if it doesn't already have one
	 * @param version The Change Version
	 * @param taskName The Name of the task
	 * @param query The query to update.
	 * @return Boolean
	 */
	fun updateTaskHash(version: String, taskName: String, query: String)

	/**
	 * Rollback changes on the database
	 */
	fun rollBack()
}
