package uk.co.agware.carpet.database

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import uk.co.agware.carpet.exception.MagicCarpetDatabaseException
import uk.co.agware.carpet.exception.MagicCarpetParseException
import java.math.BigInteger
import java.security.MessageDigest
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.Date
import java.sql.SQLException

/**
 * Default Database Connector implementation.
 * Creates a connection to a database.
 * Sets up the table to store changes that have been applied to the database
 */
open class DefaultDatabaseConnector(private val connection: Connection, private val schema: String? = null) : DatabaseConnector {

	companion object {
		protected val TABLE_NAME = "change_set"
		protected val VERSION_COLUMN = "version"
		protected val TASK_COLUMN = "task"
		protected val DATE_COLUMN = "applied"
		protected val HASH_COLUMN = "hash"
	}

	private val logger: Logger = LoggerFactory.getLogger(this.javaClass)

	init {
		this.connection.autoCommit = false
	}

	override fun commit() {
		try {
			this.connection.commit()
		}
		catch (e: SQLException) {
			throw MagicCarpetDatabaseException("Unable to commit changes to database. ${e.message}")
		}
	}

	override fun close() {
		try {
			this.logger.info("Closing connection")
			this.connection.close()
		}
		catch (e: SQLException) {
			throw MagicCarpetDatabaseException("Unable to close database connection. ${e.message}")
		}
	}

	override fun executeStatement(sql: String) {
		try {
			val statement = this.connection.createStatement()
			statement.use { stmt ->
				this.logger.info("Executing statement: {}", sql)
				stmt.execute(sql)
			}
		}
		catch (e: SQLException) {
			throw MagicCarpetDatabaseException("Could not execute statement: $sql. ${e.message}")
		}
	}

	override fun recordTask(version: String, taskName: String, query: String) {
		val sql = """INSERT INTO $TABLE_NAME
					 ($VERSION_COLUMN, $TASK_COLUMN, $DATE_COLUMN, $HASH_COLUMN)
					 VALUES (?, ?, ?, ?)"""
		try {
			val statement = this.connection.prepareStatement(sql)
			statement.use { stmt ->
				stmt.setString(1, version)
				stmt.setString(2, taskName)
				stmt.setDate(3, Date(System.currentTimeMillis()))
				stmt.setString(4, query.toMD5())
				stmt.execute()
			}
		}
		catch (e: SQLException) {
			throw MagicCarpetDatabaseException("Could not insert task: $taskName for change: $version. ${e.message}")
		}
	}

	override fun checkChangeSetTable(createTable: Boolean) {
		val dbm = this.connection.metaData
		val tables = dbm.getTables(null, this.schema, TABLE_NAME, null)

		if (createTable) {
			if (!tables.next()) {
				createChangeSetTable()
				checkTableStructure()
			}
			else {
				checkTableStructure()
			}
		}
	}

	/*
	 * Create the ChangeSet table in its original form, the "checkTableStructure"
	 * function will perform additional changes
	 */
	protected fun createChangeSetTable() {
		try {
			this.logger.info("Creating ChangeSet table")
			val createTableStatement = """CREATE TABLE $TABLE_NAME (
                                            $VERSION_COLUMN VARCHAR(255),
                                            $TASK_COLUMN VARCHAR(255),
                                            $DATE_COLUMN DATE
                                          )"""
			executeStatement(createTableStatement)
			commit()
		}
		catch (e: SQLException) {
			throw MagicCarpetDatabaseException("Could not create table. ${e.message}")
		}
	}

	/* Checks the table for any columns that have been added since the original release */
	protected fun checkTableStructure() {
		val dbm = this.connection.metaData
		checkHashColumn(dbm)
	}

	/* Hash Column was added in 2.0.0 */
	protected fun checkHashColumn(metadata: DatabaseMetaData) {
		try {
			val result = metadata.getColumns(null, this.schema, TABLE_NAME, HASH_COLUMN)
			result.use { rs ->
				if (!rs.next()) {
					this.logger.info("Adding Hash Column")
					executeStatement("ALTER TABLE $TABLE_NAME ADD COLUMN $HASH_COLUMN VARCHAR(64)")
					commit()
				}
			}
		}
		catch (e: SQLException) {
			throw MagicCarpetDatabaseException("Could not alter table: $TABLE_NAME with Column: $HASH_COLUMN. ${e.message}")
		}
	}

	/**
	 * Check if the supplied version exists
	 */
	override fun versionExists(version: String): Boolean {
		val sql = """SELECT * FROM $TABLE_NAME
                      WHERE $VERSION_COLUMN = ?"""
		try {
			val statement = this.connection.prepareStatement(sql)
			statement.use { stmt ->
				stmt.setString(1, version)
				val rs = stmt.executeQuery()
				rs.use {
					return it.next()
				}
			}
		}
		catch (e: SQLException) {
			throw MagicCarpetDatabaseException("Could not execute statement: $sql. ${e.message}")
		}
	}

	/**
	 * Checks whether a change has been previously applied
	 */
	override fun taskExists(version: String, taskName: String): Boolean {
		val select = """SELECT * FROM $TABLE_NAME
                        WHERE $VERSION_COLUMN = ?
                            AND $TASK_COLUMN = ?
                     """
		try {
			val statement = this.connection.prepareStatement(select)
			statement.use { stmt ->
				stmt.setString(1, version)
				stmt.setString(2, taskName)

				val rs = stmt.executeQuery()
				rs.use {
					return it.next()
				}
			}
		}
		catch (e: SQLException) {
			throw MagicCarpetDatabaseException("Could not execute statement: $select. ${e.message}")
		}
	}

	override fun taskHashMatches(version: String, taskName: String, query: String): Boolean {
		val select = """SELECT * FROM $TABLE_NAME
                        WHERE $VERSION_COLUMN = ?
                            AND $TASK_COLUMN = ?
                     """
		try {
			val statement = this.connection.prepareStatement(select)
			statement.use { stmt ->
				stmt.setString(1, version)
				stmt.setString(2, taskName)

				val result = stmt.executeQuery()
				result.use { rs ->
					if (!rs.next()) throw MagicCarpetDatabaseException("Version: $version Task: $taskName does not exist")

					// If there is no hash then we return false
					val storedHash = rs.getString(HASH_COLUMN) ?: return false
					val queryHash = query.toMD5()
					// Check if the hashes match
					if (storedHash == queryHash) {
						return true
					}
					throw MagicCarpetParseException("Stored Hash and calculated hash for $version $taskName do not match.\nExpected: $storedHash\nActual: $queryHash")
				}
			}
		}
		catch (e: SQLException) {
			throw MagicCarpetDatabaseException("Could not execute statement: $select. ${e.message}")
		}
	}

	override fun updateTaskHash(version: String, taskName: String, query: String) {
		//Update the row using version and task name where query is null
		val select = """UPDATE $TABLE_NAME
                        SET $HASH_COLUMN = ?
                        WHERE $VERSION_COLUMN = ?
                            AND $TASK_COLUMN = ?
                            AND $HASH_COLUMN IS NULL
                     """
		try {
			val statement = this.connection.prepareStatement(select)
			statement.use { stmt ->
				stmt.setString(1, query.toMD5())
				stmt.setString(2, version)
				stmt.setString(3, taskName)
				stmt.executeUpdate()
			}
		}
		catch (e: SQLException) {
			throw MagicCarpetDatabaseException("Could not execute statement: $select. ${e.message}")
		}
	}

	override fun rollBack() {
		try {
			this.logger.info("Rolling back transaction")
			this.connection.rollback()
		}
		catch (e: SQLException) {
			throw MagicCarpetDatabaseException("Could not roll back changes. ${e.message}")
		}
	}
}

/* Convert a String to an MD5 Hash */
fun String.toMD5(): String {
	val md5 = MessageDigest.getInstance("MD5")
	md5.reset()
	md5.update(this.toByteArray())
	val digest = md5.digest()
	return BigInteger(1, digest).toString(16)
}
