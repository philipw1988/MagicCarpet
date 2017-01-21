package uk.co.agware.carpet.database

import java.sql.Connection
/**
 * Database connection interface
 *
 * Created by Simon on 29/12/2016.
 */
interface DatabaseConnector {

    /**
     * Set the connection to the database with JDBC *String*
     * @param jdbcName
     */
    fun setConnection(jdbcName: String)

    /**
     * Set the connection to the database with connection URL, username and Password
     * @param connectionUrl
     * @param name
     * @param password
     */
    fun setConnection(connectionUrl: String, name: String, password: String)

    /**
     * Set the connection to the database as *Connection*
     * @see java.sql.Connection
     * @property connection
     */
    fun setConnection(connection: Connection)

    /**
     * Commit Changes to the database
     * @return Boolean
     */
    fun commit(): Boolean

    /**
     * close the connection to the database
     * @return Boolean
     */
    fun close(): Boolean

    /**
     * Insert a change into the database changes table
     * @param version The change version
     * @param taskName the name of the task executed
     * @return Boolean
     */
    fun insertChange(version: String, taskName: String): Boolean

    /**
     * execute a statement on the database
     * @param sql SQL statement to execute
     * @return Boolean
     */
    fun executeStatement(sql: String): Boolean

    /**
     * Create the changes table in the database if it does not exist and *createTable* is true
     * @param createTable Create the changes table if it does not exist
     * @return Boolean
     */
    fun checkChangeSetTable(createTable: Boolean)

    /**
     * Check if a change task exists in the database
     * @param version The Change Version
     * @param taskName The Name of the task
     * @return Boolean
     */
    fun changeExists(version: String, taskName: String): Boolean

    /**
     * Rollback changes on the database
     */
    fun rollBack()
}