package uk.co.agware.carpet

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import uk.co.agware.carpet.change.Change
import uk.co.agware.carpet.change.tasks.FileTask
import uk.co.agware.carpet.change.tasks.Task
import uk.co.agware.carpet.database.DatabaseConnector
import uk.co.agware.carpet.exception.MagicCarpetDatabaseException
import uk.co.agware.carpet.exception.MagicCarpetException
import uk.co.agware.carpet.exception.MagicCarpetParseException
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.util.stream.Stream

/**
 * Executes a set of changes on a database from a specified change set.
 * Accepts changes in JSON, XML or as a directory structure with numbered files
 * Changes are executed through the assigned database connection
 * Changes are tracked through an additional database table
 * Table will automatically be added or updated when run
 * If a change has already been made on the database it is ignored
 * Change statements are hashed in the database table
 * Changes can be Scripts, files or files on the classpath
 *
 * @param databaseConnector connection to the database to update.
 * @param devMode when set changes will not be executed on the database
 * @constructor Sets the database connection and dev mode.
 * @see uk.co.agware.carpet.database.DatabaseConnector
 * Created by Simon on 29/12/2016.
 */
// TODO Needs debug level logging in places it makes sense to have it so that it is easier to see whats going on
// TODO for someone with this library in their application if they need to actually do some debugging

// TODO Should ideally filter the directory contents by .sql when checking for the task files
open class MagicCarpet(private val databaseConnector: DatabaseConnector,
					   val isDevMode: Boolean = false) {

	companion object {
		val VERSION_TEST = Regex(""".*\d+\.\d+(\.\d+)+$""")
		// Will match three groups, the first being potentially a number, the second being potentially
		// some set of separating characters and the third being everything else, this is for matching
		// file names such as "12 - Create new Table" and "1.Add some things" and extracting both the
		// number and the name without worrying if the number and/or separating characters do not exist
		// The only edge case on this is going to be if someone starts a Task name with a number.
		val TASK_NAME_PATTERN = Regex("""(\d?)([ -:.]?)(.?)""").toPattern()
	}

	private val logger: Logger = LoggerFactory.getLogger(this.javaClass)
	val createTable = true

	private val xmlMapper: ObjectMapper = XmlMapper().registerModule(KotlinModule())

	fun parseChanges(inputStream: InputStream): List<Change> {
		try {
			return this.xmlMapper.readValue(inputStream)
		}
		catch (e: IOException) {
			throw MagicCarpetParseException("Unable to read provided file at. ${e.message}")
		}
	}

	fun parseChanges(baseDir: String): List<Change> {
		val versionDirs = this.readFiles(baseDir).filter { VERSION_TEST.matches(it) }
		return versionDirs.map { this.createChange(it) }
	}

	private fun readFiles(baseDir: String): List<String> {
		val input = this.javaClass.getResourceAsStream(baseDir) ?: throw MagicCarpetParseException("Unable to read directory of $baseDir")
		BufferedReader(InputStreamReader(input)).use {
			return it.lines().map { "$baseDir/$it" }.toList()
		}
	}

	/*
	 * Iterates through all folders within the supplied path and builds up a set
	 * of changes to be applied. It will only check one level deep to avoid checking
	 * the same file multiple times and building an incorrect graph
	 */
	private fun createChange(path: String): Change {
		val files = this.readFiles(path)
		val version = path.substringAfterLast("/").removeSuffix("/")
		val tasks = files.map { this.toFileTask(it) }
		return Change(version, tasks)
	}

	private fun toFileTask(path: String): Task {
		val fileName = path.substringAfterLast("/").substringBeforeLast(".")
		val matcher = TASK_NAME_PATTERN.matcher(fileName)
		matcher.find()
		val order = when (matcher.group(1).isEmpty()) {
			true -> Int.MAX_VALUE.toString()
			else -> matcher.group(1)
		}
		val taskName = matcher.group(3)
		return FileTask(taskName, order.toInt(), path)
	}

	/**
	 * Perform each task on the database
	 * No changes are implemented if devMode is set
	 *
	 * @return boolean tasks all executed successfully
	 */
	fun executeChanges(changes: List<Change>) {

		// Will close the database connector after completion
		this.databaseConnector.use { connector ->
			connector.checkChangeSetTable(this.createTable)
			try {
				changes.sorted()
					.forEach { change ->
						when (connector.versionExists(change.version)) {
							true -> this.validateExistingChange(change, connector)
							else -> change.tasks.forEach { task -> runTask(change.version, task, connector) }
						}
					}
			}
			catch (e: MagicCarpetException) {
				connector.rollBack()
				throw e
			}
			connector.commit()
		}
	}

	/*
	 * Checks the hashes of all the tasks that currently exist within the database to make sure the hashes have not
	 * been altered since they were applied
	 */
	private fun validateExistingChange(change: Change, connector: DatabaseConnector) {
		change.tasks.sorted()
			.forEach { task ->
				when (connector.taskExists(change.version, task.taskName)) {
					true -> validateTaskHash(change.version, task, connector)
					else -> runTask(change.version, task, connector)
				}
			}
	}

	private fun validateTaskHash(version: String, task: Task, connector: DatabaseConnector) {
		if (!connector.taskHashMatches(version, task.taskName, task.query)) {
			connector.updateTaskHash(version, task.taskName, task.query)
		}
	}

	/*
	 * Runs a Task and then records the task in the database, will catch and rethrow any exceptions by adding extra
	  * information onto the exception message.
	 */
	private fun runTask(version: String, task: Task, connector: DatabaseConnector) {
		try {
			task.performTask(connector)
			connector.recordTask(version, task.taskName, task.query)
		}
		catch (e: Exception) {
			throw MagicCarpetDatabaseException("Error running task $version ${task.taskName}. ${e.message}")
		}
	}

	fun run(file: InputStream) {
		if (this.isDevMode) {
			this.logger.info("MagicCarpet set to Dev Mode, changes not being implemented")
			return
		}
		val changes = this.parseChanges(file)
		this.executeChanges(changes)
	}

	fun run(baseDir: String) {
		if (this.isDevMode) {
			this.logger.info("MagicCarpet set to Dev Mode, changes not being implemented")
			return
		}
		val changes = this.parseChanges(baseDir)
		this.executeChanges(changes)
	}

	fun run() {
		val input = this.javaClass.getResourceAsStream("ChangeSet.xml")
		this.run(input)
	}
}

/* I needed it a few times... */
internal fun <T> Stream<T>.toList(): List<T> {
	return Sequence { this.iterator() }.toList()
}
