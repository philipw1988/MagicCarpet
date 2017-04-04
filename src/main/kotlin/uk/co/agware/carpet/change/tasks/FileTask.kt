package uk.co.agware.carpet.change.tasks

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.commons.io.IOUtils
import uk.co.agware.carpet.database.DatabaseConnector
import uk.co.agware.carpet.exception.MagicCarpetParseException
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Task that uses a file path to specify changes to the database.
 * The statements in the file are split using the delimiter.
 *
 * @param taskName name of the task
 * @param taskOrder order to execute the task on the database.
 * @param filePath the file path to the script to execute on the database
 * @param delimiter the delimiter used to split the script into individual statements. Default ;
 * @constructor Set task name, order and scripts with delimiter
 * Created by Simon on 29/12/2016.
 */
class FileTask @JsonCreator constructor(@JsonProperty("taskName") override var taskName: String,
                                        @JsonProperty("taskOrder") override var taskOrder: Int,
                                        @JsonProperty("filePath") val filePath: String,
                                        @JsonProperty("delimiter") delimiter: String = ";") : Task {

    val delimiter = if ("" == delimiter) ";" else delimiter
    override val query = getFileContents()

    override fun performTask(databaseConnector: DatabaseConnector) {
        val statements = this.query.split(this.delimiter)
        statements.forEach { databaseConnector.executeStatement(it.trim()) }
    }

    private fun getFileContents(): String {
        val isClasspathFile = this.filePath.toLowerCase().startsWith("classpath:")

        val contents = when(isClasspathFile) {
            true -> getClasspathContents(this.filePath)
            else -> getPathContents(this.filePath)
        }

        if(contents.isNullOrEmpty()) {
            throw MagicCarpetParseException("Empty file contents for ${this.filePath}")
        }

        return contents
    }

    /* Read the contents of a file on the classpath if it exists */
    private fun getClasspathContents(path: String): String {
        val relativePath = path.replace("classpath:", "")
        val input = javaClass.classLoader.getResourceAsStream(relativePath)

        return when(input != null) {
            true -> IOUtils.toString(input)
            else -> throw MagicCarpetParseException("Unable to find file $path")
        }
    }

    /* Return the contents of a file path if it exists */
    private fun getPathContents(path: String): String {
        val file = Paths.get(path)
        if(Files.exists(file)){
            return String(Files.readAllBytes(file))
        }
        throw MagicCarpetParseException("Unable to find file $file")
    }
}
