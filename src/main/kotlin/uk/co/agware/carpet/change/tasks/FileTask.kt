package uk.co.agware.carpet.change.tasks

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.commons.io.IOUtils
import uk.co.agware.carpet.database.DatabaseConnector
import uk.co.agware.carpet.exception.MagicCarpetException
import java.io.IOException
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
/**
 * Task that uses a file
 * @constructor Set task name, order and scripts with delimiter
 * @param taskName name of the task
 * @param taskOrder order to execute the task
 * @param filePath the file path to the script to execute
 * @param delimiter the delimiter used to split the script. Default ;
 * Created by Simon on 29/12/2016.
 */
class FileTask @JsonCreator constructor(@JsonProperty("taskName") override var taskName: String, @JsonProperty("taskOrder") override var taskOrder: Int, @JsonProperty("filePath") val filePath: String, @JsonProperty("delimiter") delimiter: String?) : Task {

    val delimiter: String
    init {
        this.delimiter = if (delimiter == null || "" == delimiter) ";" else delimiter
    }

    @Override
    override fun performTask(databaseConnector: DatabaseConnector?): Boolean {
        try {
            val contents: String = String(getFileContents())
            val statements: List<String> = contents.split(this.delimiter.orEmpty())
            statements.forEach { s -> if(!databaseConnector!!.executeStatement(s.trim())) return false }
        } catch (e: IOException) {
            throw MagicCarpetException("Error executing statement", e)
        }
        return true
    }

    fun getFileContents(): ByteArray {
        if(this.filePath.toLowerCase().startsWith("classpath:")){
            val filename: String = this.filePath.replace("classpath:", "")
            val input : InputStream = javaClass.classLoader.getResourceAsStream(filename)
            return IOUtils.toByteArray(input)

        }
        else {
            val path: Path = Paths.get(this.filePath)
            if(Files.exists(path)){
                return Files.readAllBytes(path)
            }
        }
        throw MagicCarpetException("Unable to find file " +this.filePath)
    }
}
