import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

object DataWriters {

	case class FileWriter(spark: SparkSession) {

		val tempPath = "./data/temp/"
		val finalPath = "./data/out"

		def writeSingleFile(data: DataFrame, fileName: String) = {

			val fileFormat = fileName.split("\\.").last

			fileFormat match {
				case "csv" => data.coalesce(1).write.option("header", true).mode("overwrite").csv(tempPath)
				case "json" => data.coalesce(1).write.option("header", true).mode("overwrite").json(tempPath)
				case _ => throw new Exception("Unsupported file format")
			}

			// Rename part-0000 file to desired name
			import org.apache.hadoop.fs.{FileSystem, Path}
			val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
			val tempName = fs.globStatus(new Path(s"$tempPath/*.$fileFormat"))(0).getPath().getName()
			fs.rename(new Path(tempPath + tempName), new Path(s"$finalPath/$fileName"))

			// Delete temporary files generated by Spark
			import scala.reflect.io.Directory
			import java.io.File
			val tempDir = new Directory(new File(tempPath))
			tempDir.deleteRecursively()
			for {
				files <- Option(new File(finalPath).listFiles)
				file <- files if file.getName.endsWith("crc")
			} file.delete()
		}

	}

}