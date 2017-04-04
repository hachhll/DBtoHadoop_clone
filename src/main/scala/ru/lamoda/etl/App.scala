package ru.lamoda.etl

import java.io.File

import knobs.{Config, FileResource, Required}
import org.rogach.scallop.ScallopConf
import ru.lamoda.etl.hadoop.DataLoader
import ru.lamoda.etl.metadata.MappingMeta
import ru.lamoda.etl.pentaho.PentahoTableToFile

import scala.collection.immutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scalaz.concurrent.Task

/**
  * Hello world!
  *
  */

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val apples = opt[Int](required = false)
  val bananas = opt[Int](required = false)
  val name = trailArg[String](required = false)
  verify()
}

object App {
  def main(args: Array[String]): Unit = {
    val conf = new Conf(args) // Note: This line also works for "object Main extends App"

    //Must be in unput parameters
    val rootDir: String = new File(".").getAbsolutePath

    val generalConfig: Task[Config] = knobs.loadImmutable(Required(FileResource(new File(rootDir + "/config/common.cfg"))) :: Nil)
    val config: Config = generalConfig.unsafePerformSync
    val cnf: Config = new Config(Map("common.root_dir" -> knobs.CfgText(rootDir)))

    val mapMeta = new MappingMeta()
    mapMeta.loadMetaTable()

    val pentahoTTF = new PentahoTableToFile(config ++ cnf, mapMeta)
    println("Prepare Pentaho KTR ...")
    pentahoTTF.prepareKTR()
    println("Execute Pentaho KTR ...")
    pentahoTTF.executeKTR()
    println("Execution result: ")
    for (row <- pentahoTTF.getResult) {
      println(row._1 + " ==> " + row._2)
    }

    // File transfer
    val dataLoader = new DataLoader(config ++ cnf, mapMeta)

    println("Moving files")
    try {
      val resMVExecute: DataLoader = dataLoader.executeMovingFiles()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        System.exit(dataLoader.exitStatus)
    }

    // Spark execute
    try {
      val resSparkExecute: DataLoader = dataLoader.executeSparkJob()
      println("Spark Job '" + config.require[String]("spark.sparkJob") + "' result: ")
      resSparkExecute.execSparkJob.importJob.exitCode().map {
        case 0 => "Import done, exit code 0."
        case exitCode => s"Error, process ended with exit code $exitCode."
      }
    } catch {
      case e: java.lang.IllegalStateException =>
        println("    It need to set before run:\n" +
          "    export HADOOP_HOME=/opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib/hadoop\n" +
          "    export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native\n" +
          "    export HADOOP_OPTS=\"-Djava.library.path=$HADOOP_HOME/lib\"\n" +
          "    export SPARK_HOME=/opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib/spark\n" +
          "    export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera")
      case e: Exception => e.printStackTrace()
    }
  }
}