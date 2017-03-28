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
    val rootDir = new File(".").getAbsolutePath

    val generalConfig: Task[Config] = knobs.loadImmutable(Required(FileResource(new File(rootDir + "/config/common.cfg"))) :: Nil)
    val config = generalConfig.unsafePerformSync
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
      val resMVExecute = dataLoader.executeMovingFiles()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        System.exit(dataLoader.exitStatus)
    }

    // Spark execute
    val resSparkExecute = dataLoader.executeSparkJob()
    println("Spark Job '" + config.require[String]("spark.sparkJob") + "' result: ")
    resSparkExecute.execSparkJob.importJob.exitCode().map {
      case 0 => "Import done, exit code 0."
      case exitCode => s"Error, process ended with exit code $exitCode."
    }
  }
}