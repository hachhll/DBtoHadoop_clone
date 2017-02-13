package ru.lamoda.etl

import java.io.File

import knobs.{Config, FileResource, Required}
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

object App {
  def main(args: Array[String]): Unit = {

    //Must be in unput parameters
    val rootDir = new File(".").getAbsolutePath

    val generalConfig: Task[Config] = knobs.loadImmutable(Required(FileResource(new File(rootDir + "/config/common.cfg"))) :: Nil)
    val config = generalConfig.unsafePerformSync
    val cnf: Config = new Config(Map("common.root_dir" -> knobs.CfgText(rootDir)))

    val mapMeta = new MappingMeta()
    mapMeta.loadMetaTable

    val pentahoTTF = new PentahoTableToFile(config ++ cnf, mapMeta)
    pentahoTTF.prepareKTR
    pentahoTTF.executeKTR
    println("Execution result: ")
    for (row <- pentahoTTF.getResult) {
      println(row._1 + " ==> " + row._2)
    }

    val dataLoader = new DataLoader(config ++ cnf, mapMeta)

    val resMVExecute = dataLoader.executeMovingFiles
    println("Moving files result: ")
    if (resMVExecute.exitStatus != 0) println("Moving files was done.")
    else {
      println("Moving files was faild.")
      println(resMVExecute.exitMessage)
    }

    val resSparkExecute = dataLoader.executeSparkJob
    println("Spark Job '"+mapMeta.sparkJob+"' result: ")
    resSparkExecute.execSparkJob.importJob.exitCode.map {
      case 0 => "Import done, exit code 0."
      case exitCode => "Error, process ended with exit code $exitCode."
    }
  }
}