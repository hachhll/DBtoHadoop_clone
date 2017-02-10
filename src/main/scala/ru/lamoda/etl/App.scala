package ru.lamoda.etl

import ru.lamoda.etl.metadata.MappingMeta
import knobs.{Config, FileResource, Required}

import scalaz.concurrent.Task
import java.io.File

import com.github.martincooper.datatable.DataView
import org.rogach.scallop._
import ru.lamoda.etl.hadoop.DataLoader
import ru.lamoda.etl.pentaho.PentahoTableToFile

import scala.collection.immutable.Map

/**
  * Hello world!
  *
  */

object App {
  def main(args: Array[String]): Unit = {

    //Must be in unput parameters
    val rootDir = new File(".").getAbsolutePath()

    val generalConfig: Task[Config] = knobs.loadImmutable(Required(FileResource(new File(rootDir + "/config/common.cfg"))) :: Nil)
    val config = generalConfig.unsafePerformSync
    val cnf:Config = new Config(Map("common.root_dir" -> knobs.CfgText(rootDir)))

    var mapMeta = new MappingMeta()
    mapMeta.loadMetaTable()

    val pentahoTTF = new PentahoTableToFile(config++cnf, mapMeta)
    pentahoTTF.prepareKTR
    pentahoTTF.executeKTR
    println("Execution result: ")
    for(row <- pentahoTTF.getResult){
      println(row._1 + " ==> " + row._2)
    }

    val configParams = new ru.lamoda.etl.config.Config(args)
    new DataLoader(configParams)
  }
}