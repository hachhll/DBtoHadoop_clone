package ru.lamoda.etl

import ru.lamoda.etl.metadata.MappingMeta
import knobs.{Config, FileResource, Required}

import scalaz.concurrent.Task
import java.io.File

import com.github.martincooper.datatable.DataView
import org.rogach.scallop._
import ru.lamoda.etl.pentaho.PentahoTableToFile

import scala.collection.immutable.Map

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
    val conf = new Conf(args)  // Note: This line also works for "object Main extends App"

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

  }
}