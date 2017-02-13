package ru.lamoda.etl.hadoop

import knobs.Config
import org.apache.spark.launcher.SparkLauncher
import ru.lamoda.etl.metadata.MappingMeta

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by gevorg.hachaturyan on 27/01/2017.
  */
class SparkExecute(configParams: Config, mapMeta: MappingMeta) {

  val env = Map(
    "HADOOP_CONF_DIR" -> configParams.require[String]("hadoop.hdpConfDir"),
    "YARN_CONF_DIR" -> configParams.require[String]("hadoop.hdpConfDir")
  )

  private val handler = new SparkLauncher(env.asJava)
    .setPropertiesFile(configParams.require[String]("spark.sparkConfFile"))
    .setAppResource(configParams.require[String]("sparkJobFolder") + "/" + mapMeta.sparkJob) // Lint to spark job with jar
    .setMainClass(mapMeta.sparkJobMain) // spark job main class
    .setSparkHome(configParams.require[String]("spark.sparkHome")) // spark-submit home folder on cluster node
    .setAppName(mapMeta.sparkJobMain)
    //.setConf("spark.app.id", configParams.sparkJobMain)
    .setVerbose(true)
    .addAppArgs(mapMeta.sparkArgs: _*)
    .launch()

  val importJob = new SparkImportJob(handler)

  importJob.stderrIterator.foreach {
    line => println(line)
  }

  importJob.stdoutIterator.foreach {
    line => println(line)
  }

//    importJob.exitCode.map {
//      case 0 => "Import done, exit code 0."
//      case exitCode => "Error, process ended with exit code $exitCode."
//    }

}
