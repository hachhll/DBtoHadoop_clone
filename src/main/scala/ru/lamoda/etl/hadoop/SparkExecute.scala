package ru.lamoda.etl.hadoop

import ru.lamoda.etl.config.Config

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.spark.launcher.SparkLauncher

/**
  * Created by gevorg.hachaturyan on 27/01/2017.
  */
class SparkExecute(configParams: Config) {

  val env = Map(
    "HADOOP_CONF_DIR" -> configParams.hdpConfDir,
    "YARN_CONF_DIR" -> configParams.yarnConfDir
  )

  private val handler = new SparkLauncher(env.asJava)
    .setPropertiesFile(configParams.sparkConfFile)
    .setAppResource(configParams.sparkJob) // Lint to spark job with jar
    .setMainClass(configParams.sparkJobMain) // spark job main class
    .setSparkHome(configParams.sparkHome) // spark home folder on cluster node
    .setAppName(configParams.sparkJobMain)
    //.setConf("spark.app.id", configParams.sparkJobMain)
    .setVerbose(true)
    .addAppArgs(configParams.appArgs: _*)
    .launch()

  private val importJob = new SparkImportJob(handler)

  importJob.stderrIterator.foreach { line =>
    println(line)
  }

  importJob.stdoutIterator.foreach { line =>
    println(line)
  }

  importJob.exitCode.map {
    case 0 => "Import done, exit code 0."
    case exitCode => "Error, process ended with exit code $exitCode."
  }

}
