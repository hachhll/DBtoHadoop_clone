package ru.lamoda.etl.spark

import knobs.Config
import org.apache.spark.launcher.SparkLauncher
import ru.lamoda.etl.metadata.MappingMeta

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by gevorg.hachaturyan on 27/01/2017.
  */
class SparkExecute(configParams: Config, mapMeta: MappingMeta) {

  var importJob: SparkImportJob = _

  private val env = Map(
    "HADOOP_CONF_DIR" -> configParams.require[String]("hadoop.hdpConfDir"),
    "YARN_CONF_DIR" -> configParams.require[String]("hadoop.hdpConfDir")
  )

  private val handler = new SparkLauncher(env.asJava)

  private val sparkParams = Map(
    "sparkJavaHome" -> configParams.require[String]("spark.sparkJavaHome"),
    "sparkHome" -> configParams.require[String]("spark.sparkHome"),
    "sparkMaster" -> configParams.require[String]("spark.sparkMaster"),
    "sparkDriverAllowMultipleContexts" -> configParams.require[Boolean]("spark.sparkDriverAllowMultipleContexts"),
    "sparkEventLogEnabled" -> configParams.require[Boolean]("spark.sparkEventLogEnabled")
  )

  def prepareSPK(spkProperties: SparkExecProperties): Unit = {

    handler
      // From config file
      //.setPropertiesFile(configParams.require[String]("spark.sparkJobConfFile"))
      .setConf("spark.java.home", sparkParams("sparkJavaHome").toString)
      .setConf("spark.home", sparkParams("sparkHome").toString)
      .setConf("spark.master", sparkParams("sparkMaster").toString)
      .setConf("spark.driver.allowMultipleContexts", sparkParams("sparkDriverAllowMultipleContexts").toString)
      .setConf("spark.eventLog.enabled", sparkParams("sparkEventLogEnabled").toString)
      .setVerbose(true)
      // Dynamic params
      .setAppResource(spkProperties.getSparkJobFolder + "/" + spkProperties.getSparkJob) // Lint to spark job with jar
      .setMainClass(spkProperties.getSparkJobMain) // spark job main class
      .setAppName(spkProperties.getSparkJobMain)
      .setConf("spark.driver.memory", spkProperties.getDriverMemory)
      .setConf("spark.akka.frameSize", spkProperties.getAkkaFrameSize)
      .setConf("spark.default.parallelism", spkProperties.getDefaultParallelism)
      .setConf("spark.executor.cores", spkProperties.getExecutorCores)
      .setConf("spark.executor.instances", spkProperties.getExecutorInstances)
      .setConf("spark.executor.memory", spkProperties.getExecutorMemory)
      .addAppArgs(spkProperties.getSparkArgs: _*)
  }

  def executeSPK(): Unit = {

    importJob = new SparkImportJob(handler.launch())

    importJob.stderrIterator().foreach {
      line => println(line)
    }

    importJob.stdoutIterator().foreach {
      line => println(line)
    }
  }

}