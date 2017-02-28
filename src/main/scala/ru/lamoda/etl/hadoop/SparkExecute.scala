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

  var importJob: SparkImportJob = _

  private val env = Map(
    "HADOOP_CONF_DIR" -> configParams.require[String]("hadoop.hdpConfDir"),
    "YARN_CONF_DIR" -> configParams.require[String]("hadoop.hdpConfDir")
  )

  private val handler = new SparkLauncher(env.asJava)

  private val sparkParams = scala.collection.mutable.Map[String, String](
    "sparkJobFolder" -> configParams.require[String]("spark.sparkJobFolder"),
    "sparkHome" -> configParams.require[String]("spark.sparkHome"),
    "sparkMaster" -> configParams.require[String]("spark.sparkMaster"),
    "sparkJavaHome" -> configParams.require[String]("spark.sparkJavaHome"),
    "sparkJob" -> configParams.require[String]("spark.sparkJob"),
    "sparkJobMain" -> configParams.require[String]("spark.sparkJobMain")
  )

  private var sparkArgs: Array[String] = _

  sparkArgs = Array("tableName=" + mapMeta.tableName)
  sparkArgs :+= "inc_id=" + mapMeta.inc_id
  sparkArgs :+= "fieldDelim=" + configParams.require[String]("spark.fieldDelim")

  private val dataView = mapMeta.columnList.filter(row => {
    row.as[Boolean]("is_exists_in_source").equals(true)
  })

  var listOfColumns: String = _
  dataView.foreach(row => {
    listOfColumns += row.as[String]("column_name").toString + ","
  })

  sparkArgs :+= "filedList=" + listOfColumns.dropRight(1)


  def prepareSPK {

    handler
      //.setPropertiesFile(configParams.require[String]("spark.sparkJobConfFile"))
      .setAppResource(sparkParams("sparkJobFolder") + "/" + sparkParams("sparkJob")) // Lint to spark job with jar
      .setMainClass(sparkParams("sparkJobMain")) // spark job main class
      .setSparkHome(sparkParams("sparkHome")) // spark-submit home folder on cluster node
      .setAppName(sparkParams("sparkJobMain"))
      .setMaster(sparkParams("sparkMaster"))
      .setJavaHome(sparkParams("sparkJavaHome"))
      //.setConf("spark.app.id", configParams.sparkJobMain)
      .setVerbose(true)
      .addAppArgs(sparkArgs: _*)
  }

  def executeSPK {

    importJob = new SparkImportJob(handler.launch())

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


}