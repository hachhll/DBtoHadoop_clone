package ru.lamoda.etl.hadoop

import knobs.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import ru.lamoda.etl.metadata.MappingMeta
import ru.lamoda.etl.spark.{SparkExecProperties, SparkExecute}

/**
  * Created by gevorg.hachaturyan on 27/01/2017.
  */
class DataLoader(configParams: Config, mapMeta: MappingMeta) {

  val hadoopConf = new Configuration()
  hadoopConf.addResource(new Path(configParams.require[String]("hadoop.hdpConfDir") + "/core-site.xml"))
  hadoopConf.addResource(new Path(configParams.require[String]("hadoop.hdpConfDir") + "/hdfs-site.xml"))
  hadoopConf.addResource(new Path(configParams.require[String]("hadoop.hdpConfDir") + "/ssl-client.xml"))
  var exitStatus: Int = _
  var exitMessage: String = _
  var execSparkJob: SparkExecute = _

  // Moving files from local to HDFS
  @throws(classOf[Exception])
  def executeMovingFiles(): DataLoader = {
    val fs: FileSystem = FileSystem.get(hadoopConf)
    val mvFiles = new MovingFiles(configParams, mapMeta, fs)
    mvFiles.copyLocalToHDFS() // In Prod need to Use moveLocalToHDFS
    this
  }

  @throws(classOf[Exception])
  def executeSparkJob(): DataLoader = {
    execSparkJob = new SparkExecute(configParams, mapMeta)
    val dynSparkJobParam = new SparkExecProperties

    dynSparkJobParam.setSparkJob("spark_tmptoparquet-1.0-SNAPSHOT.jar")
    dynSparkJobParam.setSparkJobMain("ru.lamoda.etl.Spark_TmpToParquet")
    dynSparkJobParam.setSparkJobFolder(configParams.require[String]("spark.sparkJobFolder") + "/spark_tmptoparquet/target")

    var sparkArgs: Array[String] = Array("")

    sparkArgs = Array("tableName=" + mapMeta.tableName)
    sparkArgs :+= "inc_id=" + mapMeta.inc_id
    sparkArgs :+= "fieldDelim=" + configParams.require[String]("spark.fieldDelim")


    var listOfColumns: String = ""
    sparkArgs :+= "filedList=" + mapMeta.columnList.filter(row => {
      row.as[Boolean]("is_exists_in_source").equals(true)
    }).foreach(row => {
      listOfColumns += row.as[String]("column_name").toString + ","
    }).toString.dropRight(1)

    dynSparkJobParam.setSparkArgs(sparkArgs)

    execSparkJob.prepareSPK(dynSparkJobParam)
    execSparkJob.executeSPK()
    this
  }
}
