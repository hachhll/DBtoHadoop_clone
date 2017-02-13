package ru.lamoda.etl.hadoop

import knobs.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import ru.lamoda.etl.metadata.MappingMeta

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
  def executeMovingFiles: DataLoader = {
    try {
      val mvFiles = new MovingFiles(configParams, mapMeta, FileSystem.get(hadoopConf))
      mvFiles.copyLocalToHDFS // In Prod need to Use moveLocalToHDFS
    } catch {
      case ex: Exception =>
        exitStatus = 1001
        exitMessage = ex.getMessage
    }
    this
  }

  def executeSparkJob: DataLoader = {
    execSparkJob = new SparkExecute(configParams, mapMeta)
    this
  }
}
