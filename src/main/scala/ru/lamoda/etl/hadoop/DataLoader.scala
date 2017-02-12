package ru.lamoda.etl.hadoop

import knobs.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import ru.lamoda.etl.metadata.MappingMeta

/**
  * Created by gevorg.hachaturyan on 27/01/2017.
  */
class DataLoader(configParams: Config, mapMeta: MappingMeta) {

  def getHDFSConn: FileSystem = {
    val hadoopConf = new Configuration()
    hadoopConf.addResource(new Path(configParams.require[String]("hadoop.hdpConfDir") + "/core-site.xml"))
    hadoopConf.addResource(new Path(configParams.require[String]("hadoop.hdpConfDir") + "/hdfs-site.xml"))
    hadoopConf.addResource(new Path(configParams.require[String]("hadoop.hdpConfDir") + "/ssl-client.xml"))
    val hdfs = FileSystem.get(hadoopConf)
    hdfs
  }

  // Moving files from local to HDFS
  val mvFiles = new MovingFiles(configParams, mapMeta, getHDFSConn)
  mvFiles.copyLocalToHDFS // In Prod need to Use moveLocalToHDFS

  new SparkExecute(configParams, mapMeta)

}
