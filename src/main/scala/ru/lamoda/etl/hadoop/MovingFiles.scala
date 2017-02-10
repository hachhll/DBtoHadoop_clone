package ru.lamoda.etl.hadoop

import java.io.File

import org.apache.hadoop.fs.{FileSystem, Path}
import ru.lamoda.etl.config.Config

/**
  * Created by gevorg.hachaturyan on 18/01/2017.
  */
class MovingFiles {

  def fromLocalToHDFS(delFiles: Boolean, srcLocalFolder: String, tableName: String, hdfs: FileSystem): Unit = {

    val destPath = new Path(tableName.toString).toUri.toString

    if (!hdfs.exists(new Path(destPath))) hdfs.mkdirs(new Path(destPath))
    // Create dir if not exist
    val filesList = new File(srcLocalFolder).listFiles().filter(_.isFile).toList // Get list of local files
    for (nameFile <- filesList) {
      hdfs.copyFromLocalFile(delFiles, new Path(nameFile.toURI.toString), new Path(destPath)) // Move files to hdfs
    }
  }

  def copyLocalToHDFS(configParams: Config): Unit = {

    val delFiles = false
    fromLocalToHDFS(delFiles,
      configParams.localDefaultField + "/" + configParams.tableName,
      configParams.hdfsHiveDefaultField + "/" + configParams.tableName + configParams.inc_id,
      configParams.getHDFSConn)
  }

  def moveLocalToHDFS(configParams: Config): Unit = {
    val delFiles = true
    fromLocalToHDFS(delFiles,
      configParams.localDefaultField + "/" + configParams.tableName,
      configParams.hdfsHiveDefaultField + "/" + configParams.tableName + configParams.inc_id,
      configParams.getHDFSConn)
  }
}
