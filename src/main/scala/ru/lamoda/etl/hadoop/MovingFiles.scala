package ru.lamoda.etl.hadoop

import java.io.File

import knobs.Config
import org.apache.hadoop.fs.{FileSystem, Path}
import ru.lamoda.etl.metadata.MappingMeta

/**
  * Created by gevorg.hachaturyan on 18/01/2017.
  */
class MovingFiles(configParams: Config, mapMeta: MappingMeta, hdfscon: FileSystem) {

  val srcLocalFolder: String = configParams.require[String]("common.root_dir") + "/" + mapMeta.groupName + "/" + mapMeta.tableName
  val destHDFSFolder: String = configParams.require[String]("hadoop.hdfsHiveDefaultField") + "/" + mapMeta.groupName + "/" + mapMeta.tableName + "_" + mapMeta.inc_id
  var exitStatus: Int = _
  var exitMessage: String = _

  def fromLocalToHDFS(delFiles: Boolean, srcLocalFolder: String, tableName: String, hdfs: org.apache.hadoop.fs.FileSystem): Unit = {

    val destPath = new Path(tableName.toString).toUri.toString

    try {
      if (!hdfs.exists(new Path(destPath))) {
        println(s"Create folder: $destPath")
        hdfs.mkdirs(new Path(destPath))
      }
      // Create dir if not exist
      val filesList = new File(srcLocalFolder).listFiles().filter(_.isFile).toList // Get list of local files
      for (nameFile <- filesList) {
        hdfs.copyFromLocalFile(delFiles, new Path(nameFile.toURI.toString), new Path(destPath)) // Move files to hdfs
      }

    }
    catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw ex
    }
  }

  def copyLocalToHDFS(): Unit = {
    try {
      val delFiles = false
      fromLocalToHDFS(delFiles,
        srcLocalFolder,
        destHDFSFolder,
        hdfscon)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw ex
    }
  }

  def moveLocalToHDFS(): Unit = {
    try {
      val delFiles = true
      fromLocalToHDFS(delFiles,
        srcLocalFolder,
        destHDFSFolder,
        hdfscon)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw ex
    }
  }
}
