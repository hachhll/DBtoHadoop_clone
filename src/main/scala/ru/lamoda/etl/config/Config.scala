package ru.lamoda.etl.config

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by gevorg.hachaturyan on 23/01/2017.
  */
class Config(args: Array[String]) {

  // If we need to read from hdfs

//  val hdfs = getHadoopConfig()
//  val in = hdfs.open(new Path(getCommandLineParam(args, "=", "defaultFileProp")));
//
//  val reader = new InputStreamReader(in)
//  val parsedConfig = try {
//    ConfigFactory.parseReader(reader)
//  } finally {
//    reader.close()
//  }

  val appArgs: Array[String] = args

  val parsedConfig = ConfigFactory.parseFile(new File(getCommandLineParam(args, "=", "defaultFileProp")))
  val props = ConfigFactory.load(parsedConfig)

  val hdpConfDir: String = props.getString("hdpConfDir")
  val yarnConfDir: String = props.getString("yarnConfDir")
  val localDefaultField: String = props.getString("localDefaultField")
  val hdfsHiveDefaultField: String = props.getString("hdfsHiveDefaultField")
  val sparkHome: String = props.getString("sparkHome")

  val sparkJob: String = getCommandLineParam(args, "=", "sparkJob")
  val sparkJobMain: String = getCommandLineParam(args, "=", "sparkJobMain")
  val sparkConfFile: String = getCommandLineParam(args, "=", "sparkConfFile")

  val tableName: String = getCommandLineParam(args, "=", "tableName")
  val filedList: String = getCommandLineParam(args, "=", "filedList")
  val fieldDelim: String = getCommandLineParam(args, "=", "fieldDelim")

  val inc_id: String = getCommandLineParam(args, "=", "inc_id")


  def getMapValuesByDelim(argsArray: Array[String], delim: String): Map[String, String] = {
    argsArray.map {
      _.split(delim)
    }.map { case Array(f1, f2) => (f1, f2) }.toMap
  }

  def getCommandLineParam(argsArray: Array[String], delim: String, paramName: String): String = {
    val ms = getMapValuesByDelim(argsArray, delim)
    val resValue = try {
      ms(paramName)
    } catch {
      case e: java.util.NoSuchElementException => println("key not found: " + paramName)
        null
    }
    resValue
  }

  def getHDFSConn: FileSystem = {
    val hadoopConf = new Configuration()
    hadoopConf.addResource(new Path("/core-site.xml"))
    hadoopConf.addResource(new Path("/hdfs-site.xml"))
    hadoopConf.addResource(new Path("/ssl-client.xml"))
    val hdfs = FileSystem.get(hadoopConf)
    hdfs
  }

}



