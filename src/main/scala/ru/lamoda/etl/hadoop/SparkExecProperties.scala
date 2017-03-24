package ru.lamoda.etl.hadoop

/**
  * Created by gevorg.hachaturyan on 21/02/2017.
  */
class SparkExecProperties {

  private var driverMemory = "512M"
  private var akkaFrameSize = "20"
  private var executorMemory = "1G"
  private var executorInstances = "5"
  private var executorCores = "5"
  private var defaultParallelism = "10"
  private var sparkJob = "spark_tmptoparquet-1.0-SNAPSHOT.jar"
  private var sparkJobMain = "ru.lamoda.etl.Spark_TmpToParquet"
  private var sparkJobFolder = "/home/gevorg.hachaturyan/spark-job/spark_tmptoparquet/target"

  def getDriverMemory: String = {
    driverMemory
  }

  def getAkkaFrameSize: String = {
    akkaFrameSize
  }

  def getExecutorMemory: String = {
    executorMemory
  }

  def getExecutorInstances: String = {
    executorInstances
  }

  def getExecutorCores: String = {
    executorCores
  }

  def getDefaultParallelism: String = {
    defaultParallelism
  }

  def getSparkJob: String = {
    sparkJob
  }

  def getSparkJobMain: String = {
    sparkJobMain
  }

  def getSparkJobFolder: String = {
    sparkJobFolder
  }

  def setDriverMemory(value: String) {
    driverMemory = value
  }

  def setAkkaFrameSize(value: String) {
    akkaFrameSize = value
  }

  def setExecutorMemory(value: String) {
    executorMemory = value
  }

  def setExecutorInstances(value: String) {
    executorInstances = value
  }

  def setExecutorCores(value: String) {
    executorCores = value
  }

  def setDefaultParallelism(value: String) {
    defaultParallelism = value
  }

  def setSparkJob(value: String) {
    sparkJob = value
  }

  def setSparkJobMain(value: String) {
    sparkJobMain = value
  }

  def setSparkJobFolder(value: String) {
    sparkJobFolder = value
  }

}
