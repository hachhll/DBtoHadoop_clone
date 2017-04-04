package ru.lamoda.etl.spark

/**
  * Created by gevorg.hachaturyan on 21/02/2017.
  */
class SparkExecProperties {

  private var driverMemory: String = "512M"
  private var akkaFrameSize: String = "20"
  private var executorMemory: String = "1G"
  private var executorInstances: String = "5"
  private var executorCores: String = "5"
  private var defaultParallelism: String = "10"
  private var sparkJob: String = "spark_tmptoparquet-1.0-SNAPSHOT.jar"
  private var sparkJobMain: String = "ru.lamoda.etl.Spark_TmpToParquet"
  private var sparkJobFolder: String = "/home/gevorg.hachaturyan/spark-job/spark_tmptoparquet/target"
  private var sparkArgs: Array[String] = _

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

  def getSparkArgs: Array[String] = {
    sparkArgs
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

  def setSparkArgs(value: Array[String]) {
    sparkArgs = value
  }

}
