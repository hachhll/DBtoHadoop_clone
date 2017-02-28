package ru.lamoda.etl.hadoop

/**
  * Created by gevorg.hachaturyan on 21/02/2017.
  */
class SparkExecProperties {

  private val driverMemory = "512M"
  private val akkaFrameSize = "20"
  private val executorMemory = "1G"
  private val executorInstances = "5"
  private val executorCores = "5"
  private val defaultParallelism = "10"

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

}
