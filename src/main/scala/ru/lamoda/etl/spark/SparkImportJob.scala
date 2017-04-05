package ru.lamoda.etl.spark

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

/**
  * Created by gevorg.hachaturyan on 24/01/2017.
  */
class SparkImportJob(process: Process)(implicit executionContext: ExecutionContext) {

  def exitCode(): Future[Int] = Future {
    process.waitFor()
  }

  def stderrIterator(): Iterator[String] = {
    Source.fromInputStream(process.getErrorStream).getLines()
  }

  def stdoutIterator(): Iterator[String] = {
    Source.fromInputStream(process.getInputStream).getLines()
  }
}
