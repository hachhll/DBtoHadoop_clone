package ru.lamoda.etl.pentaho

import org.pentaho.di.core.KettleEnvironment
import org.pentaho.di.core.util.EnvUtil
import org.pentaho.di.trans.{Trans, TransMeta}
import org.pentaho.di.shared.SharedObjects
import org.pentaho.di.trans.steps.tableinput.TableInputMeta
import org.pentaho.di.trans.steps.selectvalues.{SelectMetadataChange, SelectValuesMeta}
import org.pentaho.di.trans.steps.textfileoutput.{TextFileField, TextFileOutputMeta}
import knobs.Config
import ru.lamoda.etl.metadata._

import scala.util.Try
import scalax.file.Path

/**
  * Created by roman.gaydayenko on 27.01.17.
  */
class PentahoTableToFile(val config: Config, val mapMeta: MappingMeta) {
  var trans:Trans = new Trans()
  KettleEnvironment.init()
  EnvUtil.environmentInit()
  var tm = new TransMeta(config.require[String]("common.root_dir")  + "/config/pentaho/" + config.require[String]("pentaho.ktr_file_name"))

  def setConnection: Unit ={
    //set required connection from shared.xml file
    //program looks for <connection_name>.xml in config/pentaho folder
    //<connection_name> is case sensitive
    val so = new SharedObjects(config.require[String]("common.root_dir")  + "/config/pentaho/" + mapMeta.srcConName + ".xml") // loads shared.xml
    tm.findStep("input_table").getStepMetaInterface().asInstanceOf[TableInputMeta]
      .setDatabaseMeta(so.getSharedDatabase(mapMeta.srcConName))
  }

  def setSQL: Unit ={
    //set required connection from shared.xml file
    //program looks for <connection_name>.xml in config/pentaho folder
    //<connection_name> is case sensitive
    val so = new SharedObjects(config.require[String]("common.root_dir")  + "/config/pentaho/" + mapMeta.srcConName + ".xml") // loads shared.xml
    tm.findStep("input_table").getStepMetaInterface().asInstanceOf[TableInputMeta]
      .setSQL(mapMeta.getSrcSQL)
  }

  def setParameters: Unit ={
    //Setup input parameters for connection (must be in cycle
    //and standard input

    trans.eraseParameters()
    for(param <- mapMeta.srcConParams){
      trans.addParameterDefinition("p_" + param._1, param._2, "")
    }
    trans.addParameterDefinition("p_inc_id", mapMeta.inc_id.toString, "")
    trans.addParameterDefinition("p_source_system_id", mapMeta.source_system_id.toString, "")
    trans.addParameterDefinition("p_output_file_dir", config.require[String]("pentaho.data_directory") + "/" + mapMeta.groupName + "/" + mapMeta.tableName, "")
  }

  def setColumnsInGeneralOutput: Unit ={
    //add column into general output
    val fileOutputMeta = tm.findStep("Text file output").getStepMetaInterface().asInstanceOf[TextFileOutputMeta]
    var tffArray = Array[TextFileField]()

    val dataView = mapMeta.columnList.filter(row => {
      row.as[Boolean]("is_exists_in_source").equals(true)
    })

    dataView.foreach(row => {
      val tffField = new TextFileField()
      tffField.setName(row.as[String]("column_name"))
      tffField.setType(row.as[String]("column_type"))
      tffField.setFormat(config.require[String]("pentaho.format.fileoutput." + row.as[String]("column_type")))
      tffArray :+= tffField
    })
    fileOutputMeta.setOutputFields(tffArray)
  }

  def setColumnsInGroupByOutput: Unit ={
    //add column into "group by" output
    val selValMeta = tm.findStep("Select for group").getStepMetaInterface().asInstanceOf[SelectValuesMeta]
    var smcArray = Array[SelectMetadataChange]()

    val dataView = mapMeta.columnList.filter(row => {
      row.as[Boolean]("is_inc_val").equals(true)
    })
    dataView.foreach(row => {
      val smcField = new SelectMetadataChange(selValMeta)
      smcField.setName(row.as[String]("column_name"))
      smcField.setType(row.as[String]("column_type"))
      smcField.setConversionMask(config.require[String]("pentaho.format.groupby." + row.as[String]("column_type")))
      smcField.setRename("inc_val")
      smcArray +:= smcField
    })
    val smcField = new SelectMetadataChange(selValMeta)
    smcField.setName("inc_id")
    smcField.setType("Number")
    smcField.setConversionMask("#")
    smcArray +:= smcField

    selValMeta.setMeta(smcArray)
  }

  def prepareKTR: PentahoTableToFile ={
    trans = new Trans(tm)
    this.setConnection
    this.setSQL
    this.setColumnsInGeneralOutput
    this.setParameters
    this.setColumnsInGroupByOutput
    return this
  }

  def executeKTR: PentahoTableToFile ={
    //clear target directory
    val path = Path.fromString(config.require[String]("pentaho.data_directory") + "/" + mapMeta.groupName + "/" + mapMeta.tableName)
    Try(path.deleteRecursively(continueOnFailure = false))
    //execute transformation in synchronous mode
    trans.execute(null)
    trans.waitUntilFinished()
    return this
  }

  def getResult: Array[(String, String)] ={
    Array(("result", trans.getResult().getExitStatus().toString()),
      ("row_count", trans.getVariable("row_count")),
      ("min_val", trans.getVariable("min_val")),
      ("max_val", trans.getVariable("max_val")))
  }
}
