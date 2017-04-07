package ru.lamoda.etl.hive

import ru.lamoda.etl.metadata.MappingMeta

/**
  * Created by gevorg.hachaturyan on 06/04/2017.
  */
class HiveListOfColumns() {

  private var listOfBK: String = _
  private var listOfColumns: String = _
  private var additionalCases: String = _
  private var listOfColumnsOver: String = _

  def setListOfBK(mapMeta: MappingMeta): Unit = {
    listOfBK = genListOfBK(mapMeta)
  }

  def getListOfBK: String = {
    listOfBK
  }

  def setListOfColumns(mapMeta: MappingMeta): Unit = {
    listOfColumns = genListOfColumns(mapMeta)
  }

  def getListOfColumns: String = {
    listOfColumns
  }

  def setAdditionalCases(mapMeta: MappingMeta): Unit = {
    additionalCases = genAdditionalCases(mapMeta)
  }

  def getAdditionalCases: String = {
    additionalCases
  }

  def setListOfColumnsOver(mapMeta: MappingMeta, listOfBK: String, fieldName: String): Unit = {
    listOfColumnsOver = genListOfColumnsOver(mapMeta, listOfBK, fieldName)
  }

  def getListOfColumnsOver: String = {
    listOfColumnsOver
  }

  private def genListOfBK(mapMeta: MappingMeta): String = {

    var listOfBK: String = ""
    mapMeta.columnList.filter(row => {
      row.as[Boolean]("is_bk").equals(true)
    }).foreach(row => {
      listOfBK += row.as[String]("column_name").toString + ", "
    })

    listOfBK
  }

  private def genListOfColumns(mapMeta: MappingMeta): String ={

    var listOfColumns: String = ""
    mapMeta.columnList.filter(row => {
      row.as[Boolean]("is_exists_in_source").equals(true)
    }).filter(row => {
      row.as[Boolean]("is_bk").equals(false)
    }).foreach(row => {
      listOfColumns += row.as[String]("column_name").toString + ", \n"
    })

    listOfColumns
  }

  private def genAdditionalCases(mapMeta: MappingMeta): String = {

    var additionalCases: String = ""
    mapMeta.whereCase.foreach(row => {
      additionalCases += " AND " + row.as[String]("whereCaseField").toString + " " +
        row.as[String]("whereCaseType").toString + " " +
        row.as[String]("whereCaseVal")
    })

    additionalCases
  }

  private def genListOfColumnsOver(mapMeta: MappingMeta, listOfBK: String, fieldName: String): String = {

    var listOfColumns: String = ""
    mapMeta.columnList.filter(row => {
      row.as[Boolean]("is_exists_in_source").equals(true)
    }).filter(row => {
      !row.as[String]("column_name").equals(fieldName)
    }).filter(row => {
      row.as[Boolean]("is_bk").equals(false)
    }).foreach(row => {
      listOfColumns +=
        " max(" + row.as[String]("column_name").toString +
          ") over (partition by " +
          listOfBK.dropRight(1) +
          " order by " +
          fieldName +
          " desc) as " +
          row.as[String]("column_name").toString + ", \n"
    })

    listOfColumns +=  " max(" + fieldName +
      ") over (partition by " +
      listOfBK.dropRight(1) +
      " order by " +
      fieldName +
      " desc) as max_" +
      fieldName + ", \n"

    listOfColumns
  }

  def genStringToDateTimeForHive(value: String, pattern: String): String = {

    s"to_utc_timestamp(from_unixtime(unix_timestamp('${value}', '${pattern}')),'MSK')"

  }

}
