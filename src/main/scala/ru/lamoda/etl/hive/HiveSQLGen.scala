package ru.lamoda.etl.hive

import ru.lamoda.etl.metadata.MappingMeta

/**
  * Created by gevorg.hachaturyan on 03/04/2017.
  */
class HiveSQLGen(mapMeta: MappingMeta) {

  var hiveSQLValidate: String = _
  var hiveSQL: String = _

  @throws(classOf[Exception])
  def setHiveSQLOnDate(byValue: String, fieldName: String): HiveSQLGen = {

    var listOfColumns: String = ""
    mapMeta.columnList.filter(row => {
      row.as[Boolean]("is_exists_in_source").equals(true)
    }).foreach(row => {
      listOfColumns += row.as[String]("column_name").toString + ","
    })

    val whereCase: String = s" AND ${fieldName} <= " + setStringToDateTimeForHive(byValue, "yyyyMMdd")

    var additionalCases: String = ""
    mapMeta.whereCase.foreach(row => {
      additionalCases += " AND " + row.as[String]("whereCaseField").toString + " " +
        row.as[String]("whereCaseType").toString + " " +
        row.as[String]("whereCaseVal").toString
    })

    setHiveSQL(listOfColumns.dropRight(1), mapMeta.tableName, whereCase, additionalCases)
    this
  }

  @throws(classOf[Exception])
  def setHiveSQLPeriodDate(fromValue: String, toValue: String, fieldName: String): HiveSQLGen = {

    var listOfColumns: String = ""
    mapMeta.columnList.filter(row => {
      row.as[Boolean]("is_exists_in_source").equals(true)
    }).foreach(row => {
      listOfColumns += row.as[String]("column_name").toString + ","
    })

    val whereCase: String = s" AND ${fieldName} BETWEEN " + setStringToDateTimeForHive(fromValue, "yyyyMMdd") + " AND " + setStringToDateTimeForHive(toValue, "yyyyMMdd")

    var additionalCases: String = ""
    mapMeta.whereCase.foreach(row => {
      additionalCases += " AND " + row.as[String]("whereCaseField").toString + " " +
        row.as[String]("whereCaseType").toString + " " +
        row.as[String]("whereCaseVal").toString
    })

    setHiveSQL(listOfColumns.dropRight(1), mapMeta.tableName, whereCase, additionalCases)
    this
  }

  @throws(classOf[Exception])
  def setHiveSQLPeriodNumber(fromValue: String, toValue: String, fieldName: String): HiveSQLGen = {

    var listOfColumns: String = ""
    mapMeta.columnList.filter(row => {
      row.as[Boolean]("is_exists_in_source").equals(true)
    }).foreach(row => {
      listOfColumns += row.as[String]("column_name").toString + ","
    })

    val whereCase: String = s" AND ${fieldName} BETWEEN " + fromValue + " AND " + toValue

    var additionalCases: String = ""
    mapMeta.whereCase.foreach(row => {
      additionalCases += " AND " + row.as[String]("whereCaseField").toString + " " +
        row.as[String]("whereCaseType").toString + " " +
        row.as[String]("whereCaseVal").toString
    })

    setHiveSQL(listOfColumns.dropRight(1), mapMeta.tableName, whereCase, additionalCases)
    this
  }

  private def setHiveSQL(listOfColumns: String, tableName: String, whereCase: String, additionalCases: String) {

    hiveSQLValidate = " SELECT " + listOfColumns + " FROM " + tableName + " WHERE 0=1" + whereCase + additionalCases
    hiveSQL = " SELECT " + listOfColumns + " FROM " + tableName + " WHERE 1=1" + whereCase + additionalCases
  }

  private def setStringToDateTimeForHive(value: String, pattern: String): String = {

    s"to_utc_timestamp(from_unixtime(unix_timestamp('${value}', '${pattern}')),'MSK')"

  }
}
