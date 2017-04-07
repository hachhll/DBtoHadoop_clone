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

    val hiveListOfColumns = new HiveListOfColumns()
    hiveListOfColumns.setListOfBK(mapMeta)
    hiveListOfColumns.setListOfColumns(mapMeta)
    hiveListOfColumns.setAdditionalCases(mapMeta)
    hiveListOfColumns.setListOfColumnsOver(mapMeta, hiveListOfColumns.getListOfBK, fieldName)

    val whereCase: String = s" AND ${fieldName} <= " + hiveListOfColumns.genStringToDateTimeForHive(byValue, "yyyyMMdd")

    setHiveSQL(hiveListOfColumns.getListOfColumns, hiveListOfColumns.getListOfColumnsOver, mapMeta.tableName, whereCase, hiveListOfColumns.getAdditionalCases, hiveListOfColumns.getListOfBK, fieldName)
    this
  }

  @throws(classOf[Exception])
  def setHiveSQLPeriodDate(fromValue: String, toValue: String, fieldName: String): HiveSQLGen = {

    val hiveListOfColumns = new HiveListOfColumns()
    hiveListOfColumns.setListOfBK(mapMeta)
    hiveListOfColumns.setListOfColumns(mapMeta)
    hiveListOfColumns.setAdditionalCases(mapMeta)
    hiveListOfColumns.setListOfColumnsOver(mapMeta, hiveListOfColumns.getListOfBK, fieldName)

    val whereCase: String = s" AND ${fieldName} BETWEEN " + hiveListOfColumns.genStringToDateTimeForHive(fromValue, "yyyyMMdd") + " AND " + hiveListOfColumns.genStringToDateTimeForHive(toValue, "yyyyMMdd")

    setHiveSQL(hiveListOfColumns.getListOfColumns, hiveListOfColumns.getListOfColumnsOver, mapMeta.tableName, whereCase, hiveListOfColumns.getAdditionalCases, hiveListOfColumns.getListOfBK, fieldName)
    this
  }

  @throws(classOf[Exception])
  def setHiveSQLPeriodNumber(fromValue: Integer, toValue: Integer, fieldName: String): HiveSQLGen = {

    val hiveListOfColumns = new HiveListOfColumns()
    hiveListOfColumns.setListOfBK(mapMeta)
    hiveListOfColumns.setListOfColumns(mapMeta)
    hiveListOfColumns.setAdditionalCases(mapMeta)
    hiveListOfColumns.setListOfColumnsOver(mapMeta, hiveListOfColumns.getListOfBK, fieldName)

    val whereCase: String = s" AND ${fieldName} BETWEEN " + fromValue + " AND " + toValue

    setHiveSQL(hiveListOfColumns.getListOfColumns, hiveListOfColumns.getListOfColumnsOver, mapMeta.tableName, whereCase, hiveListOfColumns.getAdditionalCases, hiveListOfColumns.getListOfBK, fieldName)
    this
  }

  private def setHiveSQL(listOfColumns: String, ListOfColumnsOver: String, tableName: String, whereCase: String, additionalCases: String, listOfBK: String, fieldName: String) {

//    select
//    key1, key2, val1, val2, val3, valdate
//    from
//    (
//      select
//        key1,
//      key2,
//      z.valdate,
//      max(val1) over (partition by key1, key2 order by valdate desc) as val1,
//      max(val2) over (partition by key1, key2 order by valdate desc) as val2,
//      max(val3) over (partition by key1, key2 order by valdate desc) as val3,
//      max(valdate) over (partition by key1, key2 order by valdate desc) as valdate_new
//        from test_table z
//    ) k
//      where k.valdate_new = k.valdate


    hiveSQLValidate = " SELECT \n" +
      listOfBK +
      listOfColumns +
      fieldName +
      " \nFROM ( SELECT \n" +
      listOfBK +
      ListOfColumnsOver +
      fieldName +
      " FROM " +
      tableName +
      " WHERE 0=1" +
      whereCase +
      additionalCases +
      ") res \n WHERE res." + fieldName + " = res.max_" + fieldName

    hiveSQL = " SELECT \n" +
      listOfBK +
      listOfColumns +
      fieldName +
      " \nFROM ( SELECT \n" +
      listOfBK +
      ListOfColumnsOver +
      fieldName +
      " FROM " +
      tableName +
      " WHERE 1=1" +
      whereCase +
      additionalCases +
      ") res \n WHERE res." + fieldName + " = res.max_" + fieldName
  }

}
