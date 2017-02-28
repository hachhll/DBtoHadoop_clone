package ru.lamoda.etl.metadata

import com.github.martincooper.datatable.{DataColumn, DataTable, DataValue}

import scala.collection.mutable

/**
  * Created by roman.gaydayenko on 01.02.17.
  */

class MappingMeta {
  val colColumnName = new DataColumn[String]("column_name", Iterable[String]())
  val colIsExistsInSource = new DataColumn[Boolean]("is_exists_in_source", Iterable[Boolean]())
  val colColumnType = new DataColumn[String]("column_type", Iterable[String]())
  val colIsIncVal = new DataColumn[Boolean]("is_inc_val", Iterable[Boolean]())
  var columnList: DataTable = DataTable("default", Seq(colColumnName, colIsExistsInSource, colColumnType, colIsIncVal)).get

  val srcConParams = mutable.Map.empty[String, String]
  var srcConName: String = ""
  var inc_id: Integer = -1
  var source_system_id: Integer = -1
  var output_file_name: String = ""
  var groupName: String = ""
  var tableName: String = ""

  def getSrcSQL: String = {
    """SELECT
    id_sales_order_item
    , fk_sales_order
    , fk_payment_status
    , fk_sales_order_item_shipment
    , item_nr
    , base_cost
    , base_price
    , unit_price
    , tax_amount
    , codfee_amount
    , paid_price
    , coupon_money_value
    , coupon_percent
    , coupon_refundable
    , coupon_category
    , name
    , sku
    , barcode
    , description
    , weight
    , color
    , created_at
    , updated_at
    , last_payment_status_change
    , amount_paid
    , refunded_money
    , refunded_voucher
    , tax_percent
    , product_size
    , fk_delivery_status
    , delivery_status_last_change
    , fk_status_list_main
    , status_main_last_change
    , fk_stock_warehouse
    , fk_refund
    , is_gift
    , fk_refund_status
    , refund_status_last_change
    , loyalty_money_value
    , ${p_inc_id} as inc_id
    , ${p_source_system_id} as source_system_id
    , fk_axapta_status
    , axapta_status_last_change
    , '/taram_pam_pam' _part_name
      from sales_order_item
      where updated_at >= str_to_date('20170207155959', '%Y%m%d%H%i%s')"""
  }

  def loadMetaTable {
    val clList = Array(
      //Seq(colColumnName, colIsExistsInSource, colColumnType, colIsIncVal)
      ("id_sales_order_item", true, "integer", false),
      ("old_column", false, "integer", false),
      ("fk_sales_order", true, "integer", false),
      ("fk_payment_status", true, "integer", false),
      ("fk_sales_order_item_shipment", true, "integer", false),
      ("item_nr", true, "text", false),
      ("base_cost", true, "number", false),
      ("base_price", true, "number", false),
      ("unit_price", true, "number", false),
      ("codfee_amount", true, "number", false),
      ("coupon_money_value", true, "number", false),
      ("coupon_percent", true, "number", false),
      ("coupon_refundable", true, "integer", false),
      ("coupon_category", true, "text", false),
      ("name", true, "text", false),
      ("sku", true, "text", false),
      ("barcode", true, "text", false),
      ("description", true, "text", false),
      ("weight", true, "number", false),
      ("color", true, "number", false),
      ("created_at", true, "date", false),
      ("updated_at", true, "date", true),
      ("last_payment_status_change", true, "date", false),
      ("amount_paid", true, "number", false),
      ("refunded_money", true, "number", false),
      ("refunded_voucher", true, "number", false),
      ("tax_percent", true, "number", false),
      ("product_size", true, "text", false),
      ("fk_delivery_status", true, "integer", false),
      ("delivery_status_last_change", true, "date", false),
      ("fk_status_list_main", true, "integer", false),
      ("status_main_last_change", true, "date", false),
      ("fk_stock_warehouse", true, "integer", false),
      ("fk_refund", true, "integer", false),
      ("is_gift", true, "integer", false),
      ("fk_refund_status", true, "integer", false),
      ("refund_status_last_change", true, "date", false),
      ("loyalty_money_value", true, "number", false),
      ("fk_axapta_status", true, "integer", false),
      ("axapta_status_last_change", true, "date", false)
    )
    for (column <- clList) {
      columnList = columnList.rows.add(DataValue(column._1), DataValue(column._2), DataValue(column._3), DataValue(column._4)).get
    }

    //pentaho.ktr_file_name
    srcConName = "bob"
    srcConParams += "db_host" -> "localhost"
    srcConParams += "db_port" -> "3306"
    srcConParams += "db_user" -> "dwh"
    srcConParams += "db_password" -> "GhaikFud"
    srcConParams += "db_database" -> "bob_kz"
    inc_id = 99
    groupName = "bob"
    tableName = "sales_order_item"
    output_file_name = "test2.output"
  }
}