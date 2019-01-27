/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.tpc

import java.util

import com.google.common.collect.ImmutableSet
import org.apache.flink.table.api.types.{DataTypes, InternalType}

case class Column(
    name: String,
    index: Int,
    internalType: InternalType,
    isNullable: Boolean = true,
    isPrimaryKey: Boolean = false
)

trait TpcDsSchema extends Schema {

  val columns: Array[Column]

  def getFieldNames: Array[String] = columns.map(_.name)

  def getFieldTypes: Array[InternalType] = columns.map(_.internalType)

  def getFieldNullables: Array[Boolean] = columns.map(_.isNullable)

  override def getUniqueKeys: util.Set[util.Set[String]] = ImmutableSet.of(
    ImmutableSet.copyOf(columns.filter(_.isPrimaryKey).map(_.name)))
}

object CatalogSales extends TpcDsSchema {

  override val columns = Array[Column](
    Column("cs_sold_date_sk", 0, DataTypes.LONG),
    Column("cs_sold_time_sk", 1, DataTypes.LONG),
    Column("cs_ship_date_sk", 2, DataTypes.LONG),
    Column("cs_bill_customer_sk", 3, DataTypes.LONG),
    Column("cs_bill_cdemo_sk", 4, DataTypes.LONG),
    Column("cs_bill_hdemo_sk", 5, DataTypes.LONG),
    Column("cs_bill_addr_sk", 6, DataTypes.LONG),
    Column("cs_ship_customer_sk", 7, DataTypes.LONG),
    Column("cs_ship_cdemo_sk", 8, DataTypes.LONG),
    Column("cs_ship_hdemo_sk", 9, DataTypes.LONG),
    Column("cs_ship_addr_sk", 10, DataTypes.LONG),
    Column("cs_call_center_sk", 11, DataTypes.LONG),
    Column("cs_catalog_page_sk", 12, DataTypes.LONG),
    Column("cs_ship_mode_sk", 13, DataTypes.LONG),
    Column("cs_warehouse_sk", 14, DataTypes.LONG),
    Column("cs_item_sk", 15, DataTypes.LONG, false, true),
    Column("cs_promo_sk", 16, DataTypes.LONG),
    Column("cs_order_number", 17, DataTypes.LONG, false, true),
    Column("cs_quantity", 18, DataTypes.LONG),
    Column("cs_wholesale_cost", 19, DataTypes.createDecimalType(7, 2)),
    Column("cs_list_price", 20, DataTypes.createDecimalType(7, 2)),
    Column("cs_sales_price", 21, DataTypes.createDecimalType(7, 2)),
    Column("cs_ext_discount_amt", 22, DataTypes.createDecimalType(7, 2)),
    Column("cs_ext_sales_price", 23, DataTypes.createDecimalType(7, 2)),
    Column("cs_ext_wholesale_cost", 24, DataTypes.createDecimalType(7, 2)),
    Column("cs_ext_list_price", 25, DataTypes.createDecimalType(7, 2)),
    Column("cs_ext_tax", 26, DataTypes.createDecimalType(7, 2)),
    Column("cs_coupon_amt", 27, DataTypes.createDecimalType(7, 2)),
    Column("cs_ext_ship_cost", 28, DataTypes.createDecimalType(7, 2)),
    Column("cs_net_paid", 29, DataTypes.createDecimalType(7, 2)),
    Column("cs_net_paid_inc_tax", 30, DataTypes.createDecimalType(7, 2)),
    Column("cs_net_paid_inc_ship", 31, DataTypes.createDecimalType(7, 2)),
    Column("cs_net_paid_inc_ship_tax", 32, DataTypes.createDecimalType(7, 2)),
    Column("cs_net_profit", 33, DataTypes.createDecimalType(7, 2)))
}

object CatalogReturns extends TpcDsSchema {
  override val columns = Array[Column](
    Column("cr_returned_date_sk", 0, DataTypes.LONG),
    Column("cr_returned_time_sk", 1, DataTypes.LONG),
    Column("cr_item_sk", 2, DataTypes.LONG, false, true),
    Column("cr_refunded_customer_sk", 3, DataTypes.LONG),
    Column("cr_refunded_cdemo_sk", 4, DataTypes.LONG),
    Column("cr_refunded_hdemo_sk", 5, DataTypes.LONG),
    Column("cr_refunded_addr_sk", 6, DataTypes.LONG),
    Column("cr_returning_customer_sk", 7, DataTypes.LONG),
    Column("cr_returning_cdemo_sk", 8, DataTypes.LONG),
    Column("cr_returning_hdemo_sk", 9, DataTypes.LONG),
    Column("cr_returning_addr_sk", 10, DataTypes.LONG),
    Column("cr_call_center_sk", 11, DataTypes.LONG),
    Column("cr_catalog_page_sk", 12, DataTypes.LONG),
    Column("cr_ship_mode_sk", 13, DataTypes.LONG),
    Column("cr_warehouse_sk", 14, DataTypes.LONG),
    Column("cr_reason_sk", 15, DataTypes.LONG),
    Column("cr_order_number", 16, DataTypes.LONG, false, true),
    Column("cr_return_quantity", 17, DataTypes.LONG),
    Column("cr_return_amount", 18, DataTypes.createDecimalType(7, 2)),
    Column("cr_return_tax", 19, DataTypes.createDecimalType(7, 2)),
    Column("cr_return_amt_inc_tax", 20, DataTypes.createDecimalType(7, 2)),
    Column("cr_fee", 21, DataTypes.createDecimalType(7, 2)),
    Column("cr_return_ship_cost", 22, DataTypes.createDecimalType(7, 2)),
    Column("cr_refunded_cash", 23, DataTypes.createDecimalType(7, 2)),
    Column("cr_reversed_charge", 24, DataTypes.createDecimalType(7, 2)),
    Column("cr_store_credit", 25, DataTypes.createDecimalType(7, 2)),
    Column("cr_net_loss", 26, DataTypes.createDecimalType(7, 2)))
}

object Inventory extends TpcDsSchema {

  override val columns = Array[Column](
    Column("inv_date_sk", 0, DataTypes.LONG, false, true),
    Column("inv_item_sk", 1, DataTypes.LONG, false, true),
    Column("inv_warehouse_sk", 2, DataTypes.LONG, false, true),
    Column("inv_quantity_on_hand", 3, DataTypes.INT, true, false)
  )
}

object StoreSales extends TpcDsSchema {
  override val columns = Array[Column](
    Column("ss_sold_date_sk", 0, DataTypes.LONG, true, false),
    Column("ss_sold_time_sk", 1, DataTypes.LONG, true, false),
    Column("ss_item_sk", 2, DataTypes.LONG, false, true),
    Column("ss_customer_sk", 3, DataTypes.LONG, true, false),
    Column("ss_cdemo_sk", 4, DataTypes.LONG, true, false),
    Column("ss_hdemo_sk", 5, DataTypes.LONG, true, false),
    Column("ss_addr_sk", 6, DataTypes.LONG, true, false),
    Column("ss_store_sk", 7, DataTypes.LONG, true, false),
    Column("ss_promo_sk", 8, DataTypes.LONG, true, false),
    Column("ss_ticket_number", 9, DataTypes.LONG, false, true),
    Column("ss_quantity", 10, DataTypes.LONG, true, false),
    Column("ss_wholesale_cost", 11, DataTypes.createDecimalType(7, 2), true, false),
    Column("ss_list_price", 12, DataTypes.createDecimalType(7, 2), true, false),
    Column("ss_sales_price", 13, DataTypes.createDecimalType(7, 2), true, false),
    Column("ss_ext_discount_amt", 14, DataTypes.createDecimalType(7, 2), true, false),
    Column("ss_ext_sales_price", 15, DataTypes.createDecimalType(7, 2), true, false),
    Column("ss_ext_wholesale_cost", 16, DataTypes.createDecimalType(7, 2), true, false),
    Column("ss_ext_list_price", 17, DataTypes.createDecimalType(7, 2), true, false),
    Column("ss_ext_tax", 18, DataTypes.createDecimalType(7, 2), true, false),
    Column("ss_coupon_amt", 19, DataTypes.createDecimalType(7, 2), true, false),
    Column("ss_net_paid", 20, DataTypes.createDecimalType(7, 2), true, false),
    Column("ss_net_paid_inc_tax", 21, DataTypes.createDecimalType(7, 2), true, false),
    Column("ss_net_profit", 22, DataTypes.createDecimalType(7, 2), true, false)
  )
}

object StoreReturns extends TpcDsSchema {

  override val columns = Array[Column](
    Column("sr_returned_date_sk", 0, DataTypes.LONG, true, false),
    Column("sr_return_time_sk", 1, DataTypes.LONG, true, false),
    Column("sr_item_sk", 2, DataTypes.LONG, false, true),
    Column("sr_customer_sk", 3, DataTypes.LONG, true, false),
    Column("sr_cdemo_sk", 4, DataTypes.LONG, true, false),
    Column("sr_hdemo_sk", 5, DataTypes.LONG, true, false),
    Column("sr_addr_sk", 6, DataTypes.LONG, true, false),
    Column("sr_store_sk", 7, DataTypes.LONG, true, false),
    Column("sr_reason_sk", 8, DataTypes.LONG, true, false),
    Column("sr_ticket_number", 9, DataTypes.LONG, false, true),
    Column("sr_return_quantity", 10, DataTypes.LONG, true, false),
    Column("sr_return_amt", 11, DataTypes.createDecimalType(7, 2), true, false),
    Column("sr_return_tax", 12, DataTypes.createDecimalType(7, 2), true, false),
    Column("sr_return_amt_inc_tax", 13, DataTypes.createDecimalType(7, 2), true, false),
    Column("sr_fee", 14, DataTypes.createDecimalType(7, 2), true, false),
    Column("sr_return_ship_cost", 15, DataTypes.createDecimalType(7, 2), true, false),
    Column("sr_refunded_cash", 16, DataTypes.createDecimalType(7, 2), true, false),
    Column("sr_reversed_charge", 17, DataTypes.createDecimalType(7, 2), true, false),
    Column("sr_store_credit", 18, DataTypes.createDecimalType(7, 2), true, false),
    Column("sr_net_loss", 19, DataTypes.createDecimalType(7, 2), true, false)
  )

}

object WebSales extends TpcDsSchema {

  override val columns = Array[Column](
    Column("ws_sold_date_sk", 0, DataTypes.LONG, true, false),
    Column("ws_sold_time_sk", 1, DataTypes.LONG, true, false),
    Column("ws_ship_date_sk", 2, DataTypes.LONG, true, false),
    Column("ws_item_sk", 3, DataTypes.LONG, false, true),
    Column("ws_bill_customer_sk", 4, DataTypes.LONG, true, false),
    Column("ws_bill_cdemo_sk", 5, DataTypes.LONG, true, false),
    Column("ws_bill_hdemo_sk", 6, DataTypes.LONG, true, false),
    Column("ws_bill_addr_sk", 7, DataTypes.LONG, true, false),
    Column("ws_ship_customer_sk", 8, DataTypes.LONG, true, false),
    Column("ws_ship_cdemo_sk", 9, DataTypes.LONG, true, false),
    Column("ws_ship_hdemo_sk", 10, DataTypes.LONG, true, false),
    Column("ws_ship_addr_sk", 11, DataTypes.LONG, true, false),
    Column("ws_web_page_sk", 12, DataTypes.LONG, true, false),
    Column("ws_web_site_sk", 13, DataTypes.LONG, true, false),
    Column("ws_ship_mode_sk", 14, DataTypes.LONG, true, false),
    Column("ws_warehouse_sk", 15, DataTypes.LONG, true, false),
    Column("ws_promo_sk", 16, DataTypes.LONG, true, false),
    Column("ws_order_number", 17, DataTypes.LONG, false, true),
    Column("ws_quantity", 18, DataTypes.LONG, true, false),
    Column("ws_wholesale_cost", 19, DataTypes.createDecimalType(7, 2), true, false),
    Column("ws_list_price", 20, DataTypes.createDecimalType(7, 2), true, false),
    Column("ws_sales_price", 21, DataTypes.createDecimalType(7, 2), true, false),
    Column("ws_ext_discount_amt", 22, DataTypes.createDecimalType(7, 2), true, false),
    Column("ws_ext_sales_price", 23, DataTypes.createDecimalType(7, 2), true, false),
    Column("ws_ext_wholesale_cost", 24, DataTypes.createDecimalType(7, 2), true, false),
    Column("ws_ext_list_price", 25, DataTypes.createDecimalType(7, 2), true, false),
    Column("ws_ext_tax", 26, DataTypes.createDecimalType(7, 2), true, false),
    Column("ws_coupon_amt", 27, DataTypes.createDecimalType(7, 2), true, false),
    Column("ws_ext_ship_cost", 28, DataTypes.createDecimalType(7, 2), true, false),
    Column("ws_net_paid", 29, DataTypes.createDecimalType(7, 2), true, false),
    Column("ws_net_paid_inc_tax", 30, DataTypes.createDecimalType(7, 2), true, false),
    Column("ws_net_paid_inc_ship", 31, DataTypes.createDecimalType(7, 2), true, false),
    Column("ws_net_paid_inc_ship_tax", 32, DataTypes.createDecimalType(7, 2), true, false),
    Column("ws_net_profit", 33, DataTypes.createDecimalType(7, 2), true, false)
  )
}

object WebReturns extends TpcDsSchema {

  override val columns = Array[Column](
    Column("wr_returned_date_sk", 0, DataTypes.LONG, true, false),
    Column("wr_returned_time_sk", 1, DataTypes.LONG, true, false),
    Column("wr_item_sk", 2, DataTypes.LONG, false, true),
    Column("wr_refunded_customer_sk", 3, DataTypes.LONG, true, false),
    Column("wr_refunded_cdemo_sk", 4, DataTypes.LONG, true, false),
    Column("wr_refunded_hdemo_sk", 5, DataTypes.LONG, true, false),
    Column("wr_refunded_addr_sk", 6, DataTypes.LONG, true, false),
    Column("wr_returning_customer_sk", 7, DataTypes.LONG, true, false),
    Column("wr_returning_cdemo_sk", 8, DataTypes.LONG, true, false),
    Column("wr_returning_hdemo_sk", 9, DataTypes.LONG, true, false),
    Column("wr_returning_addr_sk", 10, DataTypes.LONG, true, false),
    Column("wr_web_page_sk", 11, DataTypes.LONG, true, false),
    Column("wr_reason_sk", 12, DataTypes.LONG, true, false),
    Column("wr_order_number", 13, DataTypes.LONG, false, true),
    Column("wr_return_quantity", 14, DataTypes.LONG, true, false),
    Column("wr_return_amt", 15, DataTypes.createDecimalType(7, 2), true, false),
    Column("wr_return_tax", 16, DataTypes.createDecimalType(7, 2), true, false),
    Column("wr_return_amt_inc_tax", 17, DataTypes.createDecimalType(7, 2), true, false),
    Column("wr_fee", 18, DataTypes.createDecimalType(7, 2), true, false),
    Column("wr_return_ship_cost", 19, DataTypes.createDecimalType(7, 2), true, false),
    Column("wr_refunded_cash", 20, DataTypes.createDecimalType(7, 2), true, false),
    Column("wr_reversed_charge", 21, DataTypes.createDecimalType(7, 2), true, false),
    Column("wr_account_credit", 22, DataTypes.createDecimalType(7, 2), true, false),
    Column("wr_net_loss", 23, DataTypes.createDecimalType(7, 2), true, false)
  )
}

object CallCenter extends TpcDsSchema {
  override val columns = Array[Column](
    Column("cc_call_center_sk", 0, DataTypes.LONG, false, true),
    Column("cc_call_center_id", 1, DataTypes.STRING, false, false),
    Column("cc_rec_start_date", 2, DataTypes.DATE, true, false),
    Column("cc_rec_end_date", 3, DataTypes.DATE, true, false),
    Column("cc_closed_date_sk", 4, DataTypes.LONG, true, false),
    Column("cc_open_date_sk", 5, DataTypes.LONG, true, false),
    Column("cc_name", 6, DataTypes.STRING, true, false),
    Column("cc_class", 7, DataTypes.STRING, true, false),
    Column("cc_employees", 8, DataTypes.LONG, true, false),
    Column("cc_sq_ft", 9, DataTypes.LONG, true, false),
    Column("cc_hours", 10, DataTypes.STRING, true, false),
    Column("cc_manager", 11, DataTypes.STRING, true, false),
    Column("cc_mkt_id", 12, DataTypes.LONG, true, false),
    Column("cc_mkt_class", 13, DataTypes.STRING, true, false),
    Column("cc_mkt_desc", 14, DataTypes.STRING, true, false),
    Column("cc_market_manager", 15, DataTypes.STRING, true, false),
    Column("cc_division", 16, DataTypes.LONG, true, false),
    Column("cc_division_name", 17, DataTypes.STRING, true, false),
    Column("cc_company", 18, DataTypes.LONG, true, false),
    Column("cc_company_name", 19, DataTypes.STRING, true, false),
    Column("cc_street_number", 20, DataTypes.STRING, true, false),
    Column("cc_street_name", 21, DataTypes.STRING, true, false),
    Column("cc_street_type", 22, DataTypes.STRING, true, false),
    Column("cc_suite_number", 23, DataTypes.STRING, true, false),
    Column("cc_city", 24, DataTypes.STRING, true, false),
    Column("cc_county", 25, DataTypes.STRING, true, false),
    Column("cc_state", 26, DataTypes.STRING, true, false),
    Column("cc_zip", 27, DataTypes.STRING, true, false),
    Column("cc_country", 28, DataTypes.STRING, true, false),
    Column("cc_gmt_offset", 29, DataTypes.createDecimalType(5, 2), true, false),
    Column("cc_tax_percentage", 30, DataTypes.createDecimalType(5, 2), true, false)
  )
}

//8 tables
object CatalogPage extends TpcDsSchema {

  override val columns = Array[Column](
    Column("cp_catalog_page_sk", 0, DataTypes.LONG, false, true),
    Column("cp_catalog_page_id", 1, DataTypes.STRING, false, false),
    Column("cp_start_date_sk", 2, DataTypes.LONG, true, false),
    Column("cp_end_date_sk", 3, DataTypes.LONG, true, false),
    Column("cp_department", 4, DataTypes.STRING, true, false),
    Column("cp_catalog_number", 5, DataTypes.LONG, true, false),
    Column("cp_catalog_page_number", 6, DataTypes.LONG, true, false),
    Column("cp_description", 7, DataTypes.STRING, true, false),
    Column("cp_type", 8, DataTypes.STRING, true, false)
  )
}

object CustomerDS extends TpcDsSchema {

  override val columns = Array[Column](
    Column("c_customer_sk", 0, DataTypes.LONG, false, true),
    Column("c_customer_id", 1, DataTypes.STRING, false, false),
    Column("c_current_cdemo_sk", 2, DataTypes.LONG, true, false),
    Column("c_current_hdemo_sk", 3, DataTypes.LONG, true, false),
    Column("c_current_addr_sk", 4, DataTypes.LONG, true, false),
    Column("c_first_shipto_date_sk", 5, DataTypes.LONG, true, false),
    Column("c_first_sales_date_sk", 6, DataTypes.LONG, true, false),
    Column("c_salutation", 7, DataTypes.STRING, true, false),
    Column("c_first_name", 8, DataTypes.STRING, true, false),
    Column("c_last_name", 9, DataTypes.STRING, true, false),
    Column("c_preferred_cust_flag", 10, DataTypes.STRING, true, false),
    Column("c_birth_day", 11, DataTypes.LONG, true, false),
    Column("c_birth_month", 12, DataTypes.LONG, true, false),
    Column("c_birth_year", 13, DataTypes.LONG, true, false),
    Column("c_birth_country", 14, DataTypes.STRING, true, false),
    Column("c_login", 15, DataTypes.STRING, true, false),
    Column("c_email_address", 16, DataTypes.STRING, true, false),
    Column("c_last_review_date", 17, DataTypes.LONG, true, false)
  )

}


object CustomerAddress extends TpcDsSchema {

  override val columns = Array[Column](
    Column("ca_address_sk", 0,  DataTypes.LONG, false, true),
    Column("ca_address_id", 1, DataTypes.STRING, false, false),
    Column("ca_street_number", 2, DataTypes.STRING, true, false),
    Column("ca_street_name", 3, DataTypes.STRING, true, false),
    Column("ca_street_type", 4, DataTypes.STRING, true, false),
    Column("ca_suite_number", 5, DataTypes.STRING, true, false),
    Column("ca_city", 6, DataTypes.STRING, true, false),
    Column("ca_county", 7, DataTypes.STRING, true, false),
    Column("ca_state", 8, DataTypes.STRING, true, false),
    Column("ca_zip", 9, DataTypes.STRING, true, false),
    Column("ca_country", 10, DataTypes.STRING, true, false),
    Column("ca_gmt_offset", 11, DataTypes.createDecimalType(5, 2), true, false),
    Column("ca_location_type", 12, DataTypes.STRING, true, false)
  )

}

object CustomerDemographics extends TpcDsSchema {

  override val columns = Array[Column](
    Column("cd_demo_sk", 0, DataTypes.LONG, false, true),
    Column("cd_gender", 1, DataTypes.STRING, true, false),
    Column("cd_marital_status", 2, DataTypes.STRING, true, false),
    Column("cd_education_status", 3, DataTypes.STRING, true, false),
    Column("cd_purchase_estimate", 4, DataTypes.LONG, true, false),
    Column("cd_credit_rating", 5, DataTypes.STRING, true, false),
    Column("cd_dep_count", 6, DataTypes.LONG, true, false),
    Column("cd_dep_employed_count", 7, DataTypes.LONG, true, false),
    Column("cd_dep_college_count", 8, DataTypes.LONG, true, false)
  )

}

object DateDim extends TpcDsSchema {

  override val columns = Array[Column](
    Column("d_date_sk", 0, DataTypes.LONG, false, true),
    Column("d_date_id", 1, DataTypes.STRING, false, false),
    Column("d_date", 2, DataTypes.DATE, true, false),
    Column("d_month_seq", 3, DataTypes.LONG, true, false),
    Column("d_week_seq", 4, DataTypes.LONG, true, false),
    Column("d_quarter_seq", 5, DataTypes.LONG, true, false),
    Column("d_year", 6, DataTypes.LONG, true, false),
    Column("d_dow", 7, DataTypes.LONG, true, false),
    Column("d_moy", 8, DataTypes.LONG, true, false),
    Column("d_dom", 9, DataTypes.LONG, true, false),
    Column("d_qoy", 10, DataTypes.LONG, true, false),
    Column("d_fy_year", 11, DataTypes.LONG, true, false),
    Column("d_fy_quarter_seq", 12, DataTypes.LONG, true, false),
    Column("d_fy_week_seq", 13, DataTypes.LONG, true, false),
    Column("d_day_name", 14, DataTypes.STRING, true, false),
    Column("d_quarter_name", 15, DataTypes.STRING, true, false),
    Column("d_holiday", 16, DataTypes.STRING, true, false),
    Column("d_weekend", 17, DataTypes.STRING, true, false),
    Column("d_following_holiday", 18, DataTypes.STRING, true, false),
    Column("d_first_dom", 19, DataTypes.LONG, true, false),
    Column("d_last_dom", 20, DataTypes.LONG, true, false),
    Column("d_same_day_ly", 21, DataTypes.LONG, true, false),
    Column("d_same_day_lq", 22, DataTypes.LONG, true, false),
    Column("d_current_day", 23, DataTypes.STRING, true, false),
    Column("d_current_week", 24, DataTypes.STRING, true, false),
    Column("d_current_month", 25, DataTypes.STRING, true, false),
    Column("d_current_quarter", 26, DataTypes.STRING, true, false),
    Column("d_current_year", 27, DataTypes.STRING, true, false)
  )

}

object HouseholdDemographics extends TpcDsSchema {

  override val columns = Array[Column](
    Column("hd_demo_sk", 0, DataTypes.LONG, false, true),
    Column("hd_income_band_sk", 1, DataTypes.LONG, true, false),
    Column("hd_buy_potential", 2,  DataTypes.STRING, true, false),
    Column("hd_dep_count", 3, DataTypes.LONG, true, false),
    Column("hd_vehicle_count", 4, DataTypes.LONG, true, false)
  )
}

object IncomeBand extends TpcDsSchema {

  override val columns = Array[Column](
    Column("ib_income_band_sk", 0, DataTypes.LONG, false, true),
    Column("ib_lower_bound", 1, DataTypes.LONG, true, false),
    Column("ib_upper_bound", 2, DataTypes.LONG, true, false)
  )

}

object Item extends TpcDsSchema {
  override val columns = Array[Column](
    Column("i_item_sk", 0, DataTypes.LONG, false, true),
    Column("i_item_id", 1, DataTypes.STRING, false, false),
    Column("i_rec_start_date", 2, DataTypes.DATE, true, false),
    Column("i_rec_end_date", 3, DataTypes.DATE, true, false),
    Column("i_item_desc", 4, DataTypes.STRING, true, false),
    Column("i_current_price", 5, DataTypes.createDecimalType(7, 2), true, false),
    Column("i_wholesale_cost", 6, DataTypes.createDecimalType(7, 2), true, false),
    Column("i_brand_id", 7, DataTypes.LONG, true, false),
    Column("i_brand", 8, DataTypes.STRING, true, false),
    Column("i_class_id", 9, DataTypes.LONG, true, false),
    Column("i_class", 10, DataTypes.STRING, true, false),
    Column("i_category_id", 11, DataTypes.LONG, true, false),
    Column("i_category", 12, DataTypes.STRING, true, false),
    Column("i_manufact_id", 13, DataTypes.LONG, true, false),
    Column("i_manufact", 14, DataTypes.STRING, true, false),
    Column("i_size", 15, DataTypes.STRING, true, false),
    Column("i_formulation", 16, DataTypes.STRING, true, false),
    Column("i_color", 17, DataTypes.STRING, true, false),
    Column("i_units", 18, DataTypes.STRING, true, false),
    Column("i_container", 19, DataTypes.STRING, true, false),
    Column("i_manager_id", 20, DataTypes.LONG, true, false),
    Column("i_product_name", 21, DataTypes.STRING, true, false)
  )

}

//8 tables
object Promotion extends TpcDsSchema {

  override val columns = Array[Column](
    Column("p_promo_sk", 0, DataTypes.LONG, false, true),
    Column("p_promo_id", 1, DataTypes.STRING, false, false),
    Column("p_start_date_sk", 2, DataTypes.LONG, true, false),
    Column("p_end_date_sk", 3, DataTypes.LONG, true, false),
    Column("p_item_sk", 4, DataTypes.LONG, true, false),
    Column("p_cost", 5, DataTypes.createDecimalType(15, 2), true, false),
    Column("p_response_target", 6, DataTypes.LONG, true, false),
    Column("p_promo_name", 7, DataTypes.STRING, true, false),
    Column("p_channel_dmail", 8, DataTypes.STRING, true, false),
    Column("p_channel_email", 9, DataTypes.STRING, true, false),
    Column("p_channel_catalog", 10, DataTypes.STRING, true, false),
    Column("p_channel_tv", 11, DataTypes.STRING, true, false),
    Column("p_channel_radio", 12, DataTypes.STRING, true, false),
    Column("p_channel_press", 13, DataTypes.STRING, true, false),
    Column("p_channel_event", 14, DataTypes.STRING, true, false),
    Column("p_channel_demo", 15, DataTypes.STRING, true, false),
    Column("p_channel_details", 16, DataTypes.STRING, true, false),
    Column("p_purpose", 17, DataTypes.STRING, true, false),
    Column("p_discount_active", 18, DataTypes.STRING, true, false)
  )

}

object Reason extends TpcDsSchema {

  override val columns = Array[Column](
    Column("r_reason_sk", 0, DataTypes.LONG, false, true),
    Column("r_reason_id", 1, DataTypes.STRING, false, false),
    Column("r_reason_desc", 2, DataTypes.STRING, true, false)
  )

}

object ShipMode extends TpcDsSchema {

  override val columns = Array[Column](
    Column("sm_ship_mode_sk", 0,  DataTypes.LONG, false, true),
    Column("sm_ship_mode_id", 1, DataTypes.STRING, false, false),
    Column("sm_type", 2, DataTypes.STRING, true, false),
    Column("sm_code", 3, DataTypes.STRING, true, false),
    Column("sm_carrier", 4, DataTypes.STRING, true, false),
    Column("sm_contract", 5, DataTypes.STRING, true, false)
  )

}

object Store extends TpcDsSchema {

  override val columns = Array[Column](
    Column("s_store_sk", 0, DataTypes.LONG, false, true),
    Column("s_store_id", 1, DataTypes.STRING, false, false),
    Column("s_rec_start_date", 2, DataTypes.DATE, true, false),
    Column("s_rec_end_date", 3, DataTypes.DATE, true, false),
    Column("s_closed_date_sk", 4, DataTypes.LONG, true, false),
    Column("s_store_name", 5,  DataTypes.STRING, true, false),
    Column("s_number_employees", 6, DataTypes.LONG, true, false),
    Column("s_floor_space", 7, DataTypes.LONG, true, false),
    Column("s_hours", 8, DataTypes.STRING, true, false),
    Column("s_manager", 9, DataTypes.STRING, true, false),
    Column("s_market_id", 10,  DataTypes.LONG, true, false),
    Column("s_geography_class", 11, DataTypes.STRING, true, false),
    Column("s_market_desc", 12, DataTypes.STRING, true, false),
    Column("s_market_manager", 13, DataTypes.STRING, true, false),
    Column("s_division_id", 14, DataTypes.LONG, true, false),
    Column("s_division_name", 15, DataTypes.STRING, true, false),
    Column("s_company_id", 16, DataTypes.LONG, true, false),
    Column("s_company_name", 17, DataTypes.STRING, true, false),
    Column("s_street_number", 18, DataTypes.STRING, true, false),
    Column("s_street_name", 19, DataTypes.STRING, true, false),
    Column("s_street_type", 20, DataTypes.STRING, true, false),
    Column("s_suite_number", 21, DataTypes.STRING, true, false),
    Column("s_city", 22, DataTypes.STRING, true, false),
    Column("s_county", 23, DataTypes.STRING, true, false),
    Column("s_state", 24, DataTypes.STRING, true, false),
    Column("s_zip", 25, DataTypes.STRING, true, false),
    Column("s_country", 26, DataTypes.STRING, true, false),
    Column("s_gmt_offset", 27, DataTypes.createDecimalType(5, 2), true, false),
    Column("s_tax_precentage", 28, DataTypes.createDecimalType(5, 2), true, false)
  )

}

object TimeDim extends TpcDsSchema {
  override val columns = Array[Column](
    Column("t_time_sk", 0, DataTypes.LONG, false, true),
    Column("t_time_id", 1, DataTypes.STRING, false, false),
    Column("t_time", 2, DataTypes.LONG, true, false),
    Column("t_hour", 3, DataTypes.LONG, true, false),
    Column("t_minute", 4, DataTypes.LONG, true, false),
    Column("t_second", 5, DataTypes.LONG, true, false),
    Column("t_am_pm", 6, DataTypes.STRING, true, false),
    Column("t_shift", 7, DataTypes.STRING, true, false),
    Column("t_sub_shift", 8, DataTypes.STRING, true, false),
    Column("t_meal_time", 9, DataTypes.STRING, true, false)
  )

}

object Warehouse extends TpcDsSchema {

  override val columns = Array[Column](
    Column("w_warehouse_sk", 0, DataTypes.LONG, false, true),
    Column("w_warehouse_id", 1, DataTypes.STRING, false, false),
    Column("w_warehouse_name", 2, DataTypes.STRING, true, false),
    Column("w_warehouse_sq_ft", 3, DataTypes.LONG, true, false),
    Column("w_street_number", 4, DataTypes.STRING, true, false),
    Column("w_street_name", 5, DataTypes.STRING, true, false),
    Column("w_street_type", 6, DataTypes.STRING, true, false),
    Column("w_suite_number", 7, DataTypes.STRING, true, false),
    Column("w_city", 8, DataTypes.STRING, true, false),
    Column("w_county", 9, DataTypes.STRING, true, false),
    Column("w_state", 10, DataTypes.STRING, true, false),
    Column("w_zip", 11, DataTypes.STRING, true, false),
    Column("w_country", 12, DataTypes.STRING, true, false),
    Column("w_gmt_offset", 13, DataTypes.createDecimalType(5, 2), true, false)
  )

}

object WebPage extends TpcDsSchema {

  override val columns = Array[Column](
    Column("wp_web_page_sk", 0, DataTypes.LONG, false, true),
    Column("wp_web_page_id", 1, DataTypes.STRING, false, false),
    Column("wp_rec_start_date", 2, DataTypes.DATE, true, false),
    Column("wp_rec_end_date", 3, DataTypes.DATE, true, false),
    Column("wp_creation_date_sk", 4, DataTypes.LONG, true, false),
    Column("wp_access_date_sk", 5, DataTypes.LONG, true, false),
    Column("wp_autogen_flag", 6, DataTypes.STRING, true, false),
    Column("wp_customer_sk", 7, DataTypes.LONG, true, false),
    Column("wp_url", 8, DataTypes.STRING, true, false),
    Column("wp_type", 9, DataTypes.STRING, true, false),
    Column("wp_char_count", 10, DataTypes.LONG, true, false),
    Column("wp_link_count", 11, DataTypes.LONG, true, false),
    Column("wp_image_count", 12, DataTypes.LONG, true, false),
    Column("wp_max_ad_count", 13, DataTypes.LONG, true, false)
  )

}


object WebSite extends TpcDsSchema {
  override val columns = Array[Column](
    Column("web_site_sk", 0, DataTypes.LONG, false, true),
    Column("web_site_id", 1, DataTypes.STRING, false, false),
    Column("web_rec_start_date", 2, DataTypes.DATE, true, false),
    Column("web_rec_end_date", 3, DataTypes.DATE, true, false),
    Column("web_name", 4, DataTypes.STRING, true, false),
    Column("web_open_date_sk", 5, DataTypes.LONG, true, false),
    Column("web_close_date_sk", 6, DataTypes.LONG, true, false),
    Column("web_class", 7, DataTypes.STRING, true, false),
    Column("web_manager", 8, DataTypes.STRING, true, false),
    Column("web_mkt_id", 9, DataTypes.LONG, true, false),
    Column("web_mkt_class", 10, DataTypes.STRING, true, false),
    Column("web_mkt_desc", 11, DataTypes.STRING, true, false),
    Column("web_market_manager", 12, DataTypes.STRING, true, false),
    Column("web_company_id", 13, DataTypes.LONG, true, false),
    Column("web_company_name", 14, DataTypes.STRING, true, false),
    Column("web_street_number", 15, DataTypes.STRING, true, false),
    Column("web_street_name", 16, DataTypes.STRING, true, false),
    Column("web_street_type", 17, DataTypes.STRING, true, false),
    Column("web_suite_number", 18, DataTypes.STRING, true, false),
    Column("web_city", 19, DataTypes.STRING, true, false),
    Column("web_county", 20, DataTypes.STRING, true, false),
    Column("web_state", 21, DataTypes.STRING, true, false),
    Column("web_zip", 22, DataTypes.STRING, true, false),
    Column("web_country", 23, DataTypes.STRING, true, false),
    Column("web_gmt_offset", 24, DataTypes.createDecimalType(5, 2), true, false),
    Column("web_tax_percentage", 25, DataTypes.createDecimalType(5, 2), true, false)
  )

}

//
object TpcDsSchemaProvider {
  val schemaMap: Map[String, Schema] = Map(
    "catalog_sales" -> CatalogSales,
    "catalog_returns" -> CatalogReturns,
    "inventory" -> Inventory,
    "store_sales" -> StoreSales,
    "store_returns" -> StoreReturns,
    "web_sales" -> WebSales,
    "web_returns" -> WebReturns,
    "call_center" -> CallCenter,

    "catalog_page" -> CatalogPage,
    "customer" -> CustomerDS,
    "customer_address" -> CustomerAddress,
    "customer_demographics" -> CustomerDemographics,
    "date_dim" -> DateDim,
    "household_demographics" -> HouseholdDemographics,
    "income_band" -> IncomeBand,
    "item" -> Item,

    "promotion" -> Promotion,
    "reason" -> Reason,
    "ship_mode" -> ShipMode,
    "store" -> Store,
    "time_dim" -> TimeDim,
    "warehouse" -> Warehouse,
    "web_page" -> WebPage,
    "web_site" -> WebSite
  )

  def getSchema(tableName: String): Schema = {
    if (schemaMap.contains(tableName)) {
      schemaMap(tableName)
    } else {
      throw new IllegalArgumentException(s"$tableName does not exist!")
    }
  }

}
