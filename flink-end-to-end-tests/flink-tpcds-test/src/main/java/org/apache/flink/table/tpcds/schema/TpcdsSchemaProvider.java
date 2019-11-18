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

package org.apache.flink.table.tpcds.schema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.sql.Date;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to provide all TPC-DS tables' schema information.
 * The data type of column use {@link DataType}
 */
public class TpcdsSchemaProvider {

	private static int tpcdsTableNums = 24;
	private static Map<String, TpcdsSchema> schemaMap = new HashMap<>(tpcdsTableNums);

	static {
		schemaMap.put("catalog_sales", new TpcdsSchema(
			Arrays.asList(
				new Column("cs_sold_date_sk", 0, DataTypes.BIGINT()),
				new Column("cs_sold_time_sk", 1, DataTypes.BIGINT()),
				new Column("cs_ship_date_sk", 2, DataTypes.BIGINT()),
				new Column("cs_bill_customer_sk", 3, DataTypes.BIGINT()),
				new Column("cs_bill_cdemo_sk", 4, DataTypes.BIGINT()),
				new Column("cs_bill_hdemo_sk", 5, DataTypes.BIGINT()),
				new Column("cs_bill_addr_sk", 6, DataTypes.BIGINT()),
				new Column("cs_ship_customer_sk", 7, DataTypes.BIGINT()),
				new Column("cs_ship_cdemo_sk", 8, DataTypes.BIGINT()),
				new Column("cs_ship_hdemo_sk", 9, DataTypes.BIGINT()),
				new Column("cs_ship_addr_sk", 10, DataTypes.BIGINT()),
				new Column("cs_call_center_sk", 11, DataTypes.BIGINT()),
				new Column("cs_catalog_page_sk", 12, DataTypes.BIGINT()),
				new Column("cs_ship_mode_sk", 13, DataTypes.BIGINT()),
				new Column("cs_warehouse_sk", 14, DataTypes.BIGINT()),
				new Column("cs_item_sk", 15, DataTypes.BIGINT()),
				new Column("cs_promo_sk", 16, DataTypes.BIGINT()),
				new Column("cs_order_number", 17, DataTypes.BIGINT()),
				new Column("cs_quantity", 18, DataTypes.INT()),
				new Column("cs_wholesale_cost", 19, DataTypes.DECIMAL(7, 2)),
				new Column("cs_list_price", 20, DataTypes.DECIMAL(7, 2)),
				new Column("cs_sales_price", 21, DataTypes.DECIMAL(7, 2)),
				new Column("cs_ext_discount_amt", 22, DataTypes.DECIMAL(7, 2)),
				new Column("cs_ext_sales_price", 23, DataTypes.DECIMAL(7, 2)),
				new Column("cs_ext_wholesale_cost", 24, DataTypes.DECIMAL(7, 2)),
				new Column("cs_ext_list_price", 25, DataTypes.DECIMAL(7, 2)),
				new Column("cs_ext_tax", 26, DataTypes.DECIMAL(7, 2)),
				new Column("cs_coupon_amt", 27, DataTypes.DECIMAL(7, 2)),
				new Column("cs_ext_ship_cost", 28, DataTypes.DECIMAL(7, 2)),
				new Column("cs_net_paid", 29, DataTypes.DECIMAL(7, 2)),
				new Column("cs_net_paid_inc_tax", 30, DataTypes.DECIMAL(7, 2)),
				new Column("cs_net_paid_inc_ship", 31, DataTypes.DECIMAL(7, 2)),
				new Column("cs_net_paid_inc_ship_tax", 32, DataTypes.DECIMAL(7, 2)),
				new Column("cs_net_profit", 33, DataTypes.DECIMAL(7, 2))
			)));
		schemaMap.put("catalog_returns", new TpcdsSchema(
			Arrays.asList(
				new Column("cr_returned_date_sk", 0, DataTypes.BIGINT()),
				new Column("cr_returned_time_sk", 1, DataTypes.BIGINT()),
				new Column("cr_item_sk", 2, DataTypes.BIGINT()),
				new Column("cr_refunded_customer_sk", 3, DataTypes.BIGINT()),
				new Column("cr_refunded_cdemo_sk", 4, DataTypes.BIGINT()),
				new Column("cr_refunded_hdemo_sk", 5, DataTypes.BIGINT()),
				new Column("cr_refunded_addr_sk", 6, DataTypes.BIGINT()),
				new Column("cr_returning_customer_sk", 7, DataTypes.BIGINT()),
				new Column("cr_returning_cdemo_sk", 8, DataTypes.BIGINT()),
				new Column("cr_returning_hdemo_sk", 9, DataTypes.BIGINT()),
				new Column("cr_returning_addr_sk", 10, DataTypes.BIGINT()),
				new Column("cr_call_center_sk", 11, DataTypes.BIGINT()),
				new Column("cr_catalog_page_sk", 12, DataTypes.BIGINT()),
				new Column("cr_ship_mode_sk", 13, DataTypes.BIGINT()),
				new Column("cr_warehouse_sk", 14, DataTypes.BIGINT()),
				new Column("cr_reason_sk", 15, DataTypes.BIGINT()),
				new Column("cr_order_number", 16, DataTypes.BIGINT()),
				new Column("cr_return_quantity", 17, DataTypes.INT()),
				new Column("cr_return_amount", 18, DataTypes.DECIMAL(7, 2)),
				new Column("cr_return_tax", 19, DataTypes.DECIMAL(7, 2)),
				new Column("cr_return_amt_inc_tax", 20, DataTypes.DECIMAL(7, 2)),
				new Column("cr_fee", 21, DataTypes.DECIMAL(7, 2)),
				new Column("cr_return_ship_cost", 22, DataTypes.DECIMAL(7, 2)),
				new Column("cr_refunded_cash", 23, DataTypes.DECIMAL(7, 2)),
				new Column("cr_reversed_charge", 24, DataTypes.DECIMAL(7, 2)),
				new Column("cr_store_credit", 25, DataTypes.DECIMAL(7, 2)),
				new Column("cr_net_loss", 26, DataTypes.DECIMAL(7, 2))
			)));
		schemaMap.put("inventory", new TpcdsSchema(
			Arrays.asList(
				new Column("inv_date_sk", 0, DataTypes.BIGINT()),
				new Column("inv_item_sk", 1, DataTypes.BIGINT()),
				new Column("inv_warehouse_sk", 2, DataTypes.BIGINT()),
				new Column("inv_quantity_on_hand", 3, DataTypes.INT())
			)));
		schemaMap.put("store_sales", new TpcdsSchema(Arrays.asList(
			new Column("ss_sold_date_sk", 0, DataTypes.BIGINT()),
			new Column("ss_sold_time_sk", 1, DataTypes.BIGINT()),
			new Column("ss_item_sk", 2, DataTypes.BIGINT()),
			new Column("ss_customer_sk", 3, DataTypes.BIGINT()),
			new Column("ss_cdemo_sk", 4, DataTypes.BIGINT()),
			new Column("ss_hdemo_sk", 5, DataTypes.BIGINT()),
			new Column("ss_addr_sk", 6, DataTypes.BIGINT()),
			new Column("ss_store_sk", 7, DataTypes.BIGINT()),
			new Column("ss_promo_sk", 8, DataTypes.BIGINT()),
			new Column("ss_ticket_number", 9, DataTypes.BIGINT()),
			new Column("ss_quantity", 10, DataTypes.INT()),
			new Column("ss_wholesale_cost", 11, DataTypes.DECIMAL(7, 2)),
			new Column("ss_list_price", 12, DataTypes.DECIMAL(7, 2)),
			new Column("ss_sales_price", 13, DataTypes.DECIMAL(7, 2)),
			new Column("ss_ext_discount_amt", 14, DataTypes.DECIMAL(7, 2)),
			new Column("ss_ext_sales_price", 15, DataTypes.DECIMAL(7, 2)),
			new Column("ss_ext_wholesale_cost", 16, DataTypes.DECIMAL(7, 2)),
			new Column("ss_ext_list_price", 17, DataTypes.DECIMAL(7, 2)),
			new Column("ss_ext_tax", 18, DataTypes.DECIMAL(7, 2)),
			new Column("ss_coupon_amt", 19, DataTypes.DECIMAL(7, 2)),
			new Column("ss_net_paid", 20, DataTypes.DECIMAL(7, 2)),
			new Column("ss_net_paid_inc_tax", 21, DataTypes.DECIMAL(7, 2)),
			new Column("ss_net_profit", 22, DataTypes.DECIMAL(7, 2))
		)));
		schemaMap.put("store_returns", new TpcdsSchema(Arrays.asList(
			new Column("sr_returned_date_sk", 0, DataTypes.BIGINT()),
			new Column("sr_return_time_sk", 1, DataTypes.BIGINT()),
			new Column("sr_item_sk", 2, DataTypes.BIGINT()),
			new Column("sr_customer_sk", 3, DataTypes.BIGINT()),
			new Column("sr_cdemo_sk", 4, DataTypes.BIGINT()),
			new Column("sr_hdemo_sk", 5, DataTypes.BIGINT()),
			new Column("sr_addr_sk", 6, DataTypes.BIGINT()),
			new Column("sr_store_sk", 7, DataTypes.BIGINT()),
			new Column("sr_reason_sk", 8, DataTypes.BIGINT()),
			new Column("sr_ticket_number", 9, DataTypes.BIGINT()),
			new Column("sr_return_quantity", 10, DataTypes.INT()),
			new Column("sr_return_amt", 11, DataTypes.DECIMAL(7, 2)),
			new Column("sr_return_tax", 12, DataTypes.DECIMAL(7, 2)),
			new Column("sr_return_amt_inc_tax", 13, DataTypes.DECIMAL(7, 2)),
			new Column("sr_fee", 14, DataTypes.DECIMAL(7, 2)),
			new Column("sr_return_ship_cost", 15, DataTypes.DECIMAL(7, 2)),
			new Column("sr_refunded_cash", 16, DataTypes.DECIMAL(7, 2)),
			new Column("sr_reversed_charge", 17, DataTypes.DECIMAL(7, 2)),
			new Column("sr_store_credit", 18, DataTypes.DECIMAL(7, 2)),
			new Column("sr_net_loss", 19, DataTypes.DECIMAL(7, 2))
		)));
		schemaMap.put("web_sales", new TpcdsSchema(Arrays.asList(
			new Column("ws_sold_date_sk", 0, DataTypes.BIGINT()),
			new Column("ws_sold_time_sk", 1, DataTypes.BIGINT()),
			new Column("ws_ship_date_sk", 2, DataTypes.BIGINT()),
			new Column("ws_item_sk", 3, DataTypes.BIGINT()),
			new Column("ws_bill_customer_sk", 4, DataTypes.BIGINT()),
			new Column("ws_bill_cdemo_sk", 5, DataTypes.BIGINT()),
			new Column("ws_bill_hdemo_sk", 6, DataTypes.BIGINT()),
			new Column("ws_bill_addr_sk", 7, DataTypes.BIGINT()),
			new Column("ws_ship_customer_sk", 8, DataTypes.BIGINT()),
			new Column("ws_ship_cdemo_sk", 9, DataTypes.BIGINT()),
			new Column("ws_ship_hdemo_sk", 10, DataTypes.BIGINT()),
			new Column("ws_ship_addr_sk", 11, DataTypes.BIGINT()),
			new Column("ws_web_page_sk", 12, DataTypes.BIGINT()),
			new Column("ws_web_site_sk", 13, DataTypes.BIGINT()),
			new Column("ws_ship_mode_sk", 14, DataTypes.BIGINT()),
			new Column("ws_warehouse_sk", 15, DataTypes.BIGINT()),
			new Column("ws_promo_sk", 16, DataTypes.BIGINT()),
			new Column("ws_order_number", 17, DataTypes.BIGINT()),
			new Column("ws_quantity", 18, DataTypes.INT()),
			new Column("ws_wholesale_cost", 19, DataTypes.DECIMAL(7, 2)),
			new Column("ws_list_price", 20, DataTypes.DECIMAL(7, 2)),
			new Column("ws_sales_price", 21, DataTypes.DECIMAL(7, 2)),
			new Column("ws_ext_discount_amt", 22, DataTypes.DECIMAL(7, 2)),
			new Column("ws_ext_sales_price", 23, DataTypes.DECIMAL(7, 2)),
			new Column("ws_ext_wholesale_cost", 24, DataTypes.DECIMAL(7, 2)),
			new Column("ws_ext_list_price", 25, DataTypes.DECIMAL(7, 2)),
			new Column("ws_ext_tax", 26, DataTypes.DECIMAL(7, 2)),
			new Column("ws_coupon_amt", 27, DataTypes.DECIMAL(7, 2)),
			new Column("ws_ext_ship_cost", 28, DataTypes.DECIMAL(7, 2)),
			new Column("ws_net_paid", 29, DataTypes.DECIMAL(7, 2)),
			new Column("ws_net_paid_inc_tax", 30, DataTypes.DECIMAL(7, 2)),
			new Column("ws_net_paid_inc_ship", 31, DataTypes.DECIMAL(7, 2)),
			new Column("ws_net_paid_inc_ship_tax", 32, DataTypes.DECIMAL(7, 2)),
			new Column("ws_net_profit", 33, DataTypes.DECIMAL(7, 2))
		)));
		schemaMap.put("web_returns", new TpcdsSchema(Arrays.asList(
			new Column("wr_returned_date_sk", 0, DataTypes.BIGINT()),
			new Column("wr_returned_time_sk", 1, DataTypes.BIGINT()),
			new Column("wr_item_sk", 2, DataTypes.BIGINT()),
			new Column("wr_refunded_customer_sk", 3, DataTypes.BIGINT()),
			new Column("wr_refunded_cdemo_sk", 4, DataTypes.BIGINT()),
			new Column("wr_refunded_hdemo_sk", 5, DataTypes.BIGINT()),
			new Column("wr_refunded_addr_sk", 6, DataTypes.BIGINT()),
			new Column("wr_returning_customer_sk", 7, DataTypes.BIGINT()),
			new Column("wr_returning_cdemo_sk", 8, DataTypes.BIGINT()),
			new Column("wr_returning_hdemo_sk", 9, DataTypes.BIGINT()),
			new Column("wr_returning_addr_sk", 10, DataTypes.BIGINT()),
			new Column("wr_web_page_sk", 11, DataTypes.BIGINT()),
			new Column("wr_reason_sk", 12, DataTypes.BIGINT()),
			new Column("wr_order_number", 13, DataTypes.BIGINT()),
			new Column("wr_return_quantity", 14, DataTypes.INT()),
			new Column("wr_return_amt", 15, DataTypes.DECIMAL(7, 2)),
			new Column("wr_return_tax", 16, DataTypes.DECIMAL(7, 2)),
			new Column("wr_return_amt_inc_tax", 17, DataTypes.DECIMAL(7, 2)),
			new Column("wr_fee", 18, DataTypes.DECIMAL(7, 2)),
			new Column("wr_return_ship_cost", 19, DataTypes.DECIMAL(7, 2)),
			new Column("wr_refunded_cash", 20, DataTypes.DECIMAL(7, 2)),
			new Column("wr_reversed_charge", 21, DataTypes.DECIMAL(7, 2)),
			new Column("wr_account_credit", 22, DataTypes.DECIMAL(7, 2)),
			new Column("wr_net_loss", 23, DataTypes.DECIMAL(7, 2))
		)));
		schemaMap.put("call_center", new TpcdsSchema(Arrays.asList(
			new Column("cc_call_center_sk", 0, DataTypes.BIGINT()),
			new Column("cc_call_center_id", 1, DataTypes.STRING()),
			new Column("cc_rec_start_date", 2, DataTypes.DATE().bridgedTo(Date.class)),
			new Column("cc_rec_end_date", 3, DataTypes.DATE().bridgedTo(Date.class)),
			new Column("cc_closed_date_sk", 4, DataTypes.BIGINT()),
			new Column("cc_open_date_sk", 5, DataTypes.BIGINT()),
			new Column("cc_name", 6, DataTypes.STRING()),
			new Column("cc_class", 7, DataTypes.STRING()),
			new Column("cc_employees", 8, DataTypes.INT()),
			new Column("cc_sq_ft", 9, DataTypes.INT()),
			new Column("cc_hours", 10, DataTypes.STRING()),
			new Column("cc_manager", 11, DataTypes.STRING()),
			new Column("cc_mkt_id", 12, DataTypes.INT()),
			new Column("cc_mkt_class", 13, DataTypes.STRING()),
			new Column("cc_mkt_desc", 14, DataTypes.STRING()),
			new Column("cc_market_manager", 15, DataTypes.STRING()),
			new Column("cc_division", 16, DataTypes.INT()),
			new Column("cc_division_name", 17, DataTypes.STRING()),
			new Column("cc_company", 18, DataTypes.INT()),
			new Column("cc_company_name", 19, DataTypes.STRING()),
			new Column("cc_street_number", 20, DataTypes.STRING()),
			new Column("cc_street_name", 21, DataTypes.STRING()),
			new Column("cc_street_type", 22, DataTypes.STRING()),
			new Column("cc_suite_number", 23, DataTypes.STRING()),
			new Column("cc_city", 24, DataTypes.STRING()),
			new Column("cc_county", 25, DataTypes.STRING()),
			new Column("cc_state", 26, DataTypes.STRING()),
			new Column("cc_zip", 27, DataTypes.STRING()),
			new Column("cc_country", 28, DataTypes.STRING()),
			new Column("cc_gmt_offset", 29, DataTypes.DECIMAL(5, 2)),
			new Column("cc_tax_percentage", 30, DataTypes.DECIMAL(5, 2))
		)));
		schemaMap.put("catalog_page", new TpcdsSchema(Arrays.asList(
			new Column("cp_catalog_page_sk", 0, DataTypes.BIGINT()),
			new Column("cp_catalog_page_id", 1, DataTypes.STRING()),
			new Column("cp_start_date_sk", 2, DataTypes.BIGINT()),
			new Column("cp_end_date_sk", 3, DataTypes.BIGINT()),
			new Column("cp_department", 4, DataTypes.STRING()),
			new Column("cp_catalog_number", 5, DataTypes.INT()),
			new Column("cp_catalog_page_number", 6, DataTypes.INT()),
			new Column("cp_description", 7, DataTypes.STRING()),
			new Column("cp_type", 8, DataTypes.STRING())
		)));
		schemaMap.put("customer", new TpcdsSchema(Arrays.asList(
			new Column("c_customer_sk", 0, DataTypes.BIGINT()),
			new Column("c_customer_id", 1, DataTypes.STRING()),
			new Column("c_current_cdemo_sk", 2, DataTypes.BIGINT()),
			new Column("c_current_hdemo_sk", 3, DataTypes.BIGINT()),
			new Column("c_current_addr_sk", 4, DataTypes.BIGINT()),
			new Column("c_first_shipto_date_sk", 5, DataTypes.BIGINT()),
			new Column("c_first_sales_date_sk", 6, DataTypes.BIGINT()),
			new Column("c_salutation", 7, DataTypes.STRING()),
			new Column("c_first_name", 8, DataTypes.STRING()),
			new Column("c_last_name", 9, DataTypes.STRING()),
			new Column("c_preferred_cust_flag", 10, DataTypes.STRING()),
			new Column("c_birth_day", 11, DataTypes.INT()),
			new Column("c_birth_month", 12, DataTypes.INT()),
			new Column("c_birth_year", 13, DataTypes.INT()),
			new Column("c_birth_country", 14, DataTypes.STRING()),
			new Column("c_login", 15, DataTypes.STRING()),
			new Column("c_email_address", 16, DataTypes.STRING()),
			new Column("c_last_review_date_sk", 17, DataTypes.BIGINT())
		)));
		schemaMap.put("customer_address", new TpcdsSchema(Arrays.asList(
			new Column("ca_address_sk", 0,  DataTypes.BIGINT()),
			new Column("ca_address_id", 1, DataTypes.STRING()),
			new Column("ca_street_number", 2, DataTypes.STRING()),
			new Column("ca_street_name", 3, DataTypes.STRING()),
			new Column("ca_street_type", 4, DataTypes.STRING()),
			new Column("ca_suite_number", 5, DataTypes.STRING()),
			new Column("ca_city", 6, DataTypes.STRING()),
			new Column("ca_county", 7, DataTypes.STRING()),
			new Column("ca_state", 8, DataTypes.STRING()),
			new Column("ca_zip", 9, DataTypes.STRING()),
			new Column("ca_country", 10, DataTypes.STRING()),
			new Column("ca_gmt_offset", 11, DataTypes.DECIMAL(5, 2)),
			new Column("ca_location_type", 12, DataTypes.STRING())
		)));
		schemaMap.put("customer_demographics", new TpcdsSchema(Arrays.asList(
			new Column("cd_demo_sk", 0, DataTypes.BIGINT()),
			new Column("cd_gender", 1, DataTypes.STRING()),
			new Column("cd_marital_status", 2, DataTypes.STRING()),
			new Column("cd_education_status", 3, DataTypes.STRING()),
			new Column("cd_purchase_estimate", 4, DataTypes.INT()),
			new Column("cd_credit_rating", 5, DataTypes.STRING()),
			new Column("cd_dep_count", 6, DataTypes.INT()),
			new Column("cd_dep_employed_count", 7, DataTypes.INT()),
			new Column("cd_dep_college_count", 8, DataTypes.INT())
		)));
		schemaMap.put("date_dim", new TpcdsSchema(Arrays.asList(
			new Column("d_date_sk", 0, DataTypes.BIGINT()),
			new Column("d_date_id", 1, DataTypes.STRING()),
			new Column("d_date", 2, DataTypes.DATE().bridgedTo(Date.class)),
			new Column("d_month_seq", 3, DataTypes.INT()),
			new Column("d_week_seq", 4, DataTypes.INT()),
			new Column("d_quarter_seq", 5, DataTypes.INT()),
			new Column("d_year", 6, DataTypes.INT()),
			new Column("d_dow", 7, DataTypes.INT()),
			new Column("d_moy", 8, DataTypes.INT()),
			new Column("d_dom", 9, DataTypes.INT()),
			new Column("d_qoy", 10, DataTypes.INT()),
			new Column("d_fy_year", 11, DataTypes.INT()),
			new Column("d_fy_quarter_seq", 12, DataTypes.INT()),
			new Column("d_fy_week_seq", 13, DataTypes.INT()),
			new Column("d_day_name", 14, DataTypes.STRING()),
			new Column("d_quarter_name", 15, DataTypes.STRING()),
			new Column("d_holiday", 16, DataTypes.STRING()),
			new Column("d_weekend", 17, DataTypes.STRING()),
			new Column("d_following_holiday", 18, DataTypes.STRING()),
			new Column("d_first_dom", 19, DataTypes.INT()),
			new Column("d_last_dom", 20, DataTypes.INT()),
			new Column("d_same_day_ly", 21, DataTypes.INT()),
			new Column("d_same_day_lq", 22, DataTypes.INT()),
			new Column("d_current_day", 23, DataTypes.STRING()),
			new Column("d_current_week", 24, DataTypes.STRING()),
			new Column("d_current_month", 25, DataTypes.STRING()),
			new Column("d_current_quarter", 26, DataTypes.STRING()),
			new Column("d_current_year", 27, DataTypes.STRING())
		)));
		schemaMap.put("household_demographics", new TpcdsSchema(Arrays.asList(
			new Column("hd_demo_sk", 0, DataTypes.BIGINT()),
			new Column("hd_income_band_sk", 1, DataTypes.BIGINT()),
			new Column("hd_buy_potential", 2,  DataTypes.STRING()),
			new Column("hd_dep_count", 3, DataTypes.INT()),
			new Column("hd_vehicle_count", 4, DataTypes.INT())
		)));
		schemaMap.put("income_band", new TpcdsSchema(Arrays.asList(
			new Column("ib_income_band_sk", 0, DataTypes.BIGINT()),
			new Column("ib_lower_bound", 1, DataTypes.INT()),
			new Column("ib_upper_bound", 2, DataTypes.INT())
		)));
		schemaMap.put("item", new TpcdsSchema(Arrays.asList(
			new Column("i_item_sk", 0, DataTypes.BIGINT()),
			new Column("i_item_id", 1, DataTypes.STRING()),
			new Column("i_rec_start_date", 2, DataTypes.DATE().bridgedTo(Date.class)),
			new Column("i_rec_end_date", 3, DataTypes.DATE().bridgedTo(Date.class)),
			new Column("i_item_desc", 4, DataTypes.STRING()),
			new Column("i_current_price", 5, DataTypes.DECIMAL(7, 2)),
			new Column("i_wholesale_cost", 6, DataTypes.DECIMAL(7, 2)),
			new Column("i_brand_id", 7, DataTypes.INT()),
			new Column("i_brand", 8, DataTypes.STRING()),
			new Column("i_class_id", 9, DataTypes.INT()),
			new Column("i_class", 10, DataTypes.STRING()),
			new Column("i_category_id", 11, DataTypes.INT()),
			new Column("i_category", 12, DataTypes.STRING()),
			new Column("i_manufact_id", 13, DataTypes.INT()),
			new Column("i_manufact", 14, DataTypes.STRING()),
			new Column("i_size", 15, DataTypes.STRING()),
			new Column("i_formulation", 16, DataTypes.STRING()),
			new Column("i_color", 17, DataTypes.STRING()),
			new Column("i_units", 18, DataTypes.STRING()),
			new Column("i_container", 19, DataTypes.STRING()),
			new Column("i_manager_id", 20, DataTypes.INT()),
			new Column("i_product_name", 21, DataTypes.STRING())
		)));
		schemaMap.put("promotion", new TpcdsSchema(Arrays.asList(
			new Column("p_promo_sk", 0, DataTypes.BIGINT()),
			new Column("p_promo_id", 1, DataTypes.STRING()),
			new Column("p_start_date_sk", 2, DataTypes.BIGINT()),
			new Column("p_end_date_sk", 3, DataTypes.BIGINT()),
			new Column("p_item_sk", 4, DataTypes.BIGINT()),
			new Column("p_cost", 5, DataTypes.DECIMAL(15, 2)),
			new Column("p_response_target", 6, DataTypes.INT()),
			new Column("p_promo_name", 7, DataTypes.STRING()),
			new Column("p_channel_dmail", 8, DataTypes.STRING()),
			new Column("p_channel_email", 9, DataTypes.STRING()),
			new Column("p_channel_catalog", 10, DataTypes.STRING()),
			new Column("p_channel_tv", 11, DataTypes.STRING()),
			new Column("p_channel_radio", 12, DataTypes.STRING()),
			new Column("p_channel_press", 13, DataTypes.STRING()),
			new Column("p_channel_event", 14, DataTypes.STRING()),
			new Column("p_channel_demo", 15, DataTypes.STRING()),
			new Column("p_channel_details", 16, DataTypes.STRING()),
			new Column("p_purpose", 17, DataTypes.STRING()),
			new Column("p_discount_active", 18, DataTypes.STRING())
		)));
		schemaMap.put("reason", new TpcdsSchema(Arrays.asList(
			new Column("r_reason_sk", 0, DataTypes.BIGINT()),
			new Column("r_reason_id", 1, DataTypes.STRING()),
			new Column("r_reason_desc", 2, DataTypes.STRING())
		)));
		schemaMap.put("ship_mode", new TpcdsSchema(Arrays.asList(
			new Column("sm_ship_mode_sk", 0,  DataTypes.BIGINT()),
			new Column("sm_ship_mode_id", 1, DataTypes.STRING()),
			new Column("sm_type", 2, DataTypes.STRING()),
			new Column("sm_code", 3, DataTypes.STRING()),
			new Column("sm_carrier", 4, DataTypes.STRING()),
			new Column("sm_contract", 5, DataTypes.STRING())
		)));
		schemaMap.put("store", new TpcdsSchema(Arrays.asList(
			new Column("s_store_sk", 0, DataTypes.BIGINT()),
			new Column("s_store_id", 1, DataTypes.STRING()),
			new Column("s_rec_start_date", 2, DataTypes.DATE().bridgedTo(Date.class)),
			new Column("s_rec_end_date", 3, DataTypes.DATE().bridgedTo(Date.class)),
			new Column("s_closed_date_sk", 4, DataTypes.BIGINT()),
			new Column("s_store_name", 5,  DataTypes.STRING()),
			new Column("s_number_employees", 6, DataTypes.INT()),
			new Column("s_floor_space", 7, DataTypes.INT()),
			new Column("s_hours", 8, DataTypes.STRING()),
			new Column("s_manager", 9, DataTypes.STRING()),
			new Column("s_market_id", 10,  DataTypes.INT()),
			new Column("s_geography_class", 11, DataTypes.STRING()),
			new Column("s_market_desc", 12, DataTypes.STRING()),
			new Column("s_market_manager", 13, DataTypes.STRING()),
			new Column("s_division_id", 14, DataTypes.INT()),
			new Column("s_division_name", 15, DataTypes.STRING()),
			new Column("s_company_id", 16, DataTypes.INT()),
			new Column("s_company_name", 17, DataTypes.STRING()),
			new Column("s_street_number", 18, DataTypes.STRING()),
			new Column("s_street_name", 19, DataTypes.STRING()),
			new Column("s_street_type", 20, DataTypes.STRING()),
			new Column("s_suite_number", 21, DataTypes.STRING()),
			new Column("s_city", 22, DataTypes.STRING()),
			new Column("s_county", 23, DataTypes.STRING()),
			new Column("s_state", 24, DataTypes.STRING()),
			new Column("s_zip", 25, DataTypes.STRING()),
			new Column("s_country", 26, DataTypes.STRING()),
			new Column("s_gmt_offset", 27, DataTypes.DECIMAL(5, 2)),
			new Column("s_tax_precentage", 28, DataTypes.DECIMAL(5, 2))
		)));
		schemaMap.put("time_dim", new TpcdsSchema(Arrays.asList(
			new Column("t_time_sk", 0, DataTypes.BIGINT()),
			new Column("t_time_id", 1, DataTypes.STRING()),
			new Column("t_time", 2, DataTypes.INT()),
			new Column("t_hour", 3, DataTypes.INT()),
			new Column("t_minute", 4, DataTypes.INT()),
			new Column("t_second", 5, DataTypes.INT()),
			new Column("t_am_pm", 6, DataTypes.STRING()),
			new Column("t_shift", 7, DataTypes.STRING()),
			new Column("t_sub_shift", 8, DataTypes.STRING()),
			new Column("t_meal_time", 9, DataTypes.STRING())
		)));
		schemaMap.put("warehouse", new TpcdsSchema(Arrays.asList(
			new Column("w_warehouse_sk", 0, DataTypes.BIGINT()),
			new Column("w_warehouse_id", 1, DataTypes.STRING()),
			new Column("w_warehouse_name", 2, DataTypes.STRING()),
			new Column("w_warehouse_sq_ft", 3, DataTypes.INT()),
			new Column("w_street_number", 4, DataTypes.STRING()),
			new Column("w_street_name", 5, DataTypes.STRING()),
			new Column("w_street_type", 6, DataTypes.STRING()),
			new Column("w_suite_number", 7, DataTypes.STRING()),
			new Column("w_city", 8, DataTypes.STRING()),
			new Column("w_county", 9, DataTypes.STRING()),
			new Column("w_state", 10, DataTypes.STRING()),
			new Column("w_zip", 11, DataTypes.STRING()),
			new Column("w_country", 12, DataTypes.STRING()),
			new Column("w_gmt_offset", 13, DataTypes.DECIMAL(5, 2))
		)));
		schemaMap.put("web_page", new TpcdsSchema(Arrays.asList(
			new Column("wp_web_page_sk", 0, DataTypes.BIGINT()),
			new Column("wp_web_page_id", 1, DataTypes.STRING()),
			new Column("wp_rec_start_date", 2, DataTypes.DATE().bridgedTo(Date.class)),
			new Column("wp_rec_end_date", 3, DataTypes.DATE().bridgedTo(Date.class)),
			new Column("wp_creation_date_sk", 4, DataTypes.BIGINT()),
			new Column("wp_access_date_sk", 5, DataTypes.BIGINT()),
			new Column("wp_autogen_flag", 6, DataTypes.STRING()),
			new Column("wp_customer_sk", 7, DataTypes.BIGINT()),
			new Column("wp_url", 8, DataTypes.STRING()),
			new Column("wp_type", 9, DataTypes.STRING()),
			new Column("wp_char_count", 10, DataTypes.INT()),
			new Column("wp_link_count", 11, DataTypes.INT()),
			new Column("wp_image_count", 12, DataTypes.INT()),
			new Column("wp_max_ad_count", 13, DataTypes.INT())
		)));
		schemaMap.put("web_site", new TpcdsSchema(Arrays.asList(
			new Column("web_site_sk", 0, DataTypes.BIGINT()),
			new Column("web_site_id", 1, DataTypes.STRING()),
			new Column("web_rec_start_date", 2, DataTypes.DATE().bridgedTo(Date.class)),
			new Column("web_rec_end_date", 3, DataTypes.DATE().bridgedTo(Date.class)),
			new Column("web_name", 4, DataTypes.STRING()),
			new Column("web_open_date_sk", 5, DataTypes.BIGINT()),
			new Column("web_close_date_sk", 6, DataTypes.BIGINT()),
			new Column("web_class", 7, DataTypes.STRING()),
			new Column("web_manager", 8, DataTypes.STRING()),
			new Column("web_mkt_id", 9, DataTypes.INT()),
			new Column("web_mkt_class", 10, DataTypes.STRING()),
			new Column("web_mkt_desc", 11, DataTypes.STRING()),
			new Column("web_market_manager", 12, DataTypes.STRING()),
			new Column("web_company_id", 13, DataTypes.BIGINT()),
			new Column("web_company_name", 14, DataTypes.STRING()),
			new Column("web_street_number", 15, DataTypes.STRING()),
			new Column("web_street_name", 16, DataTypes.STRING()),
			new Column("web_street_type", 17, DataTypes.STRING()),
			new Column("web_suite_number", 18, DataTypes.STRING()),
			new Column("web_city", 19, DataTypes.STRING()),
			new Column("web_county", 20, DataTypes.STRING()),
			new Column("web_state", 21, DataTypes.STRING()),
			new Column("web_zip", 22, DataTypes.STRING()),
			new Column("web_country", 23, DataTypes.STRING()),
			new Column("web_gmt_offset", 24, DataTypes.DECIMAL(5, 2)),
			new Column("web_tax_percentage", 25, DataTypes.DECIMAL(5, 2))
		)));
	}

	public static Schema getTpcdsSchema(String tableName) {
		if (schemaMap.containsKey(tableName)) {
			return schemaMap.get(tableName);
		}
		else {
			throw new IllegalArgumentException("table " + tableName + " does not exist.");
		}
	}
}

