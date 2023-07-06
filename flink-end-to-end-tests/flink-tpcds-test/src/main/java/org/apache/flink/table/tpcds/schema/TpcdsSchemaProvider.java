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
import org.apache.flink.util.CollectionUtil;

import java.sql.Date;
import java.util.Arrays;
import java.util.Map;

/**
 * Class to provide all TPC-DS tables' schema information. The data type of column use {@link
 * DataType}
 */
public class TpcdsSchemaProvider {

    private static final int tpcdsTableNums = 24;
    private static final Map<String, TpcdsSchema> schemaMap = createTableSchemas();

    private static Map<String, TpcdsSchema> createTableSchemas() {
        final Map<String, TpcdsSchema> schemaMap =
                CollectionUtil.newHashMapWithExpectedSize(tpcdsTableNums);
        schemaMap.put(
                "catalog_sales",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("cs_sold_date_sk", DataTypes.BIGINT()),
                                new Column("cs_sold_time_sk", DataTypes.BIGINT()),
                                new Column("cs_ship_date_sk", DataTypes.BIGINT()),
                                new Column("cs_bill_customer_sk", DataTypes.BIGINT()),
                                new Column("cs_bill_cdemo_sk", DataTypes.BIGINT()),
                                new Column("cs_bill_hdemo_sk", DataTypes.BIGINT()),
                                new Column("cs_bill_addr_sk", DataTypes.BIGINT()),
                                new Column("cs_ship_customer_sk", DataTypes.BIGINT()),
                                new Column("cs_ship_cdemo_sk", DataTypes.BIGINT()),
                                new Column("cs_ship_hdemo_sk", DataTypes.BIGINT()),
                                new Column("cs_ship_addr_sk", DataTypes.BIGINT()),
                                new Column("cs_call_center_sk", DataTypes.BIGINT()),
                                new Column("cs_catalog_page_sk", DataTypes.BIGINT()),
                                new Column("cs_ship_mode_sk", DataTypes.BIGINT()),
                                new Column("cs_warehouse_sk", DataTypes.BIGINT()),
                                new Column("cs_item_sk", DataTypes.BIGINT()),
                                new Column("cs_promo_sk", DataTypes.BIGINT()),
                                new Column("cs_order_number", DataTypes.BIGINT()),
                                new Column("cs_quantity", DataTypes.INT()),
                                new Column("cs_wholesale_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("cs_list_price", DataTypes.DECIMAL(7, 2)),
                                new Column("cs_sales_price", DataTypes.DECIMAL(7, 2)),
                                new Column("cs_ext_discount_amt", DataTypes.DECIMAL(7, 2)),
                                new Column("cs_ext_sales_price", DataTypes.DECIMAL(7, 2)),
                                new Column("cs_ext_wholesale_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("cs_ext_list_price", DataTypes.DECIMAL(7, 2)),
                                new Column("cs_ext_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("cs_coupon_amt", DataTypes.DECIMAL(7, 2)),
                                new Column("cs_ext_ship_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("cs_net_paid", DataTypes.DECIMAL(7, 2)),
                                new Column("cs_net_paid_inc_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("cs_net_paid_inc_ship", DataTypes.DECIMAL(7, 2)),
                                new Column("cs_net_paid_inc_ship_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("cs_net_profit", DataTypes.DECIMAL(7, 2)))));
        schemaMap.put(
                "catalog_returns",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("cr_returned_date_sk", DataTypes.BIGINT()),
                                new Column("cr_returned_time_sk", DataTypes.BIGINT()),
                                new Column("cr_item_sk", DataTypes.BIGINT()),
                                new Column("cr_refunded_customer_sk", DataTypes.BIGINT()),
                                new Column("cr_refunded_cdemo_sk", DataTypes.BIGINT()),
                                new Column("cr_refunded_hdemo_sk", DataTypes.BIGINT()),
                                new Column("cr_refunded_addr_sk", DataTypes.BIGINT()),
                                new Column("cr_returning_customer_sk", DataTypes.BIGINT()),
                                new Column("cr_returning_cdemo_sk", DataTypes.BIGINT()),
                                new Column("cr_returning_hdemo_sk", DataTypes.BIGINT()),
                                new Column("cr_returning_addr_sk", DataTypes.BIGINT()),
                                new Column("cr_call_center_sk", DataTypes.BIGINT()),
                                new Column("cr_catalog_page_sk", DataTypes.BIGINT()),
                                new Column("cr_ship_mode_sk", DataTypes.BIGINT()),
                                new Column("cr_warehouse_sk", DataTypes.BIGINT()),
                                new Column("cr_reason_sk", DataTypes.BIGINT()),
                                new Column("cr_order_number", DataTypes.BIGINT()),
                                new Column("cr_return_quantity", DataTypes.INT()),
                                new Column("cr_return_amount", DataTypes.DECIMAL(7, 2)),
                                new Column("cr_return_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("cr_return_amt_inc_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("cr_fee", DataTypes.DECIMAL(7, 2)),
                                new Column("cr_return_ship_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("cr_refunded_cash", DataTypes.DECIMAL(7, 2)),
                                new Column("cr_reversed_charge", DataTypes.DECIMAL(7, 2)),
                                new Column("cr_store_credit", DataTypes.DECIMAL(7, 2)),
                                new Column("cr_net_loss", DataTypes.DECIMAL(7, 2)))));
        schemaMap.put(
                "inventory",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("inv_date_sk", DataTypes.BIGINT()),
                                new Column("inv_item_sk", DataTypes.BIGINT()),
                                new Column("inv_warehouse_sk", DataTypes.BIGINT()),
                                new Column("inv_quantity_on_hand", DataTypes.INT()))));
        schemaMap.put(
                "store_sales",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("ss_sold_date_sk", DataTypes.BIGINT()),
                                new Column("ss_sold_time_sk", DataTypes.BIGINT()),
                                new Column("ss_item_sk", DataTypes.BIGINT()),
                                new Column("ss_customer_sk", DataTypes.BIGINT()),
                                new Column("ss_cdemo_sk", DataTypes.BIGINT()),
                                new Column("ss_hdemo_sk", DataTypes.BIGINT()),
                                new Column("ss_addr_sk", DataTypes.BIGINT()),
                                new Column("ss_store_sk", DataTypes.BIGINT()),
                                new Column("ss_promo_sk", DataTypes.BIGINT()),
                                new Column("ss_ticket_number", DataTypes.BIGINT()),
                                new Column("ss_quantity", DataTypes.INT()),
                                new Column("ss_wholesale_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_list_price", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_sales_price", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_ext_discount_amt", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_ext_sales_price", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_ext_wholesale_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_ext_list_price", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_ext_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_coupon_amt", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_net_paid", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_net_paid_inc_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("ss_net_profit", DataTypes.DECIMAL(7, 2)))));
        schemaMap.put(
                "store_returns",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("sr_returned_date_sk", DataTypes.BIGINT()),
                                new Column("sr_return_time_sk", DataTypes.BIGINT()),
                                new Column("sr_item_sk", DataTypes.BIGINT()),
                                new Column("sr_customer_sk", DataTypes.BIGINT()),
                                new Column("sr_cdemo_sk", DataTypes.BIGINT()),
                                new Column("sr_hdemo_sk", DataTypes.BIGINT()),
                                new Column("sr_addr_sk", DataTypes.BIGINT()),
                                new Column("sr_store_sk", DataTypes.BIGINT()),
                                new Column("sr_reason_sk", DataTypes.BIGINT()),
                                new Column("sr_ticket_number", DataTypes.BIGINT()),
                                new Column("sr_return_quantity", DataTypes.INT()),
                                new Column("sr_return_amt", DataTypes.DECIMAL(7, 2)),
                                new Column("sr_return_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("sr_return_amt_inc_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("sr_fee", DataTypes.DECIMAL(7, 2)),
                                new Column("sr_return_ship_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("sr_refunded_cash", DataTypes.DECIMAL(7, 2)),
                                new Column("sr_reversed_charge", DataTypes.DECIMAL(7, 2)),
                                new Column("sr_store_credit", DataTypes.DECIMAL(7, 2)),
                                new Column("sr_net_loss", DataTypes.DECIMAL(7, 2)))));
        schemaMap.put(
                "web_sales",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("ws_sold_date_sk", DataTypes.BIGINT()),
                                new Column("ws_sold_time_sk", DataTypes.BIGINT()),
                                new Column("ws_ship_date_sk", DataTypes.BIGINT()),
                                new Column("ws_item_sk", DataTypes.BIGINT()),
                                new Column("ws_bill_customer_sk", DataTypes.BIGINT()),
                                new Column("ws_bill_cdemo_sk", DataTypes.BIGINT()),
                                new Column("ws_bill_hdemo_sk", DataTypes.BIGINT()),
                                new Column("ws_bill_addr_sk", DataTypes.BIGINT()),
                                new Column("ws_ship_customer_sk", DataTypes.BIGINT()),
                                new Column("ws_ship_cdemo_sk", DataTypes.BIGINT()),
                                new Column("ws_ship_hdemo_sk", DataTypes.BIGINT()),
                                new Column("ws_ship_addr_sk", DataTypes.BIGINT()),
                                new Column("ws_web_page_sk", DataTypes.BIGINT()),
                                new Column("ws_web_site_sk", DataTypes.BIGINT()),
                                new Column("ws_ship_mode_sk", DataTypes.BIGINT()),
                                new Column("ws_warehouse_sk", DataTypes.BIGINT()),
                                new Column("ws_promo_sk", DataTypes.BIGINT()),
                                new Column("ws_order_number", DataTypes.BIGINT()),
                                new Column("ws_quantity", DataTypes.INT()),
                                new Column("ws_wholesale_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("ws_list_price", DataTypes.DECIMAL(7, 2)),
                                new Column("ws_sales_price", DataTypes.DECIMAL(7, 2)),
                                new Column("ws_ext_discount_amt", DataTypes.DECIMAL(7, 2)),
                                new Column("ws_ext_sales_price", DataTypes.DECIMAL(7, 2)),
                                new Column("ws_ext_wholesale_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("ws_ext_list_price", DataTypes.DECIMAL(7, 2)),
                                new Column("ws_ext_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("ws_coupon_amt", DataTypes.DECIMAL(7, 2)),
                                new Column("ws_ext_ship_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("ws_net_paid", DataTypes.DECIMAL(7, 2)),
                                new Column("ws_net_paid_inc_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("ws_net_paid_inc_ship", DataTypes.DECIMAL(7, 2)),
                                new Column("ws_net_paid_inc_ship_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("ws_net_profit", DataTypes.DECIMAL(7, 2)))));
        schemaMap.put(
                "web_returns",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("wr_returned_date_sk", DataTypes.BIGINT()),
                                new Column("wr_returned_time_sk", DataTypes.BIGINT()),
                                new Column("wr_item_sk", DataTypes.BIGINT()),
                                new Column("wr_refunded_customer_sk", DataTypes.BIGINT()),
                                new Column("wr_refunded_cdemo_sk", DataTypes.BIGINT()),
                                new Column("wr_refunded_hdemo_sk", DataTypes.BIGINT()),
                                new Column("wr_refunded_addr_sk", DataTypes.BIGINT()),
                                new Column("wr_returning_customer_sk", DataTypes.BIGINT()),
                                new Column("wr_returning_cdemo_sk", DataTypes.BIGINT()),
                                new Column("wr_returning_hdemo_sk", DataTypes.BIGINT()),
                                new Column("wr_returning_addr_sk", DataTypes.BIGINT()),
                                new Column("wr_web_page_sk", DataTypes.BIGINT()),
                                new Column("wr_reason_sk", DataTypes.BIGINT()),
                                new Column("wr_order_number", DataTypes.BIGINT()),
                                new Column("wr_return_quantity", DataTypes.INT()),
                                new Column("wr_return_amt", DataTypes.DECIMAL(7, 2)),
                                new Column("wr_return_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("wr_return_amt_inc_tax", DataTypes.DECIMAL(7, 2)),
                                new Column("wr_fee", DataTypes.DECIMAL(7, 2)),
                                new Column("wr_return_ship_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("wr_refunded_cash", DataTypes.DECIMAL(7, 2)),
                                new Column("wr_reversed_charge", DataTypes.DECIMAL(7, 2)),
                                new Column("wr_account_credit", DataTypes.DECIMAL(7, 2)),
                                new Column("wr_net_loss", DataTypes.DECIMAL(7, 2)))));
        schemaMap.put(
                "call_center",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("cc_call_center_sk", DataTypes.BIGINT()),
                                new Column("cc_call_center_id", DataTypes.STRING()),
                                new Column(
                                        "cc_rec_start_date",
                                        DataTypes.DATE().bridgedTo(Date.class)),
                                new Column(
                                        "cc_rec_end_date", DataTypes.DATE().bridgedTo(Date.class)),
                                new Column("cc_closed_date_sk", DataTypes.BIGINT()),
                                new Column("cc_open_date_sk", DataTypes.BIGINT()),
                                new Column("cc_name", DataTypes.STRING()),
                                new Column("cc_class", DataTypes.STRING()),
                                new Column("cc_employees", DataTypes.INT()),
                                new Column("cc_sq_ft", DataTypes.INT()),
                                new Column("cc_hours", DataTypes.STRING()),
                                new Column("cc_manager", DataTypes.STRING()),
                                new Column("cc_mkt_id", DataTypes.INT()),
                                new Column("cc_mkt_class", DataTypes.STRING()),
                                new Column("cc_mkt_desc", DataTypes.STRING()),
                                new Column("cc_market_manager", DataTypes.STRING()),
                                new Column("cc_division", DataTypes.INT()),
                                new Column("cc_division_name", DataTypes.STRING()),
                                new Column("cc_company", DataTypes.INT()),
                                new Column("cc_company_name", DataTypes.STRING()),
                                new Column("cc_street_number", DataTypes.STRING()),
                                new Column("cc_street_name", DataTypes.STRING()),
                                new Column("cc_street_type", DataTypes.STRING()),
                                new Column("cc_suite_number", DataTypes.STRING()),
                                new Column("cc_city", DataTypes.STRING()),
                                new Column("cc_county", DataTypes.STRING()),
                                new Column("cc_state", DataTypes.STRING()),
                                new Column("cc_zip", DataTypes.STRING()),
                                new Column("cc_country", DataTypes.STRING()),
                                new Column("cc_gmt_offset", DataTypes.DECIMAL(5, 2)),
                                new Column("cc_tax_percentage", DataTypes.DECIMAL(5, 2)))));
        schemaMap.put(
                "catalog_page",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("cp_catalog_page_sk", DataTypes.BIGINT()),
                                new Column("cp_catalog_page_id", DataTypes.STRING()),
                                new Column("cp_start_date_sk", DataTypes.BIGINT()),
                                new Column("cp_end_date_sk", DataTypes.BIGINT()),
                                new Column("cp_department", DataTypes.STRING()),
                                new Column("cp_catalog_number", DataTypes.INT()),
                                new Column("cp_catalog_page_number", DataTypes.INT()),
                                new Column("cp_description", DataTypes.STRING()),
                                new Column("cp_type", DataTypes.STRING()))));
        schemaMap.put(
                "customer",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("c_customer_sk", DataTypes.BIGINT()),
                                new Column("c_customer_id", DataTypes.STRING()),
                                new Column("c_current_cdemo_sk", DataTypes.BIGINT()),
                                new Column("c_current_hdemo_sk", DataTypes.BIGINT()),
                                new Column("c_current_addr_sk", DataTypes.BIGINT()),
                                new Column("c_first_shipto_date_sk", DataTypes.BIGINT()),
                                new Column("c_first_sales_date_sk", DataTypes.BIGINT()),
                                new Column("c_salutation", DataTypes.STRING()),
                                new Column("c_first_name", DataTypes.STRING()),
                                new Column("c_last_name", DataTypes.STRING()),
                                new Column("c_preferred_cust_flag", DataTypes.STRING()),
                                new Column("c_birth_day", DataTypes.INT()),
                                new Column("c_birth_month", DataTypes.INT()),
                                new Column("c_birth_year", DataTypes.INT()),
                                new Column("c_birth_country", DataTypes.STRING()),
                                new Column("c_login", DataTypes.STRING()),
                                new Column("c_email_address", DataTypes.STRING()),
                                new Column("c_last_review_date_sk", DataTypes.BIGINT()))));
        schemaMap.put(
                "customer_address",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("ca_address_sk", DataTypes.BIGINT()),
                                new Column("ca_address_id", DataTypes.STRING()),
                                new Column("ca_street_number", DataTypes.STRING()),
                                new Column("ca_street_name", DataTypes.STRING()),
                                new Column("ca_street_type", DataTypes.STRING()),
                                new Column("ca_suite_number", DataTypes.STRING()),
                                new Column("ca_city", DataTypes.STRING()),
                                new Column("ca_county", DataTypes.STRING()),
                                new Column("ca_state", DataTypes.STRING()),
                                new Column("ca_zip", DataTypes.STRING()),
                                new Column("ca_country", DataTypes.STRING()),
                                new Column("ca_gmt_offset", DataTypes.DECIMAL(5, 2)),
                                new Column("ca_location_type", DataTypes.STRING()))));
        schemaMap.put(
                "customer_demographics",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("cd_demo_sk", DataTypes.BIGINT()),
                                new Column("cd_gender", DataTypes.STRING()),
                                new Column("cd_marital_status", DataTypes.STRING()),
                                new Column("cd_education_status", DataTypes.STRING()),
                                new Column("cd_purchase_estimate", DataTypes.INT()),
                                new Column("cd_credit_rating", DataTypes.STRING()),
                                new Column("cd_dep_count", DataTypes.INT()),
                                new Column("cd_dep_employed_count", DataTypes.INT()),
                                new Column("cd_dep_college_count", DataTypes.INT()))));
        schemaMap.put(
                "date_dim",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("d_date_sk", DataTypes.BIGINT()),
                                new Column("d_date_id", DataTypes.STRING()),
                                new Column("d_date", DataTypes.DATE().bridgedTo(Date.class)),
                                new Column("d_month_seq", DataTypes.INT()),
                                new Column("d_week_seq", DataTypes.INT()),
                                new Column("d_quarter_seq", DataTypes.INT()),
                                new Column("d_year", DataTypes.INT()),
                                new Column("d_dow", DataTypes.INT()),
                                new Column("d_moy", DataTypes.INT()),
                                new Column("d_dom", DataTypes.INT()),
                                new Column("d_qoy", DataTypes.INT()),
                                new Column("d_fy_year", DataTypes.INT()),
                                new Column("d_fy_quarter_seq", DataTypes.INT()),
                                new Column("d_fy_week_seq", DataTypes.INT()),
                                new Column("d_day_name", DataTypes.STRING()),
                                new Column("d_quarter_name", DataTypes.STRING()),
                                new Column("d_holiday", DataTypes.STRING()),
                                new Column("d_weekend", DataTypes.STRING()),
                                new Column("d_following_holiday", DataTypes.STRING()),
                                new Column("d_first_dom", DataTypes.INT()),
                                new Column("d_last_dom", DataTypes.INT()),
                                new Column("d_same_day_ly", DataTypes.INT()),
                                new Column("d_same_day_lq", DataTypes.INT()),
                                new Column("d_current_day", DataTypes.STRING()),
                                new Column("d_current_week", DataTypes.STRING()),
                                new Column("d_current_month", DataTypes.STRING()),
                                new Column("d_current_quarter", DataTypes.STRING()),
                                new Column("d_current_year", DataTypes.STRING()))));
        schemaMap.put(
                "household_demographics",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("hd_demo_sk", DataTypes.BIGINT()),
                                new Column("hd_income_band_sk", DataTypes.BIGINT()),
                                new Column("hd_buy_potential", DataTypes.STRING()),
                                new Column("hd_dep_count", DataTypes.INT()),
                                new Column("hd_vehicle_count", DataTypes.INT()))));
        schemaMap.put(
                "income_band",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("ib_income_band_sk", DataTypes.BIGINT()),
                                new Column("ib_lower_bound", DataTypes.INT()),
                                new Column("ib_upper_bound", DataTypes.INT()))));
        schemaMap.put(
                "item",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("i_item_sk", DataTypes.BIGINT()),
                                new Column("i_item_id", DataTypes.STRING()),
                                new Column(
                                        "i_rec_start_date", DataTypes.DATE().bridgedTo(Date.class)),
                                new Column(
                                        "i_rec_end_date", DataTypes.DATE().bridgedTo(Date.class)),
                                new Column("i_item_desc", DataTypes.STRING()),
                                new Column("i_current_price", DataTypes.DECIMAL(7, 2)),
                                new Column("i_wholesale_cost", DataTypes.DECIMAL(7, 2)),
                                new Column("i_brand_id", DataTypes.INT()),
                                new Column("i_brand", DataTypes.STRING()),
                                new Column("i_class_id", DataTypes.INT()),
                                new Column("i_class", DataTypes.STRING()),
                                new Column("i_category_id", DataTypes.INT()),
                                new Column("i_category", DataTypes.STRING()),
                                new Column("i_manufact_id", DataTypes.INT()),
                                new Column("i_manufact", DataTypes.STRING()),
                                new Column("i_size", DataTypes.STRING()),
                                new Column("i_formulation", DataTypes.STRING()),
                                new Column("i_color", DataTypes.STRING()),
                                new Column("i_units", DataTypes.STRING()),
                                new Column("i_container", DataTypes.STRING()),
                                new Column("i_manager_id", DataTypes.INT()),
                                new Column("i_product_name", DataTypes.STRING()))));
        schemaMap.put(
                "promotion",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("p_promo_sk", DataTypes.BIGINT()),
                                new Column("p_promo_id", DataTypes.STRING()),
                                new Column("p_start_date_sk", DataTypes.BIGINT()),
                                new Column("p_end_date_sk", DataTypes.BIGINT()),
                                new Column("p_item_sk", DataTypes.BIGINT()),
                                new Column("p_cost", DataTypes.DECIMAL(15, 2)),
                                new Column("p_response_target", DataTypes.INT()),
                                new Column("p_promo_name", DataTypes.STRING()),
                                new Column("p_channel_dmail", DataTypes.STRING()),
                                new Column("p_channel_email", DataTypes.STRING()),
                                new Column("p_channel_catalog", DataTypes.STRING()),
                                new Column("p_channel_tv", DataTypes.STRING()),
                                new Column("p_channel_radio", DataTypes.STRING()),
                                new Column("p_channel_press", DataTypes.STRING()),
                                new Column("p_channel_event", DataTypes.STRING()),
                                new Column("p_channel_demo", DataTypes.STRING()),
                                new Column("p_channel_details", DataTypes.STRING()),
                                new Column("p_purpose", DataTypes.STRING()),
                                new Column("p_discount_active", DataTypes.STRING()))));
        schemaMap.put(
                "reason",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("r_reason_sk", DataTypes.BIGINT()),
                                new Column("r_reason_id", DataTypes.STRING()),
                                new Column("r_reason_desc", DataTypes.STRING()))));
        schemaMap.put(
                "ship_mode",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("sm_ship_mode_sk", DataTypes.BIGINT()),
                                new Column("sm_ship_mode_id", DataTypes.STRING()),
                                new Column("sm_type", DataTypes.STRING()),
                                new Column("sm_code", DataTypes.STRING()),
                                new Column("sm_carrier", DataTypes.STRING()),
                                new Column("sm_contract", DataTypes.STRING()))));
        schemaMap.put(
                "store",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("s_store_sk", DataTypes.BIGINT()),
                                new Column("s_store_id", DataTypes.STRING()),
                                new Column(
                                        "s_rec_start_date", DataTypes.DATE().bridgedTo(Date.class)),
                                new Column(
                                        "s_rec_end_date", DataTypes.DATE().bridgedTo(Date.class)),
                                new Column("s_closed_date_sk", DataTypes.BIGINT()),
                                new Column("s_store_name", DataTypes.STRING()),
                                new Column("s_number_employees", DataTypes.INT()),
                                new Column("s_floor_space", DataTypes.INT()),
                                new Column("s_hours", DataTypes.STRING()),
                                new Column("s_manager", DataTypes.STRING()),
                                new Column("s_market_id", DataTypes.INT()),
                                new Column("s_geography_class", DataTypes.STRING()),
                                new Column("s_market_desc", DataTypes.STRING()),
                                new Column("s_market_manager", DataTypes.STRING()),
                                new Column("s_division_id", DataTypes.INT()),
                                new Column("s_division_name", DataTypes.STRING()),
                                new Column("s_company_id", DataTypes.INT()),
                                new Column("s_company_name", DataTypes.STRING()),
                                new Column("s_street_number", DataTypes.STRING()),
                                new Column("s_street_name", DataTypes.STRING()),
                                new Column("s_street_type", DataTypes.STRING()),
                                new Column("s_suite_number", DataTypes.STRING()),
                                new Column("s_city", DataTypes.STRING()),
                                new Column("s_county", DataTypes.STRING()),
                                new Column("s_state", DataTypes.STRING()),
                                new Column("s_zip", DataTypes.STRING()),
                                new Column("s_country", DataTypes.STRING()),
                                new Column("s_gmt_offset", DataTypes.DECIMAL(5, 2)),
                                new Column("s_tax_precentage", DataTypes.DECIMAL(5, 2)))));
        schemaMap.put(
                "time_dim",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("t_time_sk", DataTypes.BIGINT()),
                                new Column("t_time_id", DataTypes.STRING()),
                                new Column("t_time", DataTypes.INT()),
                                new Column("t_hour", DataTypes.INT()),
                                new Column("t_minute", DataTypes.INT()),
                                new Column("t_second", DataTypes.INT()),
                                new Column("t_am_pm", DataTypes.STRING()),
                                new Column("t_shift", DataTypes.STRING()),
                                new Column("t_sub_shift", DataTypes.STRING()),
                                new Column("t_meal_time", DataTypes.STRING()))));
        schemaMap.put(
                "warehouse",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("w_warehouse_sk", DataTypes.BIGINT()),
                                new Column("w_warehouse_id", DataTypes.STRING()),
                                new Column("w_warehouse_name", DataTypes.STRING()),
                                new Column("w_warehouse_sq_ft", DataTypes.INT()),
                                new Column("w_street_number", DataTypes.STRING()),
                                new Column("w_street_name", DataTypes.STRING()),
                                new Column("w_street_type", DataTypes.STRING()),
                                new Column("w_suite_number", DataTypes.STRING()),
                                new Column("w_city", DataTypes.STRING()),
                                new Column("w_county", DataTypes.STRING()),
                                new Column("w_state", DataTypes.STRING()),
                                new Column("w_zip", DataTypes.STRING()),
                                new Column("w_country", DataTypes.STRING()),
                                new Column("w_gmt_offset", DataTypes.DECIMAL(5, 2)))));
        schemaMap.put(
                "web_page",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("wp_web_page_sk", DataTypes.BIGINT()),
                                new Column("wp_web_page_id", DataTypes.STRING()),
                                new Column(
                                        "wp_rec_start_date",
                                        DataTypes.DATE().bridgedTo(Date.class)),
                                new Column(
                                        "wp_rec_end_date", DataTypes.DATE().bridgedTo(Date.class)),
                                new Column("wp_creation_date_sk", DataTypes.BIGINT()),
                                new Column("wp_access_date_sk", DataTypes.BIGINT()),
                                new Column("wp_autogen_flag", DataTypes.STRING()),
                                new Column("wp_customer_sk", DataTypes.BIGINT()),
                                new Column("wp_url", DataTypes.STRING()),
                                new Column("wp_type", DataTypes.STRING()),
                                new Column("wp_char_count", DataTypes.INT()),
                                new Column("wp_link_count", DataTypes.INT()),
                                new Column("wp_image_count", DataTypes.INT()),
                                new Column("wp_max_ad_count", DataTypes.INT()))));
        schemaMap.put(
                "web_site",
                new TpcdsSchema(
                        Arrays.asList(
                                new Column("web_site_sk", DataTypes.BIGINT()),
                                new Column("web_site_id", DataTypes.STRING()),
                                new Column(
                                        "web_rec_start_date",
                                        DataTypes.DATE().bridgedTo(Date.class)),
                                new Column(
                                        "web_rec_end_date", DataTypes.DATE().bridgedTo(Date.class)),
                                new Column("web_name", DataTypes.STRING()),
                                new Column("web_open_date_sk", DataTypes.BIGINT()),
                                new Column("web_close_date_sk", DataTypes.BIGINT()),
                                new Column("web_class", DataTypes.STRING()),
                                new Column("web_manager", DataTypes.STRING()),
                                new Column("web_mkt_id", DataTypes.INT()),
                                new Column("web_mkt_class", DataTypes.STRING()),
                                new Column("web_mkt_desc", DataTypes.STRING()),
                                new Column("web_market_manager", DataTypes.STRING()),
                                new Column("web_company_id", DataTypes.BIGINT()),
                                new Column("web_company_name", DataTypes.STRING()),
                                new Column("web_street_number", DataTypes.STRING()),
                                new Column("web_street_name", DataTypes.STRING()),
                                new Column("web_street_type", DataTypes.STRING()),
                                new Column("web_suite_number", DataTypes.STRING()),
                                new Column("web_city", DataTypes.STRING()),
                                new Column("web_county", DataTypes.STRING()),
                                new Column("web_state", DataTypes.STRING()),
                                new Column("web_zip", DataTypes.STRING()),
                                new Column("web_country", DataTypes.STRING()),
                                new Column("web_gmt_offset", DataTypes.DECIMAL(5, 2)),
                                new Column("web_tax_percentage", DataTypes.DECIMAL(5, 2)))));
        return schemaMap;
    }

    public static TpcdsSchema getTableSchema(String tableName) {
        TpcdsSchema result = schemaMap.get(tableName);
        if (result != null) {
            return result;
        } else {
            throw new IllegalArgumentException(
                    "Table schema of table " + tableName + " does not exist.");
        }
    }
}
