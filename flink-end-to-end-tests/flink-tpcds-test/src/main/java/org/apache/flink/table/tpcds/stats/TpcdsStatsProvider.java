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

package org.apache.flink.table.tpcds.stats;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;

import java.util.HashMap;
import java.util.Map;

/**
 * Table statistics information for TPC-DS qualification test, TPC-DS tool version is v2.11.0.
 * The test data scale is 1G, and random seed is 19620718 which strictly follows TPC-DS standard.
 */
public class TpcdsStatsProvider {

	private static final int tpcdsTableNums = 24;
	private static final Map<String, CatalogTableStats> catalogTableStatsMap = createCatalogTableStatsMap();

	private static Map<String, CatalogTableStats> createCatalogTableStatsMap() {
		final Map<String, CatalogTableStats> catalogTableStatsMap = new HashMap<>(tpcdsTableNums);

		//CatalogTableStats for table catalog_sales
		final Map<String, CatalogColumnStatisticsDataBase> catalogsalesColumnStatisticsData = new HashMap<>();
		catalogsalesColumnStatisticsData.put("cs_ship_mode_sk", new CatalogColumnStatisticsDataLong(1L, 20L, 20L, 7127L));
		catalogsalesColumnStatisticsData.put("cs_bill_customer_sk", new CatalogColumnStatisticsDataLong(1L, 100000L, 80434L, 7029L));
		catalogsalesColumnStatisticsData.put("cs_sales_price", new CatalogColumnStatisticsDataDouble(0.00D, 297.83D, 25905L, 7015L));
		catalogsalesColumnStatisticsData.put("cs_net_profit", new CatalogColumnStatisticsDataDouble(-9897.03D, 18950.00D, 496489L, 0L));
		catalogsalesColumnStatisticsData.put("cs_ext_tax", new CatalogColumnStatisticsDataDouble(0.00D, 2416.94D, 84436L, 7135L));
		catalogsalesColumnStatisticsData.put("cs_wholesale_cost", new CatalogColumnStatisticsDataDouble(1.00D, 100.00D, 9872L, 7136L));
		catalogsalesColumnStatisticsData.put("cs_ext_wholesale_cost", new CatalogColumnStatisticsDataDouble(1.02D, 9999.00D, 338975L, 7167L));
		catalogsalesColumnStatisticsData.put("cs_ship_hdemo_sk", new CatalogColumnStatisticsDataLong(1L, 7200L, 7207L, 7133L));
		catalogsalesColumnStatisticsData.put("cs_bill_addr_sk", new CatalogColumnStatisticsDataLong(1L, 50000L, 47665L, 7101L));
		catalogsalesColumnStatisticsData.put("cs_item_sk", new CatalogColumnStatisticsDataLong(1L, 18000L, 17869L, 0L));
		catalogsalesColumnStatisticsData.put("cs_net_paid_inc_ship_tax", new CatalogColumnStatisticsDataDouble(0.00D, 43335.20D, 695438L, 0L));
		catalogsalesColumnStatisticsData.put("cs_ship_customer_sk", new CatalogColumnStatisticsDataLong(1L, 100000L, 80577L, 7226L));
		catalogsalesColumnStatisticsData.put("cs_ext_discount_amt", new CatalogColumnStatisticsDataDouble(0.00D, 28300.44D, 395836L, 7154L));
		catalogsalesColumnStatisticsData.put("cs_sold_date_sk", new CatalogColumnStatisticsDataLong(2450815L, 2452648L, 1833L, 7180L));
		catalogsalesColumnStatisticsData.put("cs_order_number", new CatalogColumnStatisticsDataLong(1L, 160000L, 160194L, 0L));
		catalogsalesColumnStatisticsData.put("cs_quantity", new CatalogColumnStatisticsDataLong(1L, 100L, 100L, 7091L));
		catalogsalesColumnStatisticsData.put("cs_bill_cdemo_sk", new CatalogColumnStatisticsDataLong(5L, 1920765L, 152845L, 7150L));
		catalogsalesColumnStatisticsData.put("cs_call_center_sk", new CatalogColumnStatisticsDataLong(1L, 6L, 6L, 7120L));
		catalogsalesColumnStatisticsData.put("cs_net_paid_inc_tax", new CatalogColumnStatisticsDataDouble(0.00D, 30261.56D, 536274L, 7127L));
		catalogsalesColumnStatisticsData.put("cs_catalog_page_sk", new CatalogColumnStatisticsDataLong(1L, 9828L, 6559L, 7170L));
		catalogsalesColumnStatisticsData.put("cs_ship_cdemo_sk", new CatalogColumnStatisticsDataLong(5L, 1920765L, 153047L, 7137L));
		catalogsalesColumnStatisticsData.put("cs_warehouse_sk", new CatalogColumnStatisticsDataLong(1L, 5L, 5L, 7146L));
		catalogsalesColumnStatisticsData.put("cs_net_paid", new CatalogColumnStatisticsDataDouble(0.00D, 28767.00D, 422353L, 7150L));
		catalogsalesColumnStatisticsData.put("cs_ext_list_price", new CatalogColumnStatisticsDataDouble(1.29D, 29976.00D, 555546L, 7051L));
		catalogsalesColumnStatisticsData.put("cs_list_price", new CatalogColumnStatisticsDataDouble(1.01D, 300.00D, 29566L, 7213L));
		catalogsalesColumnStatisticsData.put("cs_promo_sk", new CatalogColumnStatisticsDataLong(1L, 300L, 301L, 7234L));
		catalogsalesColumnStatisticsData.put("cs_ship_addr_sk", new CatalogColumnStatisticsDataLong(1L, 50000L, 47485L, 7165L));
		catalogsalesColumnStatisticsData.put("cs_net_paid_inc_ship", new CatalogColumnStatisticsDataDouble(0.00D, 41371.47D, 548346L, 0L));
		catalogsalesColumnStatisticsData.put("cs_ext_sales_price", new CatalogColumnStatisticsDataDouble(0.00D, 28846.00D, 396298L, 7208L));
		catalogsalesColumnStatisticsData.put("cs_ship_date_sk", new CatalogColumnStatisticsDataLong(2450817L, 2452738L, 1927L, 7147L));
		catalogsalesColumnStatisticsData.put("cs_ext_ship_cost", new CatalogColumnStatisticsDataDouble(0.00D, 14416.00D, 257236L, 7245L));
		catalogsalesColumnStatisticsData.put("cs_coupon_amt", new CatalogColumnStatisticsDataDouble(0.00D, 24188.55D, 156969L, 7185L));
		catalogsalesColumnStatisticsData.put("cs_bill_hdemo_sk", new CatalogColumnStatisticsDataLong(1L, 7200L, 7207L, 7060L));
		catalogsalesColumnStatisticsData.put("cs_sold_time_sk", new CatalogColumnStatisticsDataLong(4L, 86399L, 66950L, 7053L));
		final CatalogColumnStatistics catalogsalesCatalogColumnStatistics = new CatalogColumnStatistics(catalogsalesColumnStatisticsData);
		final CatalogTableStatistics catalogsalesCatalogTableStatistics = new CatalogTableStatistics(1441548, 0, 0, 0);
		final CatalogTableStats catalogsalesCatalogTableStats = new CatalogTableStats(catalogsalesCatalogTableStatistics, catalogsalesCatalogColumnStatistics);
		catalogTableStatsMap.put("catalog_sales", catalogsalesCatalogTableStats);

		//CatalogTableStats for table catalog_returns
		final Map<String, CatalogColumnStatisticsDataBase> catalogreturnsColumnStatisticsData = new HashMap<>();
		catalogreturnsColumnStatisticsData.put("cr_order_number", new CatalogColumnStatisticsDataLong(2L, 160000L, 93971L, 0L));
		catalogreturnsColumnStatisticsData.put("cr_return_amount", new CatalogColumnStatisticsDataDouble(0.00D, 24445.40D, 82653L, 2876L));
		catalogreturnsColumnStatisticsData.put("cr_refunded_addr_sk", new CatalogColumnStatisticsDataLong(1L, 50000L, 41875L, 2849L));
		catalogreturnsColumnStatisticsData.put("cr_net_loss", new CatalogColumnStatisticsDataDouble(1.01D, 13213.89D, 86796L, 2902L));
		catalogreturnsColumnStatisticsData.put("cr_return_amt_inc_tax", new CatalogColumnStatisticsDataDouble(0.00D, 25403.76D, 96483L, 2902L));
		catalogreturnsColumnStatisticsData.put("cr_catalog_page_sk", new CatalogColumnStatisticsDataLong(1L, 9828L, 6513L, 2877L));
		catalogreturnsColumnStatisticsData.put("cr_returning_customer_sk", new CatalogColumnStatisticsDataLong(1L, 99999L, 75260L, 2812L));
		catalogreturnsColumnStatisticsData.put("cr_store_credit", new CatalogColumnStatisticsDataDouble(0.00D, 16255.33D, 54851L, 2846L));
		catalogreturnsColumnStatisticsData.put("cr_returning_addr_sk", new CatalogColumnStatisticsDataLong(1L, 50000L, 46739L, 2844L));
		catalogreturnsColumnStatisticsData.put("cr_returning_hdemo_sk", new CatalogColumnStatisticsDataLong(1L, 7200L, 7207L, 2882L));
		catalogreturnsColumnStatisticsData.put("cr_fee", new CatalogColumnStatisticsDataDouble(0.50D, 100.00D, 9919L, 2873L));
		catalogreturnsColumnStatisticsData.put("cr_refunded_cash", new CatalogColumnStatisticsDataDouble(0.00D, 22110.68D, 73525L, 2849L));
		catalogreturnsColumnStatisticsData.put("cr_return_ship_cost", new CatalogColumnStatisticsDataDouble(0.00D, 11890.06D, 64215L, 2805L));
		catalogreturnsColumnStatisticsData.put("cr_returned_time_sk", new CatalogColumnStatisticsDataLong(2L, 86399L, 65102L, 0L));
		catalogreturnsColumnStatisticsData.put("cr_return_tax", new CatalogColumnStatisticsDataDouble(0.00D, 2062.43D, 25905L, 2847L));
		catalogreturnsColumnStatisticsData.put("cr_refunded_customer_sk", new CatalogColumnStatisticsDataLong(2L, 100000L, 60435L, 2819L));
		catalogreturnsColumnStatisticsData.put("cr_ship_mode_sk", new CatalogColumnStatisticsDataLong(1L, 20L, 20L, 2807L));
		catalogreturnsColumnStatisticsData.put("cr_refunded_cdemo_sk", new CatalogColumnStatisticsDataLong(5L, 1920733L, 91409L, 2819L));
		catalogreturnsColumnStatisticsData.put("cr_returned_date_sk", new CatalogColumnStatisticsDataLong(2450822L, 2452907L, 2066L, 0L));
		catalogreturnsColumnStatisticsData.put("cr_reversed_charge", new CatalogColumnStatisticsDataDouble(0.00D, 17158.23D, 54760L, 2842L));
		catalogreturnsColumnStatisticsData.put("cr_call_center_sk", new CatalogColumnStatisticsDataLong(1L, 6L, 6L, 2851L));
		catalogreturnsColumnStatisticsData.put("cr_item_sk", new CatalogColumnStatisticsDataLong(1L, 18000L, 17745L, 0L));
		catalogreturnsColumnStatisticsData.put("cr_refunded_hdemo_sk", new CatalogColumnStatisticsDataLong(1L, 7200L, 7207L, 2874L));
		catalogreturnsColumnStatisticsData.put("cr_warehouse_sk", new CatalogColumnStatisticsDataLong(1L, 5L, 5L, 2935L));
		catalogreturnsColumnStatisticsData.put("cr_returning_cdemo_sk", new CatalogColumnStatisticsDataLong(13L, 1920792L, 136751L, 2858L));
		catalogreturnsColumnStatisticsData.put("cr_return_quantity", new CatalogColumnStatisticsDataLong(1L, 100L, 100L, 2895L));
		catalogreturnsColumnStatisticsData.put("cr_reason_sk", new CatalogColumnStatisticsDataLong(1L, 35L, 35L, 2840L));
		final CatalogColumnStatistics catalogreturnsCatalogColumnStatistics = new CatalogColumnStatistics(catalogreturnsColumnStatisticsData);
		final CatalogTableStatistics catalogreturnsCatalogTableStatistics = new CatalogTableStatistics(144067, 0, 0, 0);
		final CatalogTableStats catalogreturnsCatalogTableStats = new CatalogTableStats(catalogreturnsCatalogTableStatistics, catalogreturnsCatalogColumnStatistics);
		catalogTableStatsMap.put("catalog_returns", catalogreturnsCatalogTableStats);

		//CatalogTableStats for table inventory
		final Map<String, CatalogColumnStatisticsDataBase> inventoryColumnStatisticsData = new HashMap<>();
		inventoryColumnStatisticsData.put("inv_date_sk", new CatalogColumnStatisticsDataLong(2450815L, 2452635L, 261L, 0L));
		inventoryColumnStatisticsData.put("inv_item_sk", new CatalogColumnStatisticsDataLong(1L, 18000L, 17869L, 0L));
		inventoryColumnStatisticsData.put("inv_warehouse_sk", new CatalogColumnStatisticsDataLong(1L, 5L, 5L, 0L));
		inventoryColumnStatisticsData.put("inv_quantity_on_hand", new CatalogColumnStatisticsDataLong(0L, 1000L, 1003L, 586913L));
		final CatalogColumnStatistics inventoryCatalogColumnStatistics = new CatalogColumnStatistics(inventoryColumnStatisticsData);
		final CatalogTableStatistics inventoryCatalogTableStatistics = new CatalogTableStatistics(11745000, 0, 0, 0);
		final CatalogTableStats inventoryCatalogTableStats = new CatalogTableStats(inventoryCatalogTableStatistics, inventoryCatalogColumnStatistics);
		catalogTableStatsMap.put("inventory", inventoryCatalogTableStats);

		//CatalogTableStats for table store_sales
		final Map<String, CatalogColumnStatisticsDataBase> storesalesColumnStatisticsData = new HashMap<>();
		storesalesColumnStatisticsData.put("ss_quantity", new CatalogColumnStatisticsDataLong(1L, 100L, 100L, 129996L));
		storesalesColumnStatisticsData.put("ss_wholesale_cost", new CatalogColumnStatisticsDataDouble(1.00D, 100.00D, 9872L, 130023L));
		storesalesColumnStatisticsData.put("ss_net_paid", new CatalogColumnStatisticsDataDouble(0.00D, 19562.40D, 461704L, 129397L));
		storesalesColumnStatisticsData.put("ss_net_profit", new CatalogColumnStatisticsDataDouble(-9969.53D, 9731.70D, 562950L, 130267L));
		storesalesColumnStatisticsData.put("ss_net_paid_inc_tax", new CatalogColumnStatisticsDataDouble(0.00D, 21192.87D, 607778L, 130022L));
		storesalesColumnStatisticsData.put("ss_sales_price", new CatalogColumnStatisticsDataDouble(0.00D, 199.56D, 18562L, 129666L));
		storesalesColumnStatisticsData.put("ss_ticket_number", new CatalogColumnStatisticsDataLong(1L, 240000L, 240186L, 0L));
		storesalesColumnStatisticsData.put("ss_ext_wholesale_cost", new CatalogColumnStatisticsDataDouble(1.00D, 10000.00D, 379276L, 130044L));
		storesalesColumnStatisticsData.put("ss_cdemo_sk", new CatalogColumnStatisticsDataLong(15L, 1920797L, 224219L, 129700L));
		storesalesColumnStatisticsData.put("ss_promo_sk", new CatalogColumnStatisticsDataLong(1L, 300L, 301L, 129484L));
		storesalesColumnStatisticsData.put("ss_sold_date_sk", new CatalogColumnStatisticsDataLong(2450816L, 2452642L, 1826L, 130093L));
		storesalesColumnStatisticsData.put("ss_ext_sales_price", new CatalogColumnStatisticsDataDouble(0.00D, 19562.40D, 408253L, 130327L));
		storesalesColumnStatisticsData.put("ss_sold_time_sk", new CatalogColumnStatisticsDataLong(28800L, 75599L, 45550L, 129637L));
		storesalesColumnStatisticsData.put("ss_customer_sk", new CatalogColumnStatisticsDataLong(1L, 100000L, 90766L, 129752L));
		storesalesColumnStatisticsData.put("ss_list_price", new CatalogColumnStatisticsDataDouble(1.00D, 200.00D, 19600L, 130003L));
		storesalesColumnStatisticsData.put("ss_store_sk", new CatalogColumnStatisticsDataLong(1L, 10L, 6L, 130034L));
		storesalesColumnStatisticsData.put("ss_item_sk", new CatalogColumnStatisticsDataLong(1L, 18000L, 17869L, 0L));
		storesalesColumnStatisticsData.put("ss_addr_sk", new CatalogColumnStatisticsDataLong(1L, 50000L, 49437L, 129975L));
		storesalesColumnStatisticsData.put("ss_ext_discount_amt", new CatalogColumnStatisticsDataDouble(0.00D, 17588.25D, 209393L, 129838L));
		storesalesColumnStatisticsData.put("ss_coupon_amt", new CatalogColumnStatisticsDataDouble(0.00D, 17588.25D, 209393L, 129838L));
		storesalesColumnStatisticsData.put("ss_ext_tax", new CatalogColumnStatisticsDataDouble(0.00D, 1749.87D, 78453L, 130410L));
		storesalesColumnStatisticsData.put("ss_hdemo_sk", new CatalogColumnStatisticsDataLong(1L, 7200L, 7207L, 129847L));
		storesalesColumnStatisticsData.put("ss_ext_list_price", new CatalogColumnStatisticsDataDouble(1.12D, 19984.00D, 576650L, 129933L));
		final CatalogColumnStatistics storesalesCatalogColumnStatistics = new CatalogColumnStatistics(storesalesColumnStatisticsData);
		final CatalogTableStatistics storesalesCatalogTableStatistics = new CatalogTableStatistics(2880404, 0, 0, 0);
		final CatalogTableStats storesalesCatalogTableStats = new CatalogTableStats(storesalesCatalogTableStatistics, storesalesCatalogColumnStatistics);
		catalogTableStatsMap.put("store_sales", storesalesCatalogTableStats);

		//CatalogTableStats for table store_returns
		final Map<String, CatalogColumnStatisticsDataBase> storereturnsColumnStatisticsData = new HashMap<>();
		storereturnsColumnStatisticsData.put("sr_return_time_sk", new CatalogColumnStatisticsDataLong(28799L, 61199L, 32528L, 9946L));
		storereturnsColumnStatisticsData.put("sr_return_ship_cost", new CatalogColumnStatisticsDataDouble(0.00D, 8631.06D, 82007L, 9871L));
		storereturnsColumnStatisticsData.put("sr_cdemo_sk", new CatalogColumnStatisticsDataLong(2L, 1920800L, 256899L, 9883L));
		storereturnsColumnStatisticsData.put("sr_store_credit", new CatalogColumnStatisticsDataDouble(0.00D, 11358.57D, 72114L, 10098L));
		storereturnsColumnStatisticsData.put("sr_return_amt_inc_tax", new CatalogColumnStatisticsDataDouble(0.00D, 17593.80D, 141989L, 9950L));
		storereturnsColumnStatisticsData.put("sr_item_sk", new CatalogColumnStatisticsDataLong(1L, 18000L, 17863L, 0L));
		storereturnsColumnStatisticsData.put("sr_return_quantity", new CatalogColumnStatisticsDataLong(1L, 100L, 100L, 10063L));
		storereturnsColumnStatisticsData.put("sr_net_loss", new CatalogColumnStatisticsDataDouble(0.51D, 9256.12D, 117103L, 9973L));
		storereturnsColumnStatisticsData.put("sr_ticket_number", new CatalogColumnStatisticsDataLong(1L, 240000L, 170291L, 0L));
		storereturnsColumnStatisticsData.put("sr_refunded_cash", new CatalogColumnStatisticsDataDouble(0.00D, 14887.06D, 101232L, 10040L));
		storereturnsColumnStatisticsData.put("sr_hdemo_sk", new CatalogColumnStatisticsDataLong(1L, 7200L, 7207L, 10121L));
		storereturnsColumnStatisticsData.put("sr_fee", new CatalogColumnStatisticsDataDouble(0.50D, 100.00D, 9919L, 10065L));
		storereturnsColumnStatisticsData.put("sr_addr_sk", new CatalogColumnStatisticsDataLong(1L, 50000L, 49651L, 10193L));
		storereturnsColumnStatisticsData.put("sr_returned_date_sk", new CatalogColumnStatisticsDataLong(2450820L, 2452822L, 2007L, 10012L));
		storereturnsColumnStatisticsData.put("sr_store_sk", new CatalogColumnStatisticsDataLong(1L, 10L, 6L, 10062L));
		storereturnsColumnStatisticsData.put("sr_return_tax", new CatalogColumnStatisticsDataDouble(0.00D, 1253.26D, 29420L, 10177L));
		storereturnsColumnStatisticsData.put("sr_customer_sk", new CatalogColumnStatisticsDataLong(1L, 100000L, 86887L, 10016L));
		storereturnsColumnStatisticsData.put("sr_reason_sk", new CatalogColumnStatisticsDataLong(1L, 35L, 35L, 9984L));
		storereturnsColumnStatisticsData.put("sr_reversed_charge", new CatalogColumnStatisticsDataDouble(0.00D, 11531.04D, 72486L, 10032L));
		storereturnsColumnStatisticsData.put("sr_return_amt", new CatalogColumnStatisticsDataDouble(0.00D, 16917.12D, 111419L, 10028L));
		final CatalogColumnStatistics storereturnsCatalogColumnStatistics = new CatalogColumnStatistics(storereturnsColumnStatisticsData);
		final CatalogTableStatistics storereturnsCatalogTableStatistics = new CatalogTableStatistics(287514, 0, 0, 0);
		final CatalogTableStats storereturnsCatalogTableStats = new CatalogTableStats(storereturnsCatalogTableStatistics, storereturnsCatalogColumnStatistics);
		catalogTableStatsMap.put("store_returns", storereturnsCatalogTableStats);

		//CatalogTableStats for table web_sales
		final Map<String, CatalogColumnStatisticsDataBase> websalesColumnStatisticsData = new HashMap<>();
		websalesColumnStatisticsData.put("ws_ship_mode_sk", new CatalogColumnStatisticsDataLong(1L, 20L, 20L, 174L));
		websalesColumnStatisticsData.put("ws_net_paid_inc_ship", new CatalogColumnStatisticsDataDouble(0.00D, 41222.09D, 375366L, 0L));
		websalesColumnStatisticsData.put("ws_coupon_amt", new CatalogColumnStatisticsDataDouble(0.00D, 26909.62D, 95986L, 196L));
		websalesColumnStatisticsData.put("ws_ext_discount_amt", new CatalogColumnStatisticsDataDouble(0.00D, 29317.00D, 280666L, 179L));
		websalesColumnStatisticsData.put("ws_order_number", new CatalogColumnStatisticsDataLong(1L, 60000L, 60008L, 0L));
		websalesColumnStatisticsData.put("ws_ext_list_price", new CatalogColumnStatisticsDataDouble(1.11D, 29424.00D, 387131L, 180L));
		websalesColumnStatisticsData.put("ws_bill_customer_sk", new CatalogColumnStatisticsDataLong(2L, 100000L, 45622L, 167L));
		websalesColumnStatisticsData.put("ws_promo_sk", new CatalogColumnStatisticsDataLong(1L, 300L, 301L, 184L));
		websalesColumnStatisticsData.put("ws_net_profit", new CatalogColumnStatisticsDataDouble(-9938.00D, 18864.56D, 335187L, 0L));
		websalesColumnStatisticsData.put("ws_net_paid", new CatalogColumnStatisticsDataDouble(0.00D, 28538.37D, 298736L, 169L));
		websalesColumnStatisticsData.put("ws_item_sk", new CatalogColumnStatisticsDataLong(1L, 18000L, 17869L, 0L));
		websalesColumnStatisticsData.put("ws_net_paid_inc_ship_tax", new CatalogColumnStatisticsDataDouble(0.00D, 43505.15D, 457306L, 0L));
		websalesColumnStatisticsData.put("ws_web_page_sk", new CatalogColumnStatisticsDataLong(1L, 60L, 60L, 188L));
		websalesColumnStatisticsData.put("ws_warehouse_sk", new CatalogColumnStatisticsDataLong(1L, 5L, 5L, 179L));
		websalesColumnStatisticsData.put("ws_ship_cdemo_sk", new CatalogColumnStatisticsDataLong(12L, 1920716L, 59495L, 171L));
		websalesColumnStatisticsData.put("ws_sold_date_sk", new CatalogColumnStatisticsDataLong(2450816L, 2452642L, 1826L, 189L));
		websalesColumnStatisticsData.put("ws_list_price", new CatalogColumnStatisticsDataDouble(1.01D, 300.00D, 29151L, 184L));
		websalesColumnStatisticsData.put("ws_sold_time_sk", new CatalogColumnStatisticsDataLong(7L, 86367L, 40483L, 187L));
		websalesColumnStatisticsData.put("ws_ext_ship_cost", new CatalogColumnStatisticsDataDouble(0.00D, 14231.25D, 198497L, 198L));
		websalesColumnStatisticsData.put("ws_quantity", new CatalogColumnStatisticsDataLong(1L, 100L, 100L, 182L));
		websalesColumnStatisticsData.put("ws_ship_hdemo_sk", new CatalogColumnStatisticsDataLong(1L, 7200L, 7204L, 165L));
		websalesColumnStatisticsData.put("ws_ext_tax", new CatalogColumnStatisticsDataDouble(0.00D, 2433.41D, 67989L, 184L));
		websalesColumnStatisticsData.put("ws_sales_price", new CatalogColumnStatisticsDataDouble(0.00D, 299.16D, 24557L, 167L));
		websalesColumnStatisticsData.put("ws_ext_wholesale_cost", new CatalogColumnStatisticsDataDouble(1.00D, 10000.00D, 272683L, 197L));
		websalesColumnStatisticsData.put("ws_bill_cdemo_sk", new CatalogColumnStatisticsDataLong(31L, 1920747L, 58779L, 165L));
		websalesColumnStatisticsData.put("ws_net_paid_inc_tax", new CatalogColumnStatisticsDataDouble(0.00D, 30821.43D, 363455L, 175L));
		websalesColumnStatisticsData.put("ws_ship_addr_sk", new CatalogColumnStatisticsDataLong(1L, 50000L, 35376L, 178L));
		websalesColumnStatisticsData.put("ws_bill_addr_sk", new CatalogColumnStatisticsDataLong(1L, 50000L, 34581L, 172L));
		websalesColumnStatisticsData.put("ws_ship_date_sk", new CatalogColumnStatisticsDataLong(2450817L, 2452762L, 1952L, 177L));
		websalesColumnStatisticsData.put("ws_bill_hdemo_sk", new CatalogColumnStatisticsDataLong(1L, 7200L, 7206L, 183L));
		websalesColumnStatisticsData.put("ws_ship_customer_sk", new CatalogColumnStatisticsDataLong(2L, 99999L, 44890L, 173L));
		websalesColumnStatisticsData.put("ws_ext_sales_price", new CatalogColumnStatisticsDataDouble(0.00D, 28592.19D, 283961L, 181L));
		websalesColumnStatisticsData.put("ws_wholesale_cost", new CatalogColumnStatisticsDataDouble(1.00D, 100.00D, 9872L, 173L));
		websalesColumnStatisticsData.put("ws_web_site_sk", new CatalogColumnStatisticsDataLong(1L, 30L, 30L, 186L));
		final CatalogColumnStatistics websalesCatalogColumnStatistics = new CatalogColumnStatistics(websalesColumnStatisticsData);
		final CatalogTableStatistics websalesCatalogTableStatistics = new CatalogTableStatistics(719384, 0, 0, 0);
		final CatalogTableStats websalesCatalogTableStats = new CatalogTableStats(websalesCatalogTableStatistics, websalesCatalogColumnStatistics);
		catalogTableStatsMap.put("web_sales", websalesCatalogTableStats);

		//CatalogTableStats for table web_returns
		final Map<String, CatalogColumnStatisticsDataBase> webreturnsColumnStatisticsData = new HashMap<>();
		webreturnsColumnStatisticsData.put("wr_reason_sk", new CatalogColumnStatisticsDataLong(1L, 35L, 35L, 3212L));
		webreturnsColumnStatisticsData.put("wr_fee", new CatalogColumnStatisticsDataDouble(0.50D, 100.00D, 9914L, 3165L));
		webreturnsColumnStatisticsData.put("wr_return_amt", new CatalogColumnStatisticsDataDouble(0.00D, 25649.40D, 49033L, 3211L));
		webreturnsColumnStatisticsData.put("wr_returning_addr_sk", new CatalogColumnStatisticsDataLong(2L, 50000L, 36910L, 3218L));
		webreturnsColumnStatisticsData.put("wr_returned_time_sk", new CatalogColumnStatisticsDataLong(0L, 86399L, 44393L, 3194L));
		webreturnsColumnStatisticsData.put("wr_returning_cdemo_sk", new CatalogColumnStatisticsDataLong(44L, 1920789L, 67240L, 3215L));
		webreturnsColumnStatisticsData.put("wr_web_page_sk", new CatalogColumnStatisticsDataLong(1L, 60L, 60L, 3130L));
		webreturnsColumnStatisticsData.put("wr_return_tax", new CatalogColumnStatisticsDataDouble(0.00D, 2090.05D, 18191L, 3257L));
		webreturnsColumnStatisticsData.put("wr_refunded_customer_sk", new CatalogColumnStatisticsDataLong(1L, 100000L, 49111L, 3217L));
		webreturnsColumnStatisticsData.put("wr_return_ship_cost", new CatalogColumnStatisticsDataDouble(0.00D, 11730.85D, 40436L, 3187L));
		webreturnsColumnStatisticsData.put("wr_refunded_hdemo_sk", new CatalogColumnStatisticsDataLong(1L, 7200L, 7207L, 3270L));
		webreturnsColumnStatisticsData.put("wr_account_credit", new CatalogColumnStatisticsDataDouble(0.00D, 13995.91D, 34030L, 3151L));
		webreturnsColumnStatisticsData.put("wr_returned_date_sk", new CatalogColumnStatisticsDataLong(2450842L, 2452995L, 2122L, 3127L));
		webreturnsColumnStatisticsData.put("wr_reversed_charge", new CatalogColumnStatisticsDataDouble(0.00D, 16140.11D, 33918L, 3210L));
		webreturnsColumnStatisticsData.put("wr_order_number", new CatalogColumnStatisticsDataLong(1L, 59997L, 42084L, 0L));
		webreturnsColumnStatisticsData.put("wr_returning_customer_sk", new CatalogColumnStatisticsDataLong(1L, 100000L, 49140L, 3138L));
		webreturnsColumnStatisticsData.put("wr_return_amt_inc_tax", new CatalogColumnStatisticsDataDouble(0.00D, 26675.37D, 54783L, 3193L));
		webreturnsColumnStatisticsData.put("wr_returning_hdemo_sk", new CatalogColumnStatisticsDataLong(1L, 7200L, 7207L, 3205L));
		webreturnsColumnStatisticsData.put("wr_item_sk", new CatalogColumnStatisticsDataLong(1L, 18000L, 16991L, 0L));
		webreturnsColumnStatisticsData.put("wr_return_quantity", new CatalogColumnStatisticsDataLong(1L, 100L, 100L, 3147L));
		webreturnsColumnStatisticsData.put("wr_refunded_cash", new CatalogColumnStatisticsDataDouble(0.00D, 20822.97D, 44003L, 3181L));
		webreturnsColumnStatisticsData.put("wr_refunded_addr_sk", new CatalogColumnStatisticsDataLong(2L, 50000L, 37063L, 3164L));
		webreturnsColumnStatisticsData.put("wr_refunded_cdemo_sk", new CatalogColumnStatisticsDataLong(44L, 1920789L, 67243L, 3230L));
		webreturnsColumnStatisticsData.put("wr_net_loss", new CatalogColumnStatisticsDataDouble(0.63D, 13871.85D, 51848L, 3201L));
		final CatalogColumnStatistics webreturnsCatalogColumnStatistics = new CatalogColumnStatistics(webreturnsColumnStatisticsData);
		final CatalogTableStatistics webreturnsCatalogTableStatistics = new CatalogTableStatistics(71763, 0, 0, 0);
		final CatalogTableStats webreturnsCatalogTableStats = new CatalogTableStats(webreturnsCatalogTableStatistics, webreturnsCatalogColumnStatistics);
		catalogTableStatsMap.put("web_returns", webreturnsCatalogTableStats);

		//CatalogTableStats for table call_center
		final Map<String, CatalogColumnStatisticsDataBase> callcenterColumnStatisticsData = new HashMap<>();
		callcenterColumnStatisticsData.put("cc_street_number", new CatalogColumnStatisticsDataString(3L, 3.0D, 3L, 0L));
		callcenterColumnStatisticsData.put("cc_call_center_id", new CatalogColumnStatisticsDataString(16L, 16.0D, 3L, 0L));
		callcenterColumnStatisticsData.put("cc_state", new CatalogColumnStatisticsDataString(2L, 2.0D, 1L, 0L));
		callcenterColumnStatisticsData.put("cc_tax_percentage", new CatalogColumnStatisticsDataDouble(0.01D, 0.12D, 4L, 0L));
		callcenterColumnStatisticsData.put("cc_division_name", new CatalogColumnStatisticsDataString(5L, 3.6666666666666665D, 4L, 0L));
		callcenterColumnStatisticsData.put("cc_hours", new CatalogColumnStatisticsDataString(7L, 7.0D, 2L, 0L));
		callcenterColumnStatisticsData.put("cc_manager", new CatalogColumnStatisticsDataString(14L, 12.5D, 4L, 0L));
		callcenterColumnStatisticsData.put("cc_name", new CatalogColumnStatisticsDataString(13L, 11.833333333333334D, 3L, 0L));
		callcenterColumnStatisticsData.put("cc_employees", new CatalogColumnStatisticsDataLong(1L, 7L, 5L, 0L));
		callcenterColumnStatisticsData.put("cc_mkt_id", new CatalogColumnStatisticsDataLong(2L, 6L, 3L, 0L));
		callcenterColumnStatisticsData.put("cc_class", new CatalogColumnStatisticsDataString(6L, 5.666666666666667D, 3L, 0L));
		callcenterColumnStatisticsData.put("cc_division", new CatalogColumnStatisticsDataLong(1L, 5L, 4L, 0L));
		callcenterColumnStatisticsData.put("cc_street_name", new CatalogColumnStatisticsDataString(11L, 10.0D, 3L, 0L));
		callcenterColumnStatisticsData.put("cc_rec_end_date", new CatalogColumnStatisticsDataDate(null, null, 3L, 3L));
		callcenterColumnStatisticsData.put("cc_gmt_offset", new CatalogColumnStatisticsDataDouble(-5.00D, -5.00D, 1L, 0L));
		callcenterColumnStatisticsData.put("cc_market_manager", new CatalogColumnStatisticsDataString(15L, 12.666666666666666D, 4L, 0L));
		callcenterColumnStatisticsData.put("cc_open_date_sk", new CatalogColumnStatisticsDataLong(2450806L, 2451063L, 3L, 0L));
		callcenterColumnStatisticsData.put("cc_country", new CatalogColumnStatisticsDataString(13L, 13.0D, 1L, 0L));
		callcenterColumnStatisticsData.put("cc_closed_date_sk", new CatalogColumnStatisticsDataLong(0L, 0L, 0L, 6L));
		callcenterColumnStatisticsData.put("cc_company_name", new CatalogColumnStatisticsDataString(5L, 3.8333333333333335D, 4L, 0L));
		callcenterColumnStatisticsData.put("cc_city", new CatalogColumnStatisticsDataString(6L, 6.0D, 1L, 0L));
		callcenterColumnStatisticsData.put("cc_company", new CatalogColumnStatisticsDataLong(1L, 6L, 4L, 0L));
		callcenterColumnStatisticsData.put("cc_county", new CatalogColumnStatisticsDataString(17L, 17.0D, 1L, 0L));
		callcenterColumnStatisticsData.put("cc_rec_start_date", new CatalogColumnStatisticsDataDate(null, null, 4L, 0L));
		callcenterColumnStatisticsData.put("cc_street_type", new CatalogColumnStatisticsDataString(9L, 3.5D, 3L, 0L));
		callcenterColumnStatisticsData.put("cc_sq_ft", new CatalogColumnStatisticsDataLong(649L, 4134L, 6L, 0L));
		callcenterColumnStatisticsData.put("cc_mkt_class", new CatalogColumnStatisticsDataString(45L, 37.166666666666664D, 5L, 0L));
		callcenterColumnStatisticsData.put("cc_zip", new CatalogColumnStatisticsDataString(5L, 5.0D, 1L, 0L));
		callcenterColumnStatisticsData.put("cc_mkt_desc", new CatalogColumnStatisticsDataString(92L, 69.33333333333333D, 4L, 0L));
		callcenterColumnStatisticsData.put("cc_suite_number", new CatalogColumnStatisticsDataString(8L, 7.333333333333333D, 3L, 0L));
		callcenterColumnStatisticsData.put("cc_call_center_sk", new CatalogColumnStatisticsDataLong(1L, 6L, 6L, 0L));
		final CatalogColumnStatistics callcenterCatalogColumnStatistics = new CatalogColumnStatistics(callcenterColumnStatisticsData);
		final CatalogTableStatistics callcenterCatalogTableStatistics = new CatalogTableStatistics(6, 0, 0, 0);
		final CatalogTableStats callcenterCatalogTableStats = new CatalogTableStats(callcenterCatalogTableStatistics, callcenterCatalogColumnStatistics);
		catalogTableStatsMap.put("call_center", callcenterCatalogTableStats);

		//CatalogTableStats for table catalog_page
		final Map<String, CatalogColumnStatisticsDataBase> catalogpageColumnStatisticsData = new HashMap<>();
		catalogpageColumnStatisticsData.put("cp_department", new CatalogColumnStatisticsDataString(10L, 9.897593445980542D, 2L, 0L));
		catalogpageColumnStatisticsData.put("cp_catalog_page_number", new CatalogColumnStatisticsDataLong(1L, 108L, 108L, 116L));
		catalogpageColumnStatisticsData.put("cp_catalog_page_sk", new CatalogColumnStatisticsDataLong(1L, 11718L, 11692L, 0L));
		catalogpageColumnStatisticsData.put("cp_end_date_sk", new CatalogColumnStatisticsDataLong(2450844L, 2453186L, 96L, 108L));
		catalogpageColumnStatisticsData.put("cp_catalog_page_id", new CatalogColumnStatisticsDataString(16L, 16.0D, 11829L, 0L));
		catalogpageColumnStatisticsData.put("cp_description", new CatalogColumnStatisticsDataString(99L, 73.86396996074416D, 11657L, 0L));
		catalogpageColumnStatisticsData.put("cp_type", new CatalogColumnStatisticsDataString(9L, 7.600443761734084D, 4L, 0L));
		catalogpageColumnStatisticsData.put("cp_start_date_sk", new CatalogColumnStatisticsDataLong(2450815L, 2453005L, 91L, 101L));
		catalogpageColumnStatisticsData.put("cp_catalog_number", new CatalogColumnStatisticsDataLong(1L, 109L, 109L, 104L));
		final CatalogColumnStatistics catalogpageCatalogColumnStatistics = new CatalogColumnStatistics(catalogpageColumnStatisticsData);
		final CatalogTableStatistics catalogpageCatalogTableStatistics = new CatalogTableStatistics(11718, 0, 0, 0);
		final CatalogTableStats catalogpageCatalogTableStats = new CatalogTableStats(catalogpageCatalogTableStatistics, catalogpageCatalogColumnStatistics);
		catalogTableStatsMap.put("catalog_page", catalogpageCatalogTableStats);

		//CatalogTableStats for table customer
		final Map<String, CatalogColumnStatisticsDataBase> customerColumnStatisticsData = new HashMap<>();
		customerColumnStatisticsData.put("c_email_address", new CatalogColumnStatisticsDataString(46L, 26.48564D, 98193L, 0L));
		customerColumnStatisticsData.put("c_birth_month", new CatalogColumnStatisticsDataLong(1L, 12L, 12L, 3449L));
		customerColumnStatisticsData.put("c_first_sales_date_sk", new CatalogColumnStatisticsDataLong(2448998L, 2452648L, 3623L, 3518L));
		customerColumnStatisticsData.put("c_customer_id", new CatalogColumnStatisticsDataString(16L, 16.0D, 101081L, 0L));
		customerColumnStatisticsData.put("c_birth_country", new CatalogColumnStatisticsDataString(20L, 8.40023D, 208L, 0L));
		customerColumnStatisticsData.put("c_last_review_date_sk", new CatalogColumnStatisticsDataLong(2452283L, 2452648L, 365L, 3484L));
		customerColumnStatisticsData.put("c_salutation", new CatalogColumnStatisticsDataString(4L, 3.1309D, 7L, 0L));
		customerColumnStatisticsData.put("c_last_name", new CatalogColumnStatisticsDataString(13L, 5.91061D, 4986L, 0L));
		customerColumnStatisticsData.put("c_birth_day", new CatalogColumnStatisticsDataLong(1L, 31L, 31L, 3461L));
		customerColumnStatisticsData.put("c_current_cdemo_sk", new CatalogColumnStatisticsDataLong(17L, 1920788L, 94253L, 3438L));
		customerColumnStatisticsData.put("c_login", new CatalogColumnStatisticsDataString(0L, 0.0D, 1L, 0L));
		customerColumnStatisticsData.put("c_first_shipto_date_sk", new CatalogColumnStatisticsDataLong(2449028L, 2452678L, 3624L, 3443L));
		customerColumnStatisticsData.put("c_current_addr_sk", new CatalogColumnStatisticsDataLong(1L, 50000L, 42913L, 0L));
		customerColumnStatisticsData.put("c_birth_year", new CatalogColumnStatisticsDataLong(1924L, 1992L, 69L, 3453L));
		customerColumnStatisticsData.put("c_preferred_cust_flag", new CatalogColumnStatisticsDataString(1L, 0.96574D, 3L, 0L));
		customerColumnStatisticsData.put("c_current_hdemo_sk", new CatalogColumnStatisticsDataLong(1L, 7200L, 7207L, 3431L));
		customerColumnStatisticsData.put("c_customer_sk", new CatalogColumnStatisticsDataLong(1L, 100000L, 100425L, 0L));
		customerColumnStatisticsData.put("c_first_name", new CatalogColumnStatisticsDataString(11L, 5.63556D, 4134L, 0L));
		final CatalogColumnStatistics customerCatalogColumnStatistics = new CatalogColumnStatistics(customerColumnStatisticsData);
		final CatalogTableStatistics customerCatalogTableStatistics = new CatalogTableStatistics(100000, 0, 0, 0);
		final CatalogTableStats customerCatalogTableStats = new CatalogTableStats(customerCatalogTableStatistics, customerCatalogColumnStatistics);
		catalogTableStatsMap.put("customer", customerCatalogTableStats);

		//CatalogTableStats for table customer_address
		final Map<String, CatalogColumnStatisticsDataBase> customeraddressColumnStatisticsData = new HashMap<>();
		customeraddressColumnStatisticsData.put("ca_state", new CatalogColumnStatisticsDataString(2L, 1.938D, 52L, 0L));
		customeraddressColumnStatisticsData.put("ca_street_type", new CatalogColumnStatisticsDataString(9L, 4.0803D, 21L, 0L));
		customeraddressColumnStatisticsData.put("ca_gmt_offset", new CatalogColumnStatisticsDataDouble(-10.00D, -5.00D, 6L, 1556L));
		customeraddressColumnStatisticsData.put("ca_location_type", new CatalogColumnStatisticsDataString(13L, 8.72968D, 4L, 0L));
		customeraddressColumnStatisticsData.put("ca_street_number", new CatalogColumnStatisticsDataString(4L, 2.80616D, 1005L, 0L));
		customeraddressColumnStatisticsData.put("ca_address_id", new CatalogColumnStatisticsDataString(16L, 16.0D, 50363L, 0L));
		customeraddressColumnStatisticsData.put("ca_suite_number", new CatalogColumnStatisticsDataString(9L, 7.64982D, 76L, 0L));
		customeraddressColumnStatisticsData.put("ca_country", new CatalogColumnStatisticsDataString(13L, 12.6035D, 2L, 0L));
		customeraddressColumnStatisticsData.put("ca_zip", new CatalogColumnStatisticsDataString(5L, 4.8461D, 3667L, 0L));
		customeraddressColumnStatisticsData.put("ca_address_sk", new CatalogColumnStatisticsDataLong(1L, 50000L, 49816L, 0L));
		customeraddressColumnStatisticsData.put("ca_county", new CatalogColumnStatisticsDataString(28L, 13.55138D, 1856L, 0L));
		customeraddressColumnStatisticsData.put("ca_city", new CatalogColumnStatisticsDataString(20L, 8.6713D, 690L, 0L));
		customeraddressColumnStatisticsData.put("ca_street_name", new CatalogColumnStatisticsDataString(21L, 8.45012D, 6955L, 0L));
		final CatalogColumnStatistics customeraddressCatalogColumnStatistics = new CatalogColumnStatistics(customeraddressColumnStatisticsData);
		final CatalogTableStatistics customeraddressCatalogTableStatistics = new CatalogTableStatistics(50000, 0, 0, 0);
		final CatalogTableStats customeraddressCatalogTableStats = new CatalogTableStats(customeraddressCatalogTableStatistics, customeraddressCatalogColumnStatistics);
		catalogTableStatsMap.put("customer_address", customeraddressCatalogTableStats);

		//CatalogTableStats for table customer_demographics
		final Map<String, CatalogColumnStatisticsDataBase> customerdemographicsColumnStatisticsData = new HashMap<>();
		customerdemographicsColumnStatisticsData.put("cd_demo_sk", new CatalogColumnStatisticsDataLong(1L, 1920800L, 1913326L, 0L));
		customerdemographicsColumnStatisticsData.put("cd_education_status", new CatalogColumnStatisticsDataString(15L, 9.571428571428571D, 7L, 0L));
		customerdemographicsColumnStatisticsData.put("cd_credit_rating", new CatalogColumnStatisticsDataString(9L, 7.0D, 4L, 0L));
		customerdemographicsColumnStatisticsData.put("cd_purchase_estimate", new CatalogColumnStatisticsDataLong(500L, 10000L, 20L, 0L));
		customerdemographicsColumnStatisticsData.put("cd_dep_college_count", new CatalogColumnStatisticsDataLong(0L, 6L, 7L, 0L));
		customerdemographicsColumnStatisticsData.put("cd_dep_count", new CatalogColumnStatisticsDataLong(0L, 6L, 7L, 0L));
		customerdemographicsColumnStatisticsData.put("cd_gender", new CatalogColumnStatisticsDataString(1L, 1.0D, 2L, 0L));
		customerdemographicsColumnStatisticsData.put("cd_dep_employed_count", new CatalogColumnStatisticsDataLong(0L, 6L, 7L, 0L));
		customerdemographicsColumnStatisticsData.put("cd_marital_status", new CatalogColumnStatisticsDataString(1L, 1.0D, 5L, 0L));
		final CatalogColumnStatistics customerdemographicsCatalogColumnStatistics = new CatalogColumnStatistics(customerdemographicsColumnStatisticsData);
		final CatalogTableStatistics customerdemographicsCatalogTableStatistics = new CatalogTableStatistics(1920800, 0, 0, 0);
		final CatalogTableStats customerdemographicsCatalogTableStats = new CatalogTableStats(customerdemographicsCatalogTableStatistics, customerdemographicsCatalogColumnStatistics);
		catalogTableStatsMap.put("customer_demographics", customerdemographicsCatalogTableStats);

		//CatalogTableStats for table date_dim
		final Map<String, CatalogColumnStatisticsDataBase> datedimColumnStatisticsData = new HashMap<>();
		datedimColumnStatisticsData.put("d_same_day_lq", new CatalogColumnStatisticsDataLong(2414930L, 2487978L, 72576L, 0L));
		datedimColumnStatisticsData.put("d_holiday", new CatalogColumnStatisticsDataString(1L, 1.0D, 2L, 0L));
		datedimColumnStatisticsData.put("d_date", new CatalogColumnStatisticsDataDate(null, null, 73063L, 0L));
		datedimColumnStatisticsData.put("d_current_week", new CatalogColumnStatisticsDataString(1L, 1.0D, 1L, 0L));
		datedimColumnStatisticsData.put("d_current_day", new CatalogColumnStatisticsDataString(1L, 1.0D, 1L, 0L));
		datedimColumnStatisticsData.put("d_week_seq", new CatalogColumnStatisticsDataLong(1L, 10436L, 10486L, 0L));
		datedimColumnStatisticsData.put("d_day_name", new CatalogColumnStatisticsDataString(9L, 7.142863009760572D, 7L, 0L));
		datedimColumnStatisticsData.put("d_year", new CatalogColumnStatisticsDataLong(1900L, 2100L, 201L, 0L));
		datedimColumnStatisticsData.put("d_date_id", new CatalogColumnStatisticsDataString(16L, 16.0D, 73069L, 0L));
		datedimColumnStatisticsData.put("d_first_dom", new CatalogColumnStatisticsDataLong(2415021L, 2488070L, 2392L, 0L));
		datedimColumnStatisticsData.put("d_moy", new CatalogColumnStatisticsDataLong(1L, 12L, 12L, 0L));
		datedimColumnStatisticsData.put("d_current_month", new CatalogColumnStatisticsDataString(1L, 1.0D, 2L, 0L));
		datedimColumnStatisticsData.put("d_quarter_name", new CatalogColumnStatisticsDataString(6L, 6.0D, 797L, 0L));
		datedimColumnStatisticsData.put("d_same_day_ly", new CatalogColumnStatisticsDataLong(2414657L, 2487705L, 72914L, 0L));
		datedimColumnStatisticsData.put("d_fy_year", new CatalogColumnStatisticsDataLong(1900L, 2100L, 201L, 0L));
		datedimColumnStatisticsData.put("d_following_holiday", new CatalogColumnStatisticsDataString(1L, 1.0D, 2L, 0L));
		datedimColumnStatisticsData.put("d_weekend", new CatalogColumnStatisticsDataString(1L, 1.0D, 2L, 0L));
		datedimColumnStatisticsData.put("d_fy_week_seq", new CatalogColumnStatisticsDataLong(1L, 10436L, 10486L, 0L));
		datedimColumnStatisticsData.put("d_current_year", new CatalogColumnStatisticsDataString(1L, 1.0D, 2L, 0L));
		datedimColumnStatisticsData.put("d_date_sk", new CatalogColumnStatisticsDataLong(2415022L, 2488070L, 72895L, 0L));
		datedimColumnStatisticsData.put("d_qoy", new CatalogColumnStatisticsDataLong(1L, 4L, 4L, 0L));
		datedimColumnStatisticsData.put("d_dow", new CatalogColumnStatisticsDataLong(0L, 6L, 7L, 0L));
		datedimColumnStatisticsData.put("d_fy_quarter_seq", new CatalogColumnStatisticsDataLong(1L, 801L, 802L, 0L));
		datedimColumnStatisticsData.put("d_quarter_seq", new CatalogColumnStatisticsDataLong(1L, 801L, 802L, 0L));
		datedimColumnStatisticsData.put("d_last_dom", new CatalogColumnStatisticsDataLong(2415020L, 2488372L, 2409L, 0L));
		datedimColumnStatisticsData.put("d_current_quarter", new CatalogColumnStatisticsDataString(1L, 1.0D, 2L, 0L));
		datedimColumnStatisticsData.put("d_month_seq", new CatalogColumnStatisticsDataLong(0L, 2400L, 2420L, 0L));
		datedimColumnStatisticsData.put("d_dom", new CatalogColumnStatisticsDataLong(1L, 31L, 31L, 0L));
		final CatalogColumnStatistics datedimCatalogColumnStatistics = new CatalogColumnStatistics(datedimColumnStatisticsData);
		final CatalogTableStatistics datedimCatalogTableStatistics = new CatalogTableStatistics(73049, 0, 0, 0);
		final CatalogTableStats datedimCatalogTableStats = new CatalogTableStats(datedimCatalogTableStatistics, datedimCatalogColumnStatistics);
		catalogTableStatsMap.put("date_dim", datedimCatalogTableStats);

		//CatalogTableStats for table household_demographics
		final Map<String, CatalogColumnStatisticsDataBase> householddemographicsColumnStatisticsData = new HashMap<>();
		householddemographicsColumnStatisticsData.put("hd_demo_sk", new CatalogColumnStatisticsDataLong(1L, 7200L, 7207L, 0L));
		householddemographicsColumnStatisticsData.put("hd_income_band_sk", new CatalogColumnStatisticsDataLong(1L, 20L, 20L, 0L));
		householddemographicsColumnStatisticsData.put("hd_dep_count", new CatalogColumnStatisticsDataLong(0L, 9L, 10L, 0L));
		householddemographicsColumnStatisticsData.put("hd_buy_potential", new CatalogColumnStatisticsDataString(10L, 7.5D, 6L, 0L));
		householddemographicsColumnStatisticsData.put("hd_vehicle_count", new CatalogColumnStatisticsDataLong(-1L, 4L, 6L, 0L));
		final CatalogColumnStatistics householddemographicsCatalogColumnStatistics = new CatalogColumnStatistics(householddemographicsColumnStatisticsData);
		final CatalogTableStatistics householddemographicsCatalogTableStatistics = new CatalogTableStatistics(7200, 0, 0, 0);
		final CatalogTableStats householddemographicsCatalogTableStats = new CatalogTableStats(householddemographicsCatalogTableStatistics, householddemographicsCatalogColumnStatistics);
		catalogTableStatsMap.put("household_demographics", householddemographicsCatalogTableStats);

		//CatalogTableStats for table income_band
		final Map<String, CatalogColumnStatisticsDataBase> incomebandColumnStatisticsData = new HashMap<>();
		incomebandColumnStatisticsData.put("ib_income_band_sk", new CatalogColumnStatisticsDataLong(1L, 20L, 20L, 0L));
		incomebandColumnStatisticsData.put("ib_lower_bound", new CatalogColumnStatisticsDataLong(0L, 190001L, 20L, 0L));
		incomebandColumnStatisticsData.put("ib_upper_bound", new CatalogColumnStatisticsDataLong(10000L, 200000L, 20L, 0L));
		final CatalogColumnStatistics incomebandCatalogColumnStatistics = new CatalogColumnStatistics(incomebandColumnStatisticsData);
		final CatalogTableStatistics incomebandCatalogTableStatistics = new CatalogTableStatistics(20, 0, 0, 0);
		final CatalogTableStats incomebandCatalogTableStats = new CatalogTableStats(incomebandCatalogTableStatistics, incomebandCatalogColumnStatistics);
		catalogTableStatsMap.put("income_band", incomebandCatalogTableStats);

		//CatalogTableStats for table item
		final Map<String, CatalogColumnStatisticsDataBase> itemColumnStatisticsData = new HashMap<>();
		itemColumnStatisticsData.put("i_units", new CatalogColumnStatisticsDataString(7L, 4.182111111111111D, 22L, 0L));
		itemColumnStatisticsData.put("i_brand", new CatalogColumnStatisticsDataString(22L, 16.160722222222223D, 709L, 0L));
		itemColumnStatisticsData.put("i_rec_start_date", new CatalogColumnStatisticsDataDate(null, null, 4L, 46L));
		itemColumnStatisticsData.put("i_rec_end_date", new CatalogColumnStatisticsDataDate(null, null, 3L, 9000L));
		itemColumnStatisticsData.put("i_manufact_id", new CatalogColumnStatisticsDataLong(1L, 1000L, 997L, 38L));
		itemColumnStatisticsData.put("i_manager_id", new CatalogColumnStatisticsDataLong(1L, 100L, 100L, 41L));
		itemColumnStatisticsData.put("i_container", new CatalogColumnStatisticsDataString(7L, 6.9848333333333334D, 2L, 0L));
		itemColumnStatisticsData.put("i_formulation", new CatalogColumnStatisticsDataString(20L, 19.952222222222222D, 13661L, 0L));
		itemColumnStatisticsData.put("i_product_name", new CatalogColumnStatisticsDataString(25L, 18.002777777777776D, 17974L, 0L));
		itemColumnStatisticsData.put("i_item_desc", new CatalogColumnStatisticsDataString(200L, 100.15033333333334D, 13410L, 0L));
		itemColumnStatisticsData.put("i_category_id", new CatalogColumnStatisticsDataLong(1L, 10L, 10L, 42L));
		itemColumnStatisticsData.put("i_item_id", new CatalogColumnStatisticsDataString(16L, 16.0D, 8997L, 0L));
		itemColumnStatisticsData.put("i_class", new CatalogColumnStatisticsDataString(15L, 7.7587777777777776D, 99L, 0L));
		itemColumnStatisticsData.put("i_current_price", new CatalogColumnStatisticsDataDouble(0.09D, 99.99D, 2697L, 45L));
		itemColumnStatisticsData.put("i_category", new CatalogColumnStatisticsDataString(11L, 5.8805D, 11L, 0L));
		itemColumnStatisticsData.put("i_brand_id", new CatalogColumnStatisticsDataLong(1001001L, 10016017L, 942L, 44L));
		itemColumnStatisticsData.put("i_item_sk", new CatalogColumnStatisticsDataLong(1L, 18000L, 17869L, 0L));
		itemColumnStatisticsData.put("i_manufact", new CatalogColumnStatisticsDataString(15L, 11.315888888888889D, 988L, 0L));
		itemColumnStatisticsData.put("i_size", new CatalogColumnStatisticsDataString(11L, 4.3305D, 8L, 0L));
		itemColumnStatisticsData.put("i_color", new CatalogColumnStatisticsDataString(10L, 5.3742222222222225D, 93L, 0L));
		itemColumnStatisticsData.put("i_class_id", new CatalogColumnStatisticsDataLong(1L, 16L, 16L, 35L));
		itemColumnStatisticsData.put("i_wholesale_cost", new CatalogColumnStatisticsDataDouble(0.02D, 87.36D, 2031L, 46L));
		final CatalogColumnStatistics itemCatalogColumnStatistics = new CatalogColumnStatistics(itemColumnStatisticsData);
		final CatalogTableStatistics itemCatalogTableStatistics = new CatalogTableStatistics(18000, 0, 0, 0);
		final CatalogTableStats itemCatalogTableStats = new CatalogTableStats(itemCatalogTableStatistics, itemCatalogColumnStatistics);
		catalogTableStatsMap.put("item", itemCatalogTableStats);

		//CatalogTableStats for table promotion
		final Map<String, CatalogColumnStatisticsDataBase> promotionColumnStatisticsData = new HashMap<>();
		promotionColumnStatisticsData.put("p_channel_radio", new CatalogColumnStatisticsDataString(1L, 0.98D, 2L, 0L));
		promotionColumnStatisticsData.put("p_item_sk", new CatalogColumnStatisticsDataLong(28L, 17926L, 292L, 7L));
		promotionColumnStatisticsData.put("p_channel_catalog", new CatalogColumnStatisticsDataString(1L, 0.9866666666666667D, 2L, 0L));
		promotionColumnStatisticsData.put("p_response_target", new CatalogColumnStatisticsDataLong(1L, 1L, 1L, 7L));
		promotionColumnStatisticsData.put("p_start_date_sk", new CatalogColumnStatisticsDataLong(2450100L, 2450913L, 253L, 6L));
		promotionColumnStatisticsData.put("p_discount_active", new CatalogColumnStatisticsDataString(1L, 0.9766666666666667D, 2L, 0L));
		promotionColumnStatisticsData.put("p_promo_id", new CatalogColumnStatisticsDataString(16L, 16.0D, 300L, 0L));
		promotionColumnStatisticsData.put("p_promo_name", new CatalogColumnStatisticsDataString(5L, 3.9D, 11L, 0L));
		promotionColumnStatisticsData.put("p_cost", new CatalogColumnStatisticsDataDouble(1000.00D, 1000.00D, 1L, 6L));
		promotionColumnStatisticsData.put("p_purpose", new CatalogColumnStatisticsDataString(7L, 6.906666666666666D, 2L, 0L));
		promotionColumnStatisticsData.put("p_channel_dmail", new CatalogColumnStatisticsDataString(1L, 0.9833333333333333D, 3L, 0L));
		promotionColumnStatisticsData.put("p_channel_press", new CatalogColumnStatisticsDataString(1L, 0.9766666666666667D, 2L, 0L));
		promotionColumnStatisticsData.put("p_end_date_sk", new CatalogColumnStatisticsDataLong(2450132L, 2450955L, 251L, 6L));
		promotionColumnStatisticsData.put("p_channel_email", new CatalogColumnStatisticsDataString(1L, 0.9833333333333333D, 2L, 0L));
		promotionColumnStatisticsData.put("p_channel_tv", new CatalogColumnStatisticsDataString(1L, 0.98D, 2L, 0L));
		promotionColumnStatisticsData.put("p_promo_sk", new CatalogColumnStatisticsDataLong(1L, 300L, 301L, 0L));
		promotionColumnStatisticsData.put("p_channel_event", new CatalogColumnStatisticsDataString(1L, 0.9866666666666667D, 2L, 0L));
		promotionColumnStatisticsData.put("p_channel_demo", new CatalogColumnStatisticsDataString(1L, 0.99D, 2L, 0L));
		promotionColumnStatisticsData.put("p_channel_details", new CatalogColumnStatisticsDataString(60L, 39.96333333333333D, 301L, 0L));
		final CatalogColumnStatistics promotionCatalogColumnStatistics = new CatalogColumnStatistics(promotionColumnStatisticsData);
		final CatalogTableStatistics promotionCatalogTableStatistics = new CatalogTableStatistics(300, 0, 0, 0);
		final CatalogTableStats promotionCatalogTableStats = new CatalogTableStats(promotionCatalogTableStatistics, promotionCatalogColumnStatistics);
		catalogTableStatsMap.put("promotion", promotionCatalogTableStats);

		//CatalogTableStats for table reason
		final Map<String, CatalogColumnStatisticsDataBase> reasonColumnStatisticsData = new HashMap<>();
		reasonColumnStatisticsData.put("r_reason_sk", new CatalogColumnStatisticsDataLong(1L, 35L, 35L, 0L));
		reasonColumnStatisticsData.put("r_reason_id", new CatalogColumnStatisticsDataString(16L, 16.0D, 35L, 0L));
		reasonColumnStatisticsData.put("r_reason_desc", new CatalogColumnStatisticsDataString(43L, 16.514285714285716D, 34L, 0L));
		final CatalogColumnStatistics reasonCatalogColumnStatistics = new CatalogColumnStatistics(reasonColumnStatisticsData);
		final CatalogTableStatistics reasonCatalogTableStatistics = new CatalogTableStatistics(35, 0, 0, 0);
		final CatalogTableStats reasonCatalogTableStats = new CatalogTableStats(reasonCatalogTableStatistics, reasonCatalogColumnStatistics);
		catalogTableStatsMap.put("reason", reasonCatalogTableStats);

		//CatalogTableStats for table ship_mode
		final Map<String, CatalogColumnStatisticsDataBase> shipmodeColumnStatisticsData = new HashMap<>();
		shipmodeColumnStatisticsData.put("sm_type", new CatalogColumnStatisticsDataString(9L, 7.5D, 6L, 0L));
		shipmodeColumnStatisticsData.put("sm_ship_mode_id", new CatalogColumnStatisticsDataString(16L, 16.0D, 20L, 0L));
		shipmodeColumnStatisticsData.put("sm_ship_mode_sk", new CatalogColumnStatisticsDataLong(1L, 20L, 20L, 0L));
		shipmodeColumnStatisticsData.put("sm_contract", new CatalogColumnStatisticsDataString(20L, 12.6D, 20L, 0L));
		shipmodeColumnStatisticsData.put("sm_code", new CatalogColumnStatisticsDataString(7L, 4.35D, 4L, 0L));
		shipmodeColumnStatisticsData.put("sm_carrier", new CatalogColumnStatisticsDataString(14L, 6.65D, 20L, 0L));
		final CatalogColumnStatistics shipmodeCatalogColumnStatistics = new CatalogColumnStatistics(shipmodeColumnStatisticsData);
		final CatalogTableStatistics shipmodeCatalogTableStatistics = new CatalogTableStatistics(20, 0, 0, 0);
		final CatalogTableStats shipmodeCatalogTableStats = new CatalogTableStats(shipmodeCatalogTableStatistics, shipmodeCatalogColumnStatistics);
		catalogTableStatsMap.put("ship_mode", shipmodeCatalogTableStats);

		//CatalogTableStats for table store
		final Map<String, CatalogColumnStatisticsDataBase> storeColumnStatisticsData = new HashMap<>();
		storeColumnStatisticsData.put("s_country", new CatalogColumnStatisticsDataString(13L, 13.0D, 1L, 0L));
		storeColumnStatisticsData.put("s_tax_precentage", new CatalogColumnStatisticsDataDouble(0.01D, 0.11D, 5L, 0L));
		storeColumnStatisticsData.put("s_market_desc", new CatalogColumnStatisticsDataString(94L, 55.5D, 10L, 0L));
		storeColumnStatisticsData.put("s_store_sk", new CatalogColumnStatisticsDataLong(1L, 12L, 12L, 0L));
		storeColumnStatisticsData.put("s_city", new CatalogColumnStatisticsDataString(8L, 6.5D, 2L, 0L));
		storeColumnStatisticsData.put("s_store_id", new CatalogColumnStatisticsDataString(16L, 16.0D, 6L, 0L));
		storeColumnStatisticsData.put("s_suite_number", new CatalogColumnStatisticsDataString(9L, 8.25D, 11L, 0L));
		storeColumnStatisticsData.put("s_company_id", new CatalogColumnStatisticsDataLong(1L, 1L, 1L, 0L));
		storeColumnStatisticsData.put("s_store_name", new CatalogColumnStatisticsDataString(5L, 4.25D, 8L, 0L));
		storeColumnStatisticsData.put("s_floor_space", new CatalogColumnStatisticsDataLong(5219562L, 9341467L, 10L, 0L));
		storeColumnStatisticsData.put("s_street_number", new CatalogColumnStatisticsDataString(3L, 2.8333333333333335D, 9L, 0L));
		storeColumnStatisticsData.put("s_street_type", new CatalogColumnStatisticsDataString(9L, 4.833333333333333D, 8L, 0L));
		storeColumnStatisticsData.put("s_number_employees", new CatalogColumnStatisticsDataLong(218L, 297L, 9L, 0L));
		storeColumnStatisticsData.put("s_company_name", new CatalogColumnStatisticsDataString(7L, 7.0D, 1L, 0L));
		storeColumnStatisticsData.put("s_division_name", new CatalogColumnStatisticsDataString(7L, 7.0D, 1L, 0L));
		storeColumnStatisticsData.put("s_zip", new CatalogColumnStatisticsDataString(5L, 5.0D, 2L, 0L));
		storeColumnStatisticsData.put("s_hours", new CatalogColumnStatisticsDataString(8L, 7.083333333333333D, 2L, 0L));
		storeColumnStatisticsData.put("s_manager", new CatalogColumnStatisticsDataString(15L, 12.0D, 7L, 0L));
		storeColumnStatisticsData.put("s_market_manager", new CatalogColumnStatisticsDataString(16L, 14.0D, 7L, 0L));
		storeColumnStatisticsData.put("s_geography_class", new CatalogColumnStatisticsDataString(7L, 7.0D, 1L, 0L));
		storeColumnStatisticsData.put("s_gmt_offset", new CatalogColumnStatisticsDataDouble(-5.00D, -5.00D, 1L, 0L));
		storeColumnStatisticsData.put("s_state", new CatalogColumnStatisticsDataString(2L, 2.0D, 1L, 0L));
		storeColumnStatisticsData.put("s_street_name", new CatalogColumnStatisticsDataString(11L, 6.583333333333333D, 12L, 0L));
		storeColumnStatisticsData.put("s_closed_date_sk", new CatalogColumnStatisticsDataLong(2450910L, 2451189L, 3L, 9L));
		storeColumnStatisticsData.put("s_rec_start_date", new CatalogColumnStatisticsDataDate(null, null, 4L, 0L));
		storeColumnStatisticsData.put("s_county", new CatalogColumnStatisticsDataString(17L, 17.0D, 1L, 0L));
		storeColumnStatisticsData.put("s_division_id", new CatalogColumnStatisticsDataLong(1L, 1L, 1L, 0L));
		storeColumnStatisticsData.put("s_rec_end_date", new CatalogColumnStatisticsDataDate(null, null, 3L, 6L));
		storeColumnStatisticsData.put("s_market_id", new CatalogColumnStatisticsDataLong(2L, 10L, 7L, 0L));
		final CatalogColumnStatistics storeCatalogColumnStatistics = new CatalogColumnStatistics(storeColumnStatisticsData);
		final CatalogTableStatistics storeCatalogTableStatistics = new CatalogTableStatistics(12, 0, 0, 0);
		final CatalogTableStats storeCatalogTableStats = new CatalogTableStats(storeCatalogTableStatistics, storeCatalogColumnStatistics);
		catalogTableStatsMap.put("store", storeCatalogTableStats);

		//CatalogTableStats for table time_dim
		final Map<String, CatalogColumnStatisticsDataBase> timedimColumnStatisticsData = new HashMap<>();
		timedimColumnStatisticsData.put("t_minute", new CatalogColumnStatisticsDataLong(0L, 59L, 60L, 0L));
		timedimColumnStatisticsData.put("t_am_pm", new CatalogColumnStatisticsDataString(2L, 2.0D, 2L, 0L));
		timedimColumnStatisticsData.put("t_time_sk", new CatalogColumnStatisticsDataLong(0L, 86399L, 86180L, 0L));
		timedimColumnStatisticsData.put("t_time", new CatalogColumnStatisticsDataLong(0L, 86399L, 87750L, 0L));
		timedimColumnStatisticsData.put("t_time_id", new CatalogColumnStatisticsDataString(16L, 16.0D, 86824L, 0L));
		timedimColumnStatisticsData.put("t_second", new CatalogColumnStatisticsDataLong(0L, 59L, 60L, 0L));
		timedimColumnStatisticsData.put("t_meal_time", new CatalogColumnStatisticsDataString(9L, 2.875D, 4L, 0L));
		timedimColumnStatisticsData.put("t_shift", new CatalogColumnStatisticsDataString(6L, 5.333333333333333D, 3L, 0L));
		timedimColumnStatisticsData.put("t_hour", new CatalogColumnStatisticsDataLong(0L, 23L, 24L, 0L));
		timedimColumnStatisticsData.put("t_sub_shift", new CatalogColumnStatisticsDataString(9L, 6.916666666666667D, 4L, 0L));
		final CatalogColumnStatistics timedimCatalogColumnStatistics = new CatalogColumnStatistics(timedimColumnStatisticsData);
		final CatalogTableStatistics timedimCatalogTableStatistics = new CatalogTableStatistics(86400, 0, 0, 0);
		final CatalogTableStats timedimCatalogTableStats = new CatalogTableStats(timedimCatalogTableStatistics, timedimCatalogColumnStatistics);
		catalogTableStatsMap.put("time_dim", timedimCatalogTableStats);

		//CatalogTableStats for table warehouse
		final CatalogTableStatistics warehouseCatalogTableStatistics = new CatalogTableStatistics(5, 0, 0, 0);
		final Map<String, CatalogColumnStatisticsDataBase> warehouseColumnStatisticsData = new HashMap<>();
		warehouseColumnStatisticsData.put("w_state", new CatalogColumnStatisticsDataString(2L, 2.0D, 1L, 0L));
		warehouseColumnStatisticsData.put("w_gmt_offset", new CatalogColumnStatisticsDataDouble(-5.00D, -5.00D, 1L, 1L));
		warehouseColumnStatisticsData.put("w_warehouse_id", new CatalogColumnStatisticsDataString(16L, 16.0D, 5L, 0L));
		warehouseColumnStatisticsData.put("w_county", new CatalogColumnStatisticsDataString(17L, 17.0D, 1L, 0L));
		warehouseColumnStatisticsData.put("w_zip", new CatalogColumnStatisticsDataString(5L, 5.0D, 1L, 0L));
		warehouseColumnStatisticsData.put("w_city", new CatalogColumnStatisticsDataString(8L, 8.0D, 1L, 0L));
		warehouseColumnStatisticsData.put("w_country", new CatalogColumnStatisticsDataString(13L, 13.0D, 1L, 0L));
		warehouseColumnStatisticsData.put("w_warehouse_name", new CatalogColumnStatisticsDataString(20L, 14.0D, 5L, 0L));
		warehouseColumnStatisticsData.put("w_street_type", new CatalogColumnStatisticsDataString(7L, 4.2D, 5L, 0L));
		warehouseColumnStatisticsData.put("w_street_name", new CatalogColumnStatisticsDataString(10L, 6.8D, 5L, 0L));
		warehouseColumnStatisticsData.put("w_street_number", new CatalogColumnStatisticsDataString(3L, 2.4D, 5L, 0L));
		warehouseColumnStatisticsData.put("w_warehouse_sk", new CatalogColumnStatisticsDataLong(1L, 5L, 5L, 0L));
		warehouseColumnStatisticsData.put("w_warehouse_sq_ft", new CatalogColumnStatisticsDataLong(138504L, 977787L, 4L, 1L));
		warehouseColumnStatisticsData.put("w_suite_number", new CatalogColumnStatisticsDataString(9L, 6.2D, 5L, 0L));
		final CatalogColumnStatistics warehouseCatalogColumnStatistics = new CatalogColumnStatistics(warehouseColumnStatisticsData);
		final CatalogTableStats warehouseCatalogTableStats = new CatalogTableStats(warehouseCatalogTableStatistics, warehouseCatalogColumnStatistics);
		catalogTableStatsMap.put("warehouse", warehouseCatalogTableStats);

		//CatalogTableStats for table web_page
		final Map<String, CatalogColumnStatisticsDataBase> webpageColumnStatisticsData = new HashMap<>();
		webpageColumnStatisticsData.put("wp_image_count", new CatalogColumnStatisticsDataLong(1L, 7L, 7L, 1L));
		webpageColumnStatisticsData.put("wp_char_count", new CatalogColumnStatisticsDataLong(701L, 7046L, 42L, 1L));
		webpageColumnStatisticsData.put("wp_autogen_flag", new CatalogColumnStatisticsDataString(1L, 1.0D, 2L, 0L));
		webpageColumnStatisticsData.put("wp_creation_date_sk", new CatalogColumnStatisticsDataLong(2450807L, 2450815L, 9L, 1L));
		webpageColumnStatisticsData.put("wp_link_count", new CatalogColumnStatisticsDataLong(2L, 25L, 21L, 1L));
		webpageColumnStatisticsData.put("wp_rec_start_date", new CatalogColumnStatisticsDataDate(null, null, 4L, 0L));
		webpageColumnStatisticsData.put("wp_web_page_id", new CatalogColumnStatisticsDataString(16L, 16.0D, 30L, 0L));
		webpageColumnStatisticsData.put("wp_rec_end_date", new CatalogColumnStatisticsDataDate(null, null, 3L, 30L));
		webpageColumnStatisticsData.put("wp_access_date_sk", new CatalogColumnStatisticsDataLong(2452549L, 2452648L, 40L, 0L));
		webpageColumnStatisticsData.put("wp_customer_sk", new CatalogColumnStatisticsDataLong(1898L, 98633L, 17L, 39L));
		webpageColumnStatisticsData.put("wp_web_page_sk", new CatalogColumnStatisticsDataLong(1L, 60L, 60L, 0L));
		webpageColumnStatisticsData.put("wp_max_ad_count", new CatalogColumnStatisticsDataLong(0L, 4L, 5L, 1L));
		webpageColumnStatisticsData.put("wp_type", new CatalogColumnStatisticsDataString(9L, 6.366666666666666D, 8L, 0L));
		webpageColumnStatisticsData.put("wp_url", new CatalogColumnStatisticsDataString(18L, 18.0D, 1L, 0L));
		final CatalogColumnStatistics webpageCatalogColumnStatistics = new CatalogColumnStatistics(webpageColumnStatisticsData);
		final CatalogTableStatistics webpageCatalogTableStatistics = new CatalogTableStatistics(60, 0, 0, 0);
		final CatalogTableStats webpageCatalogTableStats = new CatalogTableStats(webpageCatalogTableStatistics, webpageCatalogColumnStatistics);
		catalogTableStatsMap.put("web_page", webpageCatalogTableStats);

		//CatalogTableStats for table web_site
		final Map<String, CatalogColumnStatisticsDataBase> websiteColumnStatisticsData = new HashMap<>();
		websiteColumnStatisticsData.put("web_market_manager", new CatalogColumnStatisticsDataString(16L, 12.733333333333333D, 25L, 0L));
		websiteColumnStatisticsData.put("web_country", new CatalogColumnStatisticsDataString(13L, 13.0D, 1L, 0L));
		websiteColumnStatisticsData.put("web_open_date_sk", new CatalogColumnStatisticsDataLong(2450577L, 2450807L, 15L, 0L));
		websiteColumnStatisticsData.put("web_street_type", new CatalogColumnStatisticsDataString(9L, 4.066666666666666D, 15L, 0L));
		websiteColumnStatisticsData.put("web_zip", new CatalogColumnStatisticsDataString(5L, 5.0D, 2L, 0L));
		websiteColumnStatisticsData.put("web_gmt_offset", new CatalogColumnStatisticsDataDouble(-5.00D, -5.00D, 1L, 0L));
		websiteColumnStatisticsData.put("web_street_number", new CatalogColumnStatisticsDataString(3L, 2.933333333333333D, 18L, 0L));
		websiteColumnStatisticsData.put("web_state", new CatalogColumnStatisticsDataString(2L, 2.0D, 1L, 0L));
		websiteColumnStatisticsData.put("web_suite_number", new CatalogColumnStatisticsDataString(9L, 8.2D, 23L, 0L));
		websiteColumnStatisticsData.put("web_rec_end_date", new CatalogColumnStatisticsDataDate(null, null, 3L, 15L));
		websiteColumnStatisticsData.put("web_close_date_sk", new CatalogColumnStatisticsDataLong(2446944L, 2448956L, 10L, 5L));
		websiteColumnStatisticsData.put("web_company_name", new CatalogColumnStatisticsDataString(5L, 3.966666666666667D, 6L, 0L));
		websiteColumnStatisticsData.put("web_manager", new CatalogColumnStatisticsDataString(16L, 12.6D, 22L, 0L));
		websiteColumnStatisticsData.put("web_mkt_class", new CatalogColumnStatisticsDataString(49L, 32.733333333333334D, 22L, 0L));
		websiteColumnStatisticsData.put("web_mkt_id", new CatalogColumnStatisticsDataLong(1L, 6L, 6L, 0L));
		websiteColumnStatisticsData.put("web_name", new CatalogColumnStatisticsDataString(6L, 6.0D, 5L, 0L));
		websiteColumnStatisticsData.put("web_tax_percentage", new CatalogColumnStatisticsDataDouble(0.00D, 0.12D, 10L, 0L));
		websiteColumnStatisticsData.put("web_company_id", new CatalogColumnStatisticsDataLong(1L, 6L, 6L, 0L));
		websiteColumnStatisticsData.put("web_site_sk", new CatalogColumnStatisticsDataLong(1L, 30L, 30L, 0L));
		websiteColumnStatisticsData.put("web_mkt_desc", new CatalogColumnStatisticsDataString(94L, 68.8D, 18L, 0L));
		websiteColumnStatisticsData.put("web_rec_start_date", new CatalogColumnStatisticsDataDate(null, null, 4L, 0L));
		websiteColumnStatisticsData.put("web_class", new CatalogColumnStatisticsDataString(7L, 7.0D, 1L, 0L));
		websiteColumnStatisticsData.put("web_city", new CatalogColumnStatisticsDataString(8L, 6.733333333333333D, 2L, 0L));
		websiteColumnStatisticsData.put("web_county", new CatalogColumnStatisticsDataString(17L, 17.0D, 1L, 0L));
		websiteColumnStatisticsData.put("web_site_id", new CatalogColumnStatisticsDataString(16L, 16.0D, 15L, 0L));
		websiteColumnStatisticsData.put("web_street_name", new CatalogColumnStatisticsDataString(14L, 9.066666666666666D, 30L, 0L));
		final CatalogColumnStatistics websiteCatalogColumnStatistics = new CatalogColumnStatistics(websiteColumnStatisticsData);
		final CatalogTableStatistics websiteCatalogTableStatistics = new CatalogTableStatistics(30, 0, 0, 0);
		final CatalogTableStats websiteCatalogTableStats = new CatalogTableStats(websiteCatalogTableStatistics, websiteCatalogColumnStatistics);
		catalogTableStatsMap.put("web_site", websiteCatalogTableStats);

		return catalogTableStatsMap;
	}

	public static void registerTpcdsStats(TableEnvironment tEnv) {
		for (Map.Entry<String, CatalogTableStats> enrty : catalogTableStatsMap.entrySet()) {
			String table = enrty.getKey();
			CatalogTableStats catalogTableStats = enrty.getValue();
			catalogTableStats.register2Catalog(tEnv, table);
		}
	}
}
