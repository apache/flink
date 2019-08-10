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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBinary;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.Date;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;

/**
 * Test for HiveCatalog on Hive metadata.
 */
public class HiveCatalogHiveMetadataTest extends HiveCatalogTestBase {

	@BeforeClass
	public static void init() {
		catalog = HiveTestUtils.createHiveCatalog();
		catalog.open();
	}

	// =====================
	// HiveCatalog doesn't support streaming table operation. Ignore this test in CatalogTest.
	// =====================

	public void testCreateTable_Streaming() throws Exception {
	}

	@Test
	// verifies that input/output formats and SerDe are set for Hive tables
	public void testCreateTable_StorageFormatSet() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		Table hiveTable = ((HiveCatalog) catalog).getHiveTable(path1);
		String inputFormat = hiveTable.getSd().getInputFormat();
		String outputFormat = hiveTable.getSd().getOutputFormat();
		String serde = hiveTable.getSd().getSerdeInfo().getSerializationLib();
		assertFalse(StringUtils.isNullOrWhitespaceOnly(inputFormat));
		assertFalse(StringUtils.isNullOrWhitespaceOnly(outputFormat));
		assertFalse(StringUtils.isNullOrWhitespaceOnly(serde));
	}

	// ------ table and column stats ------
	@Test
	public void testAlterTableColumnStatistics() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		TableSchema tableSchema = TableSchema.builder()
											.field("first", DataTypes.STRING())
											.field("second", DataTypes.INT())
											.field("third", DataTypes.BOOLEAN())
											.field("fourth", DataTypes.DATE())
											.field("fifth", DataTypes.DOUBLE())
											.field("sixth", DataTypes.BIGINT())
											.field("seventh", DataTypes.BYTES())
											.build();
		CatalogTable catalogTable = new CatalogTableImpl(tableSchema, getBatchTableProperties(), TEST_COMMENT);
		catalog.createTable(path1, catalogTable, false);
		Map<String, CatalogColumnStatisticsDataBase> columnStatisticsDataBaseMap = new HashMap<>();
		columnStatisticsDataBaseMap.put("first", new CatalogColumnStatisticsDataString(10, 5.2, 3, 100));
		columnStatisticsDataBaseMap.put("second", new CatalogColumnStatisticsDataLong(0, 1000, 3, 0));
		columnStatisticsDataBaseMap.put("third", new CatalogColumnStatisticsDataBoolean(15, 20, 3));
		columnStatisticsDataBaseMap.put("fourth", new CatalogColumnStatisticsDataDate(new Date(71L), new Date(17923L), 1321, 0L));
		columnStatisticsDataBaseMap.put("fifth", new CatalogColumnStatisticsDataDouble(15.02, 20.01, 3, 10));
		columnStatisticsDataBaseMap.put("sixth", new CatalogColumnStatisticsDataLong(0, 20, 3, 2));
		columnStatisticsDataBaseMap.put("seventh", new CatalogColumnStatisticsDataBinary(150, 20, 3));
		CatalogColumnStatistics catalogColumnStatistics = new CatalogColumnStatistics(columnStatisticsDataBaseMap);
		catalog.alterTableColumnStatistics(path1, catalogColumnStatistics, false);

		checkEquals(catalogColumnStatistics, catalog.getTableColumnStatistics(path1));
	}

	@Test
	public void testAlterPartitionColumnStatistics() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable catalogTable = createPartitionedTable();
		catalog.createTable(path1, catalogTable, false);
		CatalogPartitionSpec partitionSpec = createPartitionSpec();
		catalog.createPartition(path1, partitionSpec, createPartition(), true);
		Map<String, CatalogColumnStatisticsDataBase> columnStatisticsDataBaseMap = new HashMap<>();
		columnStatisticsDataBaseMap.put("first", new CatalogColumnStatisticsDataString(10, 5.2, 3, 100));
		CatalogColumnStatistics catalogColumnStatistics = new CatalogColumnStatistics(columnStatisticsDataBaseMap);
		catalog.alterPartitionColumnStatistics(path1, partitionSpec, catalogColumnStatistics, false);

		checkEquals(catalogColumnStatistics, catalog.getPartitionColumnStatistics(path1, partitionSpec));
	}

	// ------ utils ------

	@Override
	protected boolean isGeneric() {
		return false;
	}

	@Override
	public CatalogTable createStreamingTable() {
		throw new UnsupportedOperationException(
			"Hive table cannot be streaming."
		);
	}
}
