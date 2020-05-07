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

import org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabase.AlterHiveDatabaseOp;
import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBinary;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.Date;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabase.ALTER_DATABASE_OP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for HiveCatalog on Hive metadata.
 */
public class HiveCatalogHiveMetadataTest extends HiveCatalogMetadataTestBase {

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

	@Override
	@Test
	public void testAlterDb() throws Exception {
		// altering Hive DB merges properties, which is different from generic DB
		CatalogDatabase db = createDb();
		catalog.createDatabase(db1, db, false);

		CatalogDatabase newDb = createAnotherDb();
		newDb.getProperties().put(ALTER_DATABASE_OP, AlterHiveDatabaseOp.CHANGE_PROPS.name());
		catalog.alterDatabase(db1, newDb, false);

		Map<String, String> mergedProps = new HashMap<>(db.getProperties());
		mergedProps.putAll(newDb.getProperties());

		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(mergedProps.entrySet()));
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
		String hiveVersion = ((HiveCatalog) catalog).getHiveVersion();
		boolean supportDateStats = hiveVersion.compareTo(HiveShimLoader.HIVE_VERSION_V1_2_0) >= 0;
		catalog.createDatabase(db1, createDb(), false);
		TableSchema.Builder builder = TableSchema.builder()
				.field("first", DataTypes.STRING())
				.field("second", DataTypes.INT())
				.field("third", DataTypes.BOOLEAN())
				.field("fourth", DataTypes.DOUBLE())
				.field("fifth", DataTypes.BIGINT())
				.field("sixth", DataTypes.BYTES());
		if (supportDateStats) {
			builder.field("seventh", DataTypes.DATE());
		}
		TableSchema tableSchema = builder.build();
		CatalogTable catalogTable = new CatalogTableImpl(tableSchema, getBatchTableProperties(), TEST_COMMENT);
		catalog.createTable(path1, catalogTable, false);
		Map<String, CatalogColumnStatisticsDataBase> columnStatisticsDataBaseMap = new HashMap<>();
		columnStatisticsDataBaseMap.put("first", new CatalogColumnStatisticsDataString(10L, 5.2, 3L, 100L));
		columnStatisticsDataBaseMap.put("second", new CatalogColumnStatisticsDataLong(0L, 1000L, 3L, 0L));
		columnStatisticsDataBaseMap.put("third", new CatalogColumnStatisticsDataBoolean(15L, 20L, 3L));
		columnStatisticsDataBaseMap.put("fourth", new CatalogColumnStatisticsDataDouble(15.02, 20.01, 3L, 10L));
		columnStatisticsDataBaseMap.put("fifth", new CatalogColumnStatisticsDataLong(0L, 20L, 3L, 2L));
		columnStatisticsDataBaseMap.put("sixth", new CatalogColumnStatisticsDataBinary(150L, 20D, 3L));
		if (supportDateStats) {
			columnStatisticsDataBaseMap.put("seventh", new CatalogColumnStatisticsDataDate(
					new Date(71L), new Date(17923L), 132L, 0L));
		}
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
		columnStatisticsDataBaseMap.put("first", new CatalogColumnStatisticsDataString(10L, 5.2, 3L, 100L));
		CatalogColumnStatistics catalogColumnStatistics = new CatalogColumnStatistics(columnStatisticsDataBaseMap);
		catalog.alterPartitionColumnStatistics(path1, partitionSpec, catalogColumnStatistics, false);

		checkEquals(catalogColumnStatistics, catalog.getPartitionColumnStatistics(path1, partitionSpec));
	}

	@Test
	public void testHiveStatistics() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		checkStatistics(0, -1);
		checkStatistics(1, 1);
		checkStatistics(1000, 1000);
	}

	@Test
	public void testCreateTableWithConstraints() throws Exception {
		Assume.assumeTrue(HiveVersionTestUtil.HIVE_310_OR_LATER);
		HiveCatalog hiveCatalog = (HiveCatalog) catalog;
		hiveCatalog.createDatabase(db1, createDb(), false);
		TableSchema.Builder builder = TableSchema.builder();
		builder.fields(
				new String[]{"x", "y", "z"},
				new DataType[]{DataTypes.INT().notNull(), DataTypes.TIMESTAMP(9).notNull(), DataTypes.BIGINT()});
		builder.primaryKey("pk_name", new String[]{"x"});
		hiveCatalog.createTable(path1, new CatalogTableImpl(builder.build(), getBatchTableProperties(), null), false);
		CatalogTable catalogTable = (CatalogTable) hiveCatalog.getTable(path1);
		assertTrue("PK not present", catalogTable.getSchema().getPrimaryKey().isPresent());
		UniqueConstraint pk = catalogTable.getSchema().getPrimaryKey().get();
		assertEquals("pk_name", pk.getName());
		assertEquals(Collections.singletonList("x"), pk.getColumns());
		assertFalse(catalogTable.getSchema().getFieldDataTypes()[0].getLogicalType().isNullable());
		assertFalse(catalogTable.getSchema().getFieldDataTypes()[1].getLogicalType().isNullable());
		assertTrue(catalogTable.getSchema().getFieldDataTypes()[2].getLogicalType().isNullable());

		hiveCatalog.dropDatabase(db1, false, true);
	}

	private void checkStatistics(int inputStat, int expectStat) throws Exception {
		catalog.dropTable(path1, true);

		Map<String, String> properties = new HashMap<>();
		properties.put(CatalogConfig.IS_GENERIC, "false");
		properties.put(StatsSetupConst.ROW_COUNT, String.valueOf(inputStat));
		properties.put(StatsSetupConst.NUM_FILES, String.valueOf(inputStat));
		properties.put(StatsSetupConst.TOTAL_SIZE, String.valueOf(inputStat));
		properties.put(StatsSetupConst.RAW_DATA_SIZE, String.valueOf(inputStat));
		CatalogTable catalogTable = new CatalogTableImpl(
				TableSchema.builder().field("f0", DataTypes.INT()).build(),
				properties,
				"");
		catalog.createTable(path1, catalogTable, false);

		CatalogTableStatistics statistics = catalog.getTableStatistics(path1);
		assertEquals(expectStat, statistics.getRowCount());
		assertEquals(expectStat, statistics.getFileCount());
		assertEquals(expectStat, statistics.getRawDataSize());
		assertEquals(expectStat, statistics.getTotalSize());
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
