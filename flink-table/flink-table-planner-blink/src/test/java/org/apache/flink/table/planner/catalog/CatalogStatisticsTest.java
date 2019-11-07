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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.Date;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.planner.utils.TestTableSource;
import org.apache.flink.table.types.DataType;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test for Catalog Statistics.
 */
public class CatalogStatisticsTest {

	private TableSchema tableSchema = TableSchema.builder().fields(
			new String[] { "b1", "l2", "s3", "d4", "dd5" },
			new DataType[] { DataTypes.BOOLEAN(), DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.DATE(),
					DataTypes.DOUBLE() }
	).build();

	@Test
	public void testGetStatsFromCatalogForConnectorCatalogTable() throws Exception {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
		TableEnvironment tEnv = TableEnvironment.create(settings);
		Catalog catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).orElse(null);
		assertNotNull(catalog);
		catalog.createTable(
				ObjectPath.fromString("default_database.T1"),
				ConnectorCatalogTable.source(new TestTableSource(true, tableSchema), true),
				false);
		catalog.createTable(
				ObjectPath.fromString("default_database.T2"),
				ConnectorCatalogTable.source(new TestTableSource(true, tableSchema), true),
				false);

		alterTableStatistics(catalog);

		Table table = tEnv.sqlQuery("select * from T1, T2 where T1.s3 = T2.s3");
		String result = tEnv.explain(table);
		// T1 is broadcast side
		String expected = TableTestUtil.readFromResource("/explain/testGetStatsFromCatalogForConnectorCatalogTable.out");
		assertEquals(expected, TableTestUtil.replaceStageId(result));
	}

	@Test
	public void testGetStatsFromCatalogForCatalogTableImpl() throws Exception {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
		TableEnvironment tEnv = TableEnvironment.create(settings);
		Catalog catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).orElse(null);
		assertNotNull(catalog);

		Map<String, String> properties = new HashMap<>();
		properties.put("connector.type", "filesystem");
		properties.put("connector.property-version", "1");
		properties.put("connector.path", "/path/to/csv");

		properties.put("format.type", "csv");
		properties.put("format.property-version", "1");
		properties.put("format.field-delimiter", ";");

		// schema
		DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putTableSchema("format.fields", tableSchema);
		properties.putAll(descriptorProperties.asMap());

		catalog.createTable(
				ObjectPath.fromString("default_database.T1"),
				new CatalogTableImpl(tableSchema, properties, ""),
				false);
		catalog.createTable(
				ObjectPath.fromString("default_database.T2"),
				new CatalogTableImpl(tableSchema, properties, ""),
				false);

		alterTableStatistics(catalog);

		Table table = tEnv.sqlQuery("select * from T1, T2 where T1.s3 = T2.s3");
		String result = tEnv.explain(table);
		// T1 is broadcast side
		String expected = TableTestUtil.readFromResource("/explain/testGetStatsFromCatalogForCatalogTableImpl.out");
		assertEquals(expected, TableTestUtil.replaceStageId(result));
	}

	private void alterTableStatistics(Catalog catalog) throws TableNotExistException, TablePartitionedException {
		catalog.alterTableStatistics(ObjectPath.fromString("default_database.T1"),
				new CatalogTableStatistics(100, 10, 1000L, 2000L), true);
		catalog.alterTableStatistics(ObjectPath.fromString("default_database.T2"),
				new CatalogTableStatistics(100000000, 1000, 1000000000L, 2000000000L), true);
		catalog.alterTableColumnStatistics(ObjectPath.fromString("default_database.T1"), createColumnStats(), true);
		catalog.alterTableColumnStatistics(ObjectPath.fromString("default_database.T2"), createColumnStats(), true);
	}

	private CatalogColumnStatistics createColumnStats() {
		CatalogColumnStatisticsDataBoolean booleanColStats = new CatalogColumnStatisticsDataBoolean(55L, 45L, 5L);
		CatalogColumnStatisticsDataLong longColStats = new CatalogColumnStatisticsDataLong(-123L, 763322L, 23L, 79L);
		CatalogColumnStatisticsDataString stringColStats = new CatalogColumnStatisticsDataString(152L, 43.5D, 20L, 0L);
		CatalogColumnStatisticsDataDate dateColStats =
				new CatalogColumnStatisticsDataDate(new Date(71L), new Date(17923L), 1321, 0L);
		CatalogColumnStatisticsDataDouble doubleColStats =
				new CatalogColumnStatisticsDataDouble(-123.35D, 7633.22D, 23L, 79L);
		Map<String, CatalogColumnStatisticsDataBase> colStatsMap = new HashMap<>(6);
		colStatsMap.put("b1", booleanColStats);
		colStatsMap.put("l2", longColStats);
		colStatsMap.put("s3", stringColStats);
		colStatsMap.put("d4", dateColStats);
		colStatsMap.put("dd5", doubleColStats);
		return new CatalogColumnStatistics(colStatsMap);
	}

}
