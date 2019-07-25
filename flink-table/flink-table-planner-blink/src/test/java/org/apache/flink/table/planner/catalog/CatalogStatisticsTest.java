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
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.Date;
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
	public void testGetStatsFromCatalog() throws Exception {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
		TableEnvironment tEnv = TableEnvironment.create(settings);
		tEnv.registerTableSource("T1", new TestTableSource(true, tableSchema));
		tEnv.registerTableSource("T2", new TestTableSource(true, tableSchema));
		Catalog catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).orElse(null);
		assertNotNull(catalog);

		catalog.alterTableStatistics(ObjectPath.fromString("default_database.T1"),
				new CatalogTableStatistics(100, 10, 1000L, 2000L), true);
		catalog.alterTableStatistics(ObjectPath.fromString("default_database.T2"),
				new CatalogTableStatistics(100000000, 1000, 1000000000L, 2000000000L), true);
		catalog.alterTableColumnStatistics(ObjectPath.fromString("default_database.T1"), createColumnStats(), true);
		catalog.alterTableColumnStatistics(ObjectPath.fromString("default_database.T2"), createColumnStats(), true);

		Table table = tEnv.sqlQuery("select * from T1, T2 where T1.s3 = T2.s3");
		String result = tEnv.explain(table);
		// T1 is broadcast side
		String expected = TableTestUtil.readFromResource("/explain/testGetStatsFromCatalog.out");
		assertEquals(expected, TableTestUtil.replaceStageId(result));
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
