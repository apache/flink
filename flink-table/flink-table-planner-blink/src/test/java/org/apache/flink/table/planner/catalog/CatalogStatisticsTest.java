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
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.stats.ValueInterval$;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.planner.utils.TestTableSource;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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

		alterTableStatistics(catalog);

		assertStatistics(tEnv);
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

		alterTableStatistics(catalog);

		assertStatistics(tEnv);
	}

	private void alterTableStatistics(Catalog catalog) throws TableNotExistException, TablePartitionedException {
		catalog.alterTableStatistics(ObjectPath.fromString("default_database.T1"),
				new CatalogTableStatistics(100, 10, 1000L, 2000L), true);
		catalog.alterTableColumnStatistics(ObjectPath.fromString("default_database.T1"), createColumnStats(), true);
	}

	private CatalogColumnStatistics createColumnStats() {
		CatalogColumnStatisticsDataBoolean booleanColStats = new CatalogColumnStatisticsDataBoolean(55L, 45L, 5L);
		CatalogColumnStatisticsDataLong longColStats = new CatalogColumnStatisticsDataLong(-123L, 763322L, 23L, 77L);
		CatalogColumnStatisticsDataString stringColStats = new CatalogColumnStatisticsDataString(152L, 43.5D, 20L, 0L);
		CatalogColumnStatisticsDataDate dateColStats =
				new CatalogColumnStatisticsDataDate(new Date(71L), new Date(17923L), 100, 0L);
		CatalogColumnStatisticsDataDouble doubleColStats =
				new CatalogColumnStatisticsDataDouble(-123.35D, 7633.22D, 73L, 27L);
		Map<String, CatalogColumnStatisticsDataBase> colStatsMap = new HashMap<>(6);
		colStatsMap.put("b1", booleanColStats);
		colStatsMap.put("l2", longColStats);
		colStatsMap.put("s3", stringColStats);
		colStatsMap.put("d4", dateColStats);
		colStatsMap.put("dd5", doubleColStats);
		return new CatalogColumnStatistics(colStatsMap);
	}

	private void assertStatistics(TableEnvironment tEnv) {
		RelNode t1 = TableTestUtil.toRelNode(tEnv.sqlQuery("select * from T1"));
		FlinkRelMetadataQuery mq = FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
		assertEquals(100.0, mq.getRowCount(t1), 0.0);
		assertEquals(Arrays.asList(1.0, 8.0, 43.5, 12.0, 8.0), mq.getAverageColumnSizes(t1));

		// boolean type
		assertEquals(2.0, mq.getDistinctRowCount(t1, ImmutableBitSet.of(0), null), 0.0);
		assertEquals(5.0, mq.getColumnNullCount(t1, 0), 0.0);
		assertNull(mq.getColumnInterval(t1, 0));

		// long type
		assertEquals(23.0, mq.getDistinctRowCount(t1, ImmutableBitSet.of(1), null), 0.0);
		assertEquals(77.0, mq.getColumnNullCount(t1, 1), 0.0);
		assertEquals(ValueInterval$.MODULE$.apply(-123L, 763322L, true, true), mq.getColumnInterval(t1, 1));

		// string type
		assertEquals(20.0, mq.getDistinctRowCount(t1, ImmutableBitSet.of(2), null), 0.0);
		assertEquals(0.0, mq.getColumnNullCount(t1, 2), 0.0);
		assertNull(mq.getColumnInterval(t1, 2));

		// date type
		assertEquals(100.0, mq.getDistinctRowCount(t1, ImmutableBitSet.of(3), null), 0.0);
		assertEquals(0.0, mq.getColumnNullCount(t1, 3), 0.0);
		assertEquals(
				ValueInterval$.MODULE$.apply(
						java.sql.Date.valueOf(DateTimeUtils.unixDateToString(71)),
						java.sql.Date.valueOf(DateTimeUtils.unixDateToString(17923)), true, true),
				mq.getColumnInterval(t1, 3));

		// double type
		assertEquals(73.0, mq.getDistinctRowCount(t1, ImmutableBitSet.of(4), null), 0.0);
		assertEquals(27.0, mq.getColumnNullCount(t1, 4), 0.0);
		assertEquals(ValueInterval$.MODULE$.apply(-123.35, 7633.22, true, true), mq.getColumnInterval(t1, 4));
	}

}
