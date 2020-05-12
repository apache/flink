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

package org.apache.flink.connectors.hive;

import org.apache.flink.connectors.hive.read.HiveTableLookupFunction;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.factories.TableSourceFactoryContextImpl;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.types.Row;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test lookup join of hive tables.
 */
@RunWith(FlinkStandaloneHiveRunner.class)
public class HiveLookupJoinTest {

	@HiveSQL(files = {})
	private static HiveShell hiveShell;

	@Test
	public void test() throws Exception {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		TableEnvironment tableEnv = TableEnvironment.create(settings);
		HiveCatalog hiveCatalog = HiveTestUtils.createHiveCatalog(hiveShell.getHiveConf());
		tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
		tableEnv.useCatalog(hiveCatalog.getName());

		hiveShell.execute(String.format("create table build (x int,y string,z int) tblproperties ('%s'='5min')",
				HiveOptions.LOOKUP_JOIN_CACHE_TTL.key()));

		// verify we properly configured the cache TTL
		ObjectIdentifier tableIdentifier = ObjectIdentifier.of(hiveCatalog.getName(), "default", "build");
		CatalogTable catalogTable = (CatalogTable) hiveCatalog.getTable(tableIdentifier.toObjectPath());
		HiveTableSource hiveTableSource = (HiveTableSource) ((HiveTableFactory) hiveCatalog.getTableFactory().get()).createTableSource(
				new TableSourceFactoryContextImpl(tableIdentifier, catalogTable, tableEnv.getConfig().getConfiguration()));
		HiveTableLookupFunction lookupFunction = (HiveTableLookupFunction) hiveTableSource.getLookupFunction(new String[]{"x"});
		assertEquals(Duration.ofMinutes(5), lookupFunction.getCacheTTL());

		try {
			HiveTestUtils.createTextTableInserter(hiveShell, "default", "build")
					.addRow(new Object[]{1, "a", 10})
					.addRow(new Object[]{2, "a", 21})
					.addRow(new Object[]{2, "b", 22})
					.addRow(new Object[]{3, "c", 33})
					.commit();

			TestCollectionTableFactory.initData(Arrays.asList(Row.of(1, 1), Row.of(1, 0), Row.of(2, 1), Row.of(2, 3), Row.of(3, 1), Row.of(4, 4)));
			tableEnv.sqlUpdate("create table default_catalog.default_database.probe (x int,y int,p as proctime()) with ('connector'='COLLECTION','is-bounded' = 'false')");

			TableImpl flinkTable = (TableImpl) tableEnv.sqlQuery("select p.x,p.y from default_catalog.default_database.probe as p join " +
					"build for system_time as of p.p as b on p.x=b.x");
			List<Row> results = Lists.newArrayList(flinkTable.execute().collect());
			assertEquals("[1,1, 1,0, 2,1, 2,1, 2,3, 2,3, 3,1]", results.toString());
		} finally {
			hiveShell.execute("drop table build");
		}
	}
}
