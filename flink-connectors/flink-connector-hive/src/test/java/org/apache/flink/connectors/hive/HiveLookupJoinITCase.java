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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.factories.TableSourceFactoryContextImpl;
import org.apache.flink.table.filesystem.FileSystemLookupFunction;
import org.apache.flink.table.filesystem.FileSystemOptions;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test lookup join of hive tables.
 */
public class HiveLookupJoinITCase {

	private TableEnvironment tableEnv;
	private HiveCatalog hiveCatalog;

	@Before
	public void setup() {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		tableEnv = TableEnvironment.create(settings);
		hiveCatalog = HiveTestUtils.createHiveCatalog();
		tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
		tableEnv.useCatalog(hiveCatalog.getName());
	}

	@After
	public void tearDown() {
		if (hiveCatalog != null) {
			hiveCatalog.close();
		}
	}

	@Test
	public void test() throws Exception {
		// create the hive build table
		tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
		tableEnv.executeSql(String.format("create table build (x int,y string,z int) tblproperties ('%s'='5min')",
				FileSystemOptions.LOOKUP_JOIN_CACHE_TTL.key()));
		tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

		// verify we properly configured the cache TTL
		ObjectIdentifier tableIdentifier = ObjectIdentifier.of(hiveCatalog.getName(), "default", "build");
		CatalogTable catalogTable = (CatalogTable) hiveCatalog.getTable(tableIdentifier.toObjectPath());
		HiveTableSource hiveTableSource = (HiveTableSource) ((HiveTableFactory) hiveCatalog.getTableFactory().get()).createTableSource(
				new TableSourceFactoryContextImpl(tableIdentifier, catalogTable, tableEnv.getConfig().getConfiguration()));
		FileSystemLookupFunction lookupFunction = (FileSystemLookupFunction) hiveTableSource.getLookupFunction(new String[]{"x"});
		assertEquals(Duration.ofMinutes(5), lookupFunction.getCacheTTL());

		try {
			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv,
					"insert into build values (1,'a',10),(2,'a',21),(2,'b',22),(3,'c',33)");

			TestCollectionTableFactory.initData(
					Arrays.asList(Row.of(1, "a"), Row.of(1, "c"), Row.of(2, "b"), Row.of(2, "c"), Row.of(3, "c"), Row.of(4, "d")));
			tableEnv.executeSql("create table default_catalog.default_database.probe (x int,y string,p as proctime()) " +
					"with ('connector'='COLLECTION','is-bounded' = 'false')");

			TableImpl flinkTable = (TableImpl) tableEnv.sqlQuery("select p.x,p.y from default_catalog.default_database.probe as p join " +
					"build for system_time as of p.p as b on p.x=b.x and p.y=b.y");
			List<Row> results = Lists.newArrayList(flinkTable.execute().collect());
			assertEquals("[1,a, 2,b, 3,c]", results.toString());
		} finally {
			tableEnv.executeSql("drop table build");
		}
	}
}
