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

package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.apache.flink.connector.jdbc.catalog.PostgresCatalog.DEFAULT_DATABASE;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.junit.Assert.assertEquals;

/**
 * E2E test for {@link PostgresCatalog}.
 */
public class PostgresCatalogITCase extends PostgresCatalogTestBase {

	private TableEnvironment tEnv;

	@Before
	public void setup() {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		this.tEnv = TableEnvironment.create(settings);
		tEnv.getConfig().getConfiguration().setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 1);

		// use PG catalog
		tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
		tEnv.useCatalog(TEST_CATALOG_NAME);
	}

	@Test
	public void testSelectField() {
		List<Row> results = Lists.newArrayList(
			tEnv.sqlQuery(String.format("select id from %s", TABLE1)).execute().collect());
		assertEquals("[1]", results.toString());
	}

	@Test
	public void testWithoutSchema() {
		List<Row> results = Lists.newArrayList(
			tEnv.sqlQuery(String.format("select * from %s", TABLE1)).execute().collect());
		assertEquals("[1]", results.toString());
	}

	@Test
	public void testWithSchema() {
		List<Row> results = Lists.newArrayList(
			tEnv.sqlQuery(String.format("select * from `%s`", PostgresTablePath.fromFlinkTableName(TABLE1))).execute().collect());
		assertEquals("[1]", results.toString());
	}

	@Test
	public void testFullPath() {
		List<Row> results = Lists.newArrayList(
			tEnv.sqlQuery(String.format("select * from %s.%s.`%s`",
				TEST_CATALOG_NAME,
				DEFAULT_DATABASE,
				PostgresTablePath.fromFlinkTableName(TABLE1))).execute().collect());
		assertEquals("[1]", results.toString());
	}

	@Test
	public void testInsert() {
		TableEnvUtil.execInsertSqlAndWaitResult(
			tEnv,
			String.format("insert into %s select * from `%s`", TABLE4, TABLE1));

		List<Row> results = Lists.newArrayList(
			tEnv.sqlQuery(String.format("select * from %s", TABLE4)).execute().collect());
		assertEquals("[1]", results.toString());
	}

	@Test
	public void testGroupByInsert() {
		TableEnvUtil.execInsertSqlAndWaitResult(
			tEnv,
			String.format(
				"insert into `%s` " +
					"select `int`, cast('A' as bytes), `short`, max(`long`), max(`real`), " +
					"max(`double_precision`), max(`numeric`), max(`decimal`), max(`boolean`), " +
					"max(`text`), 'B', 'C', max(`character_varying`), max(`timestamp`), " +
					"max(`date`), max(`time`), max(`default_numeric`) " +
					"from `%s` group by `int`, `short`",
				TABLE_PRIMITIVE_TYPE2,
				TABLE_PRIMITIVE_TYPE));

		List<Row> results = Lists.newArrayList(
			tEnv.sqlQuery(String.format("select * from `%s`", TABLE_PRIMITIVE_TYPE2)).execute().collect());
		assertEquals("[1,[65],3,4,5.5,6.6,7.70000,8.8,true,a,B,C  ,d,2016-06-22T19:10:25,2015-01-01,00:51:03,500.000000000000000000]", results.toString());
	}

	@Test
	public void testPrimitiveTypes() {
		List<Row> results = Lists.newArrayList(
			tEnv.sqlQuery(String.format("select * from %s", TABLE_PRIMITIVE_TYPE)).execute().collect());

		assertEquals("[1,[50],3,4,5.5,6.6,7.70000,8.8,true,a,b,c  ,d,2016-06-22T19:10:25,2015-01-01,00:51:03,500.000000000000000000]", results.toString());
	}

	@Test
	public void testArrayTypes() {
		List<Row> results = Lists.newArrayList(
			tEnv.sqlQuery(String.format("select * from %s", TABLE_ARRAY_TYPE)).execute().collect());

		assertEquals("[" +
				"[1, 2, 3]," +
				"[[92, 120, 51, 50], [92, 120, 51, 51], [92, 120, 51, 52]]," +
				"[3, 4, 5]," +
				"[4, 5, 6]," +
				"[5.5, 6.6, 7.7]," +
				"[6.6, 7.7, 8.8]," +
				"[7.70000, 8.80000, 9.90000]," +
				"[8.800000000000000000, 9.900000000000000000, 10.100000000000000000]," +
				"[9.90, 10.10, 11.11]," +
				"[true, false, true]," +
				"[a, b, c]," +
				"[b, c, d]," +
				"[b  , c  , d  ]," +
				"[b, c, d]," +
				"[2016-06-22T19:10:25, 2019-06-22T19:10:25]," +
				"[2015-01-01, 2020-01-01]," +
				"[00:51:03, 00:59:03]]",
			results.toString());
	}

	@Test
	public void testSerialTypes() {
		List<Row> results = Lists.newArrayList(
			tEnv.sqlQuery(String.format("select * from %s", TABLE_SERIAL_TYPE)).execute().collect());

		assertEquals("[" +
				"32767," +
				"2147483647," +
				"32767," +
				"2147483647," +
				"9223372036854775807," +
				"9223372036854775807]",
			results.toString());
	}
}
