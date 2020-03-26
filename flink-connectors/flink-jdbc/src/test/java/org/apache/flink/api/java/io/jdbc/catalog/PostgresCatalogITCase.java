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

package org.apache.flink.api.java.io.jdbc.catalog;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableUtils;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.List;

import static org.apache.flink.api.java.io.jdbc.catalog.PostgresCatalog.DEFAULT_DATABASE;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.junit.Assert.assertEquals;

/**
 * E2E test for {@link PostgresCatalog}.
 */
public class PostgresCatalogITCase extends PostgresCatalogTestBase {

	@Test
	public void test_withoutSchema() throws Exception {
		TableEnvironment tEnv = getTableEnvWithPgCatalog();

		List<Row> results = TableUtils.collectToList(
			tEnv.sqlQuery(String.format("select * from %s", TABLE1)));
		assertEquals("[1]", results.toString());
	}

	@Test
	public void test_withSchema() throws Exception {
		TableEnvironment tEnv = getTableEnvWithPgCatalog();

		List<Row> results = TableUtils.collectToList(
			tEnv.sqlQuery(String.format("select * from `%s`", PostgresTablePath.fromFlinkTableName(TABLE1))));
		assertEquals("[1]", results.toString());
	}

	@Test
	public void test_fullPath() throws Exception {
		TableEnvironment tEnv = getTableEnvWithPgCatalog();

		List<Row> results = TableUtils.collectToList(
			tEnv.sqlQuery(String.format("select * from %s.%s.`%s`",
				TEST_CATALOG_NAME,
				DEFAULT_DATABASE,
				PostgresTablePath.fromFlinkTableName(TABLE1))));
		assertEquals("[1]", results.toString());
	}

	@Test
	public void test_insert() throws Exception {
		TableEnvironment tEnv = getTableEnvWithPgCatalog();

		tEnv.sqlUpdate(String.format("insert into %s select * from `%s`", TABLE4, TABLE1));
		tEnv.execute("test");

		List<Row> results = TableUtils.collectToList(
			tEnv.sqlQuery(String.format("select * from %s", TABLE1)));
		assertEquals("[1]", results.toString());
	}

	private TableEnvironment getTableEnvWithPgCatalog() {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		TableEnvironment tableEnv = TableEnvironment.create(settings);
		tableEnv.getConfig().getConfiguration().setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 1);

		tableEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
		tableEnv.useCatalog(TEST_CATALOG_NAME);
		return tableEnv;
	}
}
