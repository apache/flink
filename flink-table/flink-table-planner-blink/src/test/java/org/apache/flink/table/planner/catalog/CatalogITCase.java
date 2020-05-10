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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

import org.junit.Test;

import static org.apache.flink.table.descriptors.GenericInMemoryCatalogValidator.CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY;
import static org.junit.Assert.assertTrue;

/**
 * IT Case for catalog ddl.
 */
public class CatalogITCase {

	@Test
	public void testCreateCatalog() {
		String name = "c1";
		TableEnvironment tableEnv = getTableEnvironment();
		String ddl = String.format("create catalog %s with('type'='%s')", name, CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY);

		tableEnv.executeSql(ddl);

		assertTrue(tableEnv.getCatalog(name).isPresent());
		assertTrue(tableEnv.getCatalog(name).get() instanceof GenericInMemoryCatalog);
	}

	private TableEnvironment getTableEnvironment() {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		return StreamTableEnvironment.create(env, settings);
	}
}
