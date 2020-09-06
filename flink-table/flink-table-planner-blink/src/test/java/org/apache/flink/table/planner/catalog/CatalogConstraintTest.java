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
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.types.DataType;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test for Catalog constraints.
 */
public class CatalogConstraintTest {

	private String databaseName = "default_database";

	private TableEnvironment tEnv;
	private Catalog catalog;

	@Before
	public void setup() {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
		tEnv = TableEnvironment.create(settings);
		catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).orElse(null);
		assertNotNull(catalog);
	}

	@Test
	public void testWithPrimaryKey() throws Exception {
		TableSchema tableSchema = TableSchema.builder().fields(
				new String[] { "a", "b", "c" },
				new DataType[] { DataTypes.STRING(), DataTypes.BIGINT().notNull(), DataTypes.INT() }
		).primaryKey("b").build();
		Map<String, String> properties = buildCatalogTableProperties(tableSchema);

		catalog.createTable(
				new ObjectPath(databaseName, "T1"),
				new CatalogTableImpl(tableSchema, properties, ""),
				false);

		RelNode t1 = TableTestUtil.toRelNode(tEnv.sqlQuery("select * from T1"));
		FlinkRelMetadataQuery mq = FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
		assertEquals(ImmutableSet.of(ImmutableBitSet.of(1)), mq.getUniqueKeys(t1));
	}

	@Test
	public void testWithoutPrimaryKey() throws Exception {
		TableSchema tableSchema = TableSchema.builder().fields(
				new String[] { "a", "b", "c" },
				new DataType[] { DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.INT() }
		).build();
		Map<String, String> properties = buildCatalogTableProperties(tableSchema);

		catalog.createTable(
				new ObjectPath(databaseName, "T1"),
				new CatalogTableImpl(tableSchema, properties, ""),
				false);

		RelNode t1 = TableTestUtil.toRelNode(tEnv.sqlQuery("select * from T1"));
		FlinkRelMetadataQuery mq = FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
		assertEquals(ImmutableSet.of(), mq.getUniqueKeys(t1));
	}

	private Map<String, String> buildCatalogTableProperties(TableSchema tableSchema) {
		Map<String, String> properties = new HashMap<>();
		properties.put("connector.type", "filesystem");
		properties.put("connector.property-version", "1");
		properties.put("connector.path", "/path/to/csv");

		properties.put("format.type", "csv");
		properties.put("format.property-version", "1");
		properties.put("format.field-delimiter", ";");

		return properties;
	}

}
