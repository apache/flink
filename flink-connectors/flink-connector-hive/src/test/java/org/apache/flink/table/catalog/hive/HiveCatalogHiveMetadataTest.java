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

import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTestBase;
import org.apache.flink.table.catalog.CatalogView;

import org.junit.BeforeClass;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for HiveCatalog on Hive metadata.
 */
public class HiveCatalogHiveMetadataTest extends CatalogTestBase {

	@BeforeClass
	public static void init() throws IOException {
		catalog = HiveTestUtils.createHiveCatalog();
		catalog.open();
	}

	// =====================
	// HiveCatalog doesn't support streaming table operation. Ignore this test in CatalogTestBase.
	// =====================

	public void testCreateTable_Streaming() throws Exception {
	}

	// ------ utils ------

	@Override
	public CatalogDatabase createDb() {
		return new HiveCatalogDatabase(
			new HashMap<String, String>() {{
				put("k1", "v1");
			}},
			TEST_COMMENT
		);
	}

	@Override
	public CatalogDatabase createAnotherDb() {
		return new HiveCatalogDatabase(
			new HashMap<String, String>() {{
				put("k2", "v2");
			}},
			TEST_COMMENT
		);
	}

	@Override
	public CatalogTable createTable() {
		return new HiveCatalogTable(
			createTableSchema(),
			getBatchTableProperties(),
			TEST_COMMENT
		);
	}

	@Override
	public CatalogTable createAnotherTable() {
		return new HiveCatalogTable(
			createAnotherTableSchema(),
			getBatchTableProperties(),
			TEST_COMMENT
		);
	}

	@Override
	public CatalogTable createStreamingTable() {
		throw new UnsupportedOperationException("HiveCatalog doesn't support streaming tables.");
	}

	@Override
	public CatalogTable createPartitionedTable() {
		return new HiveCatalogTable(
			createTableSchema(),
			createPartitionKeys(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createAnotherPartitionedTable() {
		return new HiveCatalogTable(
			createAnotherTableSchema(),
			createPartitionKeys(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogView createView() {
		return new HiveCatalogView(
			String.format("select * from %s", t1),
			String.format("select * from %s.%s", TEST_CATALOG_NAME, path1.getFullName()),
			createTableSchema(),
			new HashMap<>(),
			"This is a hive view");
	}

	@Override
	public CatalogView createAnotherView() {
		return new HiveCatalogView(
			String.format("select * from %s", t2),
			String.format("select * from %s.%s", TEST_CATALOG_NAME, path2.getFullName()),
			createAnotherTableSchema(),
			new HashMap<>(),
			"This is another hive view");
	}

	@Override
	protected CatalogFunction createFunction() {
		return new HiveCatalogFunction("test.class.name");
	}

	@Override
	protected CatalogFunction createAnotherFunction() {
		return new HiveCatalogFunction("test.another.class.name");
	}

	@Override
	public CatalogPartition createPartition() {
		return new HiveCatalogPartition(getBatchTableProperties());
	}

	@Override
	public void checkEquals(CatalogTable t1, CatalogTable t2) {
		assertEquals(t1.getSchema(), t2.getSchema());
		assertEquals(t1.getComment(), t2.getComment());
		assertEquals(t1.getPartitionKeys(), t2.getPartitionKeys());
		assertEquals(t1.isPartitioned(), t2.isPartitioned());

		// Hive tables may have properties created by itself
		// thus properties of Hive table is a super set of those in its corresponding Flink table
		assertTrue(t2.getProperties().entrySet().containsAll(t1.getProperties().entrySet()));
	}

	@Override
	protected void checkEquals(CatalogView v1, CatalogView v2) {
		assertEquals(v1.getSchema(), v1.getSchema());
		assertEquals(v1.getComment(), v2.getComment());
		assertEquals(v1.getOriginalQuery(), v2.getOriginalQuery());
		assertEquals(v1.getExpandedQuery(), v2.getExpandedQuery());

		// Hive views may have properties created by itself
		// thus properties of Hive view is a super set of those in its corresponding Flink view
		assertTrue(v2.getProperties().entrySet().containsAll(v1.getProperties().entrySet()));
	}

	@Override
	protected void checkEquals(CatalogPartition expected, CatalogPartition actual) {
		assertTrue(expected instanceof HiveCatalogPartition && actual instanceof HiveCatalogPartition);
		assertEquals(expected.getClass(), actual.getClass());
		HiveCatalogPartition hivePartition1 = (HiveCatalogPartition) expected;
		HiveCatalogPartition hivePartition2 = (HiveCatalogPartition) actual;
		assertEquals(hivePartition1.getDescription(), hivePartition2.getDescription());
		assertEquals(hivePartition1.getDetailedDescription(), hivePartition2.getDetailedDescription());
		assertTrue(hivePartition2.getProperties().entrySet().containsAll(hivePartition1.getProperties().entrySet()));
	}
}
