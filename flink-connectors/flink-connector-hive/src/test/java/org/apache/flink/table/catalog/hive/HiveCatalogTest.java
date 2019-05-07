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
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTestBase;
import org.apache.flink.table.catalog.CatalogView;

import org.junit.BeforeClass;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for HiveCatalog.
 */
public class HiveCatalogTest extends CatalogTestBase {

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

	// =====================
	// HiveCatalog doesn't support view operation yet
	// Thus, overriding the following tests which involve table operation in CatalogTestBase so they won't run against HiveCatalog
	// =====================

	// TODO: re-enable these tests once HiveCatalog support view operations

	public void testListTables() throws Exception {
	}

	public void testCreateView() throws Exception {
	}

	public void testCreateView_DatabaseNotExistException() throws Exception {
	}

	public void testCreateView_TableAlreadyExistException() throws Exception {
	}

	public void testCreateView_TableAlreadyExist_ignored() throws Exception {
	}

	public void testDropView() throws Exception {
	}

	public void testAlterView() throws Exception {
	}

	public void testAlterView_TableNotExistException() throws Exception {
	}

	public void testAlterView_TableNotExist_ignored() throws Exception {
	}

	public void testListView() throws Exception {
	}

	public void testRenameView() throws Exception {
	}

	// ------ utils ------

	@Override
	public String getBuiltInDefaultDatabase() {
		return HiveCatalogBase.DEFAULT_DB;
	}

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
		// TODO: implement this once HiveCatalog support view operations
		return null;
	}

	@Override
	public CatalogView createAnotherView() {
		// TODO: implement this once HiveCatalog support view operations
		return null;
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
}
