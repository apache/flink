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

/**
 * Test for HiveCatalog.
 */
public class HiveCatalogTest extends CatalogTestBase {
	@BeforeClass
	public static void init() throws IOException {
		catalog = HiveTestUtils.createGenericHiveMetastoreCatalog();
		catalog.open();
	}

	// =====================
	// HiveCatalog doesn't support table operation yet
	// Thus, overriding the following tests which involve table operation in CatalogTestBase so they won't run against HiveCatalog
	// =====================

	// TODO: re-enable these tests once HiveCatalog support table operations
	public void testDropDb_DatabaseNotEmptyException() throws Exception {
	}

	public void testCreateTable_Streaming() throws Exception {
	}

	public void testCreateTable_Batch() throws Exception {
	}

	public void testCreateTable_DatabaseNotExistException() throws Exception {
	}

	public void testCreateTable_TableAlreadyExistException() throws Exception {
	}

	public void testCreateTable_TableAlreadyExist_ignored() throws Exception {
	}

	public void testGetTable_TableNotExistException() throws Exception {
	}

	public void testGetTable_TableNotExistException_NoDb() throws Exception {
	}

	public void testDropTable_nonPartitionedTable() throws Exception {
	}

	public void testDropTable_TableNotExistException() throws Exception {
	}

	public void testDropTable_TableNotExist_ignored() throws Exception {
	}

	public void testAlterTable() throws Exception {
	}

	public void testAlterTable_TableNotExistException() throws Exception {
	}

	public void testAlterTable_TableNotExist_ignored() throws Exception {
	}

	public void testRenameTable_nonPartitionedTable() throws Exception {
	}

	public void testRenameTable_TableNotExistException() throws Exception {
	}

	public void testRenameTable_TableNotExistException_ignored() throws Exception {
	}

	public void testRenameTable_TableAlreadyExistException() throws Exception {
	}

	public void testListTables() throws Exception {
	}

	public void testTableExists() throws Exception {
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
		// TODO: implement this once HiveCatalog support table operations
		return null;
	}

	@Override
	public CatalogTable createAnotherTable() {
		// TODO: implement this once HiveCatalog support table operations
		return null;
	}

	@Override
	public CatalogTable createStreamingTable() {
		// TODO: implement this once HiveCatalog support table operations
		return null;
	}

	@Override
	public CatalogTable createPartitionedTable() {
		// TODO: implement this once HiveCatalog support table operations
		return null;
	}

	@Override
	public CatalogTable createAnotherPartitionedTable() {
		// TODO: implement this once HiveCatalog support table operations
		return null;
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
}
