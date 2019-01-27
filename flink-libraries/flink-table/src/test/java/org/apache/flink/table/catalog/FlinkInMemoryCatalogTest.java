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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.TableNotExistException;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for FlinkInMemoryCatalog.
 */
public class FlinkInMemoryCatalogTest extends CatalogTestBase {

	@Override
	public String getTableType() {
		return "csv";
	}

	@Before
	public void setUp() {
		catalog = new FlinkInMemoryCatalog(db1);
	}

	@Test
	public void testRenameDb() {
		CatalogDatabase schema = createDb();
		catalog.createDatabase(db1, schema, false);

		assertTrue(catalog.listDatabases().contains(db1));

		catalog.renameDatabase(db1, db2, false);

		assertTrue(catalog.listDatabases().contains(db2));
		assertFalse(catalog.listDatabases().contains(db1));
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testRenameNonexistentDb() {
		catalog.renameDatabase("nonexisit", db2, false);
	}

	@Test
	public void testRenameTable() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createDatabase(db2, createAnotherDb(), false);

		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		assertEquals(path1, catalog.listAllTables().get(0));

		catalog.renameTable(path1, path2.getObjectName(), false);

		assertEquals(new ObjectPath(path1.getDbName(), path2.getObjectName()), catalog.listAllTables().get(0));
	}

	@Test(expected = TableNotExistException.class)
	public void testRenameTableNonexistentDb() {
		catalog.renameTable(nonExistDbPath, "", false);
	}

	@Test(expected = TableNotExistException.class)
	public void testRenameNonexistentTable() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.renameTable(nonExistTablePath, path2.getObjectName(), false);
	}
}
