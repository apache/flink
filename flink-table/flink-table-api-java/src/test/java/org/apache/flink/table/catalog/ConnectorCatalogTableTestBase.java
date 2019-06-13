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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.sources.TableSource;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base testing class for operations of {@link ConnectorCatalogTable} in catalogs.
 */
public abstract class ConnectorCatalogTableTestBase {

	protected final String db1 = "db1";

	protected final String t1 = "t1";
	protected final String t2 = "t2";
	protected final String t3 = "t3";
	protected final ObjectPath path1 = new ObjectPath(db1, t1);
	protected final ObjectPath path2 = new ObjectPath(db1, t2);
	protected final ObjectPath path3 = new ObjectPath(db1, t3);
	protected final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
	protected final ObjectPath nonExistObjectPath = ObjectPath.fromString("db1.nonexist");

	public static final String TEST_CATALOG_NAME = "test-catalog";

	protected static Catalog catalog;

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@After
	public void cleanup() throws Exception {
		if (catalog.tableExists(path1)) {
			catalog.dropTable(path1, true);
		}
		if (catalog.tableExists(path2)) {
			catalog.dropTable(path2, true);
		}
		if (catalog.tableExists(path3)) {
			catalog.dropTable(path3, true);
		}
		if (catalog.databaseExists(db1)) {
			catalog.dropDatabase(db1, true);
		}
	}

	@AfterClass
	public static void closeup() {
		catalog.close();
	}

	// ------ connector tables ------

	@Test
	public void testCreateTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		ConnectorCatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		CatalogBaseTable tableCreated = catalog.getTable(path1);

		checkEquals(table, (ConnectorCatalogTable) tableCreated);

		List<String> tables = catalog.listTables(db1);

		assertEquals(1, tables.size());
		assertEquals(path1.getObjectName(), tables.get(0));
	}

	@Test
	public void testCreateTable_DatabaseNotExistException() throws Exception {
		assertFalse(catalog.databaseExists(db1));

		exception.expect(DatabaseNotExistException.class);
		exception.expectMessage("Database db1 does not exist in Catalog");
		catalog.createTable(nonExistObjectPath, createTable(), false);
	}

	@Test
	public void testCreateTable_TableAlreadyExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1,  createTable(), false);

		exception.expect(TableAlreadyExistException.class);
		exception.expectMessage("Table (or view) db1.t1 already exists in Catalog");
		catalog.createTable(path1, createTable(), false);
	}

	@Test
	public void testCreateTable_TableAlreadyExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		ConnectorCatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		checkEquals(table, (ConnectorCatalogTable) catalog.getTable(path1));

		catalog.createTable(path1, createAnotherTable(), true);

		checkEquals(table, (ConnectorCatalogTable) catalog.getTable(path1));
	}

	@Test
	public void testGetTable_TableNotExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(TableNotExistException.class);
		exception.expectMessage("Table (or view) db1.nonexist does not exist in Catalog");
		catalog.getTable(nonExistObjectPath);
	}

	@Test
	public void testGetTable_TableNotExistException_NoDb() throws Exception {
		exception.expect(TableNotExistException.class);
		exception.expectMessage("Table (or view) db1.nonexist does not exist in Catalog");
		catalog.getTable(nonExistObjectPath);
	}

	@Test
	public void testDropTable_nonPartitionedTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));
	}

	@Test
	public void testDropTable_TableNotExistException() throws Exception {
		exception.expect(TableNotExistException.class);
		exception.expectMessage("Table (or view) non.exist does not exist in Catalog");
		catalog.dropTable(nonExistDbPath, false);
	}

	@Test
	public void testDropTable_TableNotExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.dropTable(nonExistObjectPath, true);
	}

	@Test
	public void testAlterTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		ConnectorCatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		checkEquals(table, (ConnectorCatalogTable) catalog.getTable(path1));

		ConnectorCatalogTable newTable = createAnotherTable();
		catalog.alterTable(path1, newTable, false);

		assertNotEquals(table, catalog.getTable(path1));
		checkEquals(newTable, (ConnectorCatalogTable) catalog.getTable(path1));

		catalog.dropTable(path1, false);
	}

	@Test
	public void testAlterTable_TableNotExistException() throws Exception {
		exception.expect(TableNotExistException.class);
		exception.expectMessage("Table (or view) non.exist does not exist in Catalog");
		catalog.alterTable(nonExistDbPath, createTable(), false);
	}

	@Test
	public void testAlterTable_TableNotExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterTable(nonExistObjectPath, createTable(), true);

		assertFalse(catalog.tableExists(nonExistObjectPath));
	}

	@Test
	public void testRenameTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		ConnectorCatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		checkEquals(table, (ConnectorCatalogTable) catalog.getTable(path1));

		catalog.renameTable(path1, t2, false);

		checkEquals(table, (ConnectorCatalogTable) catalog.getTable(path2));
		assertFalse(catalog.tableExists(path1));
	}

	@Test
	public void testRenameTable_TableNotExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(TableNotExistException.class);
		exception.expectMessage("Table (or view) db1.t1 does not exist in Catalog");
		catalog.renameTable(path1, t2, false);
	}

	@Test
	public void testRenameTable_TableNotExistException_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.renameTable(path1, t2, true);
	}

	@Test
	public void testRenameTable_TableAlreadyExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);
		catalog.createTable(path2, createAnotherTable(), false);

		exception.expect(TableAlreadyExistException.class);
		exception.expectMessage("Table (or view) db1.t2 already exists in Catalog");
		catalog.renameTable(path1, t2, false);
	}

	@Test
	public void testListTables() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		catalog.createTable(path1, createTable(), false);
		catalog.createTable(path2, createTable(), false);

		assertEquals(2, catalog.listTables(db1).size());
		assertEquals(0, catalog.listViews(db1).size());
	}

	@Test
	public void testTableExists() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		assertFalse(catalog.tableExists(path1));

		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.tableExists(path1));
	}

	// ------ utilities ------

	/**
	 * Create a CatalogDatabase instance by specific catalog implementation.
	 *
	 * @return a CatalogDatabase instance
	 */
	public abstract CatalogDatabase createDb();

	/**
	 * Create a ConnectorCatalogTable instance.
	 */
	private ConnectorCatalogTable createTable() {
		return ConnectorCatalogTable.source(
			new TableSource<String>() {
				@Override
				public TableSchema getTableSchema() {
					return createTableSchema();
				}
			},
			false
		);
	}

	/**
	 * Create another ConnectorCatalogTable.
	 */
	private ConnectorCatalogTable createAnotherTable() {
		return ConnectorCatalogTable.source(
			new TableSource<String>() {
				@Override
				public TableSchema getTableSchema() {
					return createAnotherTableSchema();
				}
			},
			false
		);
	}

	private TableSchema createTableSchema() {
		return TableSchema.builder()
			.field("first", DataTypes.STRING())
			.field("second", DataTypes.INT())
			.field("third", DataTypes.STRING())
			.build();
	}

	private TableSchema createAnotherTableSchema() {
		return TableSchema.builder()
			.field("first", DataTypes.STRING())
			.field("second", DataTypes.STRING())
			.field("third", DataTypes.STRING())
			.build();
	}

	// ------ equality check utils ------

	protected void checkEquals(ConnectorCatalogTable t1, ConnectorCatalogTable t2) {
		assertEquals(t1.getSchema(), t2.getSchema());
		assertEquals(t1.getProperties(), t2.getProperties());
		assertEquals(t1.getComment(), t2.getComment());
		assertEquals(t1.getPartitionKeys(), t2.getPartitionKeys());
		assertEquals(t1.isPartitioned(), t2.isPartitioned());
	}
}
