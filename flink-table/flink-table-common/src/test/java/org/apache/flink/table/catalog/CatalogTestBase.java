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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base testing class for unit tests of a specific catalog, like GenericInMemoryCatalog and HiveCatalog.
 */
public abstract class CatalogTestBase {
	protected static final String IS_STREAMING = "is_streaming";

	protected final String db1 = "db1";
	protected final String db2 = "db2";
	protected final String nonExistentDatabase = "non-existent-db";

	protected final String t1 = "t1";
	protected final String t2 = "t2";
	protected final ObjectPath path1 = new ObjectPath(db1, t1);
	protected final ObjectPath path2 = new ObjectPath(db2, t2);
	protected final ObjectPath path3 = new ObjectPath(db1, t2);
	protected final ObjectPath path4 = new ObjectPath(db1, "t3");
	protected final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
	protected final ObjectPath nonExistObjectPath = ObjectPath.fromString("db1.nonexist");

	public static final String TEST_CATALOG_NAME = "test-catalog";

	protected static final String TEST_COMMENT = "test comment";

	protected static ReadableWritableCatalog catalog;

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
		if (catalog.databaseExists(db2)) {
			catalog.dropDatabase(db2, true);
		}
	}

	@AfterClass
	public static void closeup() {
		catalog.close();
	}

	// ------ databases ------

	@Test
	public void testCreateDb() throws Exception {
		catalog.createDatabase(db2, createDb(), false);

		assertEquals(2, catalog.listDatabases().size());
	}

	@Test
	public void testSetCurrentDatabase() throws Exception {
		assertEquals(getBuiltInDefaultDatabase(), catalog.getCurrentDatabase());
		catalog.createDatabase(db2, createDb(), true);
		catalog.setCurrentDatabase(db2);
		assertEquals(db2, catalog.getCurrentDatabase());
		catalog.setCurrentDatabase(getBuiltInDefaultDatabase());
		assertEquals(getBuiltInDefaultDatabase(), catalog.getCurrentDatabase());
		catalog.dropDatabase(db2, false);
	}

	@Test
	public void testSetCurrentDatabaseNegative() throws Exception {
		exception.expect(DatabaseNotExistException.class);
		exception.expectMessage("Database " + this.nonExistentDatabase + " does not exist in Catalog");
		catalog.setCurrentDatabase(this.nonExistentDatabase);
	}

	@Test
	public void testCreateDb_DatabaseAlreadyExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(DatabaseAlreadyExistException.class);
		exception.expectMessage("Database db1 already exists in Catalog");
		catalog.createDatabase(db1, createDb(), false);
	}

	@Test
	public void testCreateDb_DatabaseAlreadyExist_ignored() throws Exception {
		CatalogDatabase cd1 = createDb();
		catalog.createDatabase(db1, cd1, false);
		List<String> dbs = catalog.listDatabases();

		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(cd1.getProperties().entrySet()));
		assertEquals(2, dbs.size());
		assertEquals(new HashSet<>(Arrays.asList(db1, catalog.getCurrentDatabase())), new HashSet<>(dbs));

		catalog.createDatabase(db1, createAnotherDb(), true);

		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(cd1.getProperties().entrySet()));
		assertEquals(2, dbs.size());
		assertEquals(new HashSet<>(Arrays.asList(db1, catalog.getCurrentDatabase())), new HashSet<>(dbs));
	}

	@Test
	public void testGetDb_DatabaseNotExistException() throws Exception {
		exception.expect(DatabaseNotExistException.class);
		exception.expectMessage("Database nonexistent does not exist in Catalog");
		catalog.getDatabase("nonexistent");
	}

	@Test
	public void testDropDb() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.listDatabases().contains(db1));

		catalog.dropDatabase(db1, false);

		assertFalse(catalog.listDatabases().contains(db1));
	}

	@Test
	public void testDropDb_DatabaseNotExistException() throws Exception {
		exception.expect(DatabaseNotExistException.class);
		exception.expectMessage("Database db1 does not exist in Catalog");
		catalog.dropDatabase(db1, false);
	}

	@Test
	public void testDropDb_DatabaseNotExist_Ignore() throws Exception {
		catalog.dropDatabase(db1, true);
	}

	@Test
	public void testDropDb_DatabaseNotEmptyException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		exception.expect(DatabaseNotEmptyException.class);
		exception.expectMessage("Database db1 in catalog test-catalog is not empty");
		catalog.dropDatabase(db1, true);
	}

	@Test
	public void testAlterDb() throws Exception {
		CatalogDatabase db = createDb();
		catalog.createDatabase(db1, db, false);

		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(db.getProperties().entrySet()));

		CatalogDatabase newDb = createAnotherDb();
		catalog.alterDatabase(db1, newDb, false);

		assertFalse(catalog.getDatabase(db1).getProperties().entrySet().containsAll(db.getProperties().entrySet()));
		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(newDb.getProperties().entrySet()));
	}

	@Test
	public void testAlterDb_DatabaseNotExistException() throws Exception {
		exception.expect(DatabaseNotExistException.class);
		exception.expectMessage("Database nonexistent does not exist in Catalog");
		catalog.alterDatabase("nonexistent", createDb(), false);
	}

	@Test
	public void testAlterDb_DatabaseNotExist_ignored() throws Exception {
		catalog.alterDatabase("nonexistent", createDb(), true);

		assertFalse(catalog.databaseExists("nonexistent"));
	}

	@Test
	public void testDbExists() throws Exception {
		assertFalse(catalog.databaseExists("nonexistent"));

		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.databaseExists(db1));
	}

	// ------ tables ------

	@Test
	public void testCreateTable_Streaming() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createStreamingTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));
	}

	@Test
	public void testCreateTable_Batch() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		// Non-partitioned table
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		CatalogBaseTable tableCreated = catalog.getTable(path1);

		CatalogTestUtil.checkEquals(table, (CatalogTable) tableCreated);
		assertEquals(TEST_COMMENT, tableCreated.getDescription().get());

		List<String> tables = catalog.listTables(db1);

		assertEquals(1, tables.size());
		assertEquals(path1.getObjectName(), tables.get(0));

		catalog.dropTable(path1, false);

		// Partitioned table
		table = createPartitionedTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));

		tables = catalog.listTables(db1);

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

		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));

		catalog.createTable(path1, createAnotherTable(), true);

		CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));
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

		// Non-partitioned table
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));

		CatalogTable newTable = createAnotherTable();
		catalog.alterTable(path1, newTable, false);

		assertNotEquals(table, catalog.getTable(path1));
		CatalogTestUtil.checkEquals(newTable, (CatalogTable) catalog.getTable(path1));

		catalog.dropTable(path1, false);

		// Partitioned table
		table = createPartitionedTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));

		newTable = createAnotherPartitionedTable();
		catalog.alterTable(path1, newTable, false);

		CatalogTestUtil.checkEquals(newTable, (CatalogTable) catalog.getTable(path1));
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
	public void testRenameTable_nonPartitionedTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));

		catalog.renameTable(path1, t2, false);

		CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path3));
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
		catalog.createTable(path3, createAnotherTable(), false);

		exception.expect(TableAlreadyExistException.class);
		exception.expectMessage("Table (or view) db1.t2 already exists in Catalog");
		catalog.renameTable(path1, t2, false);
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
	 * Get the built-in default database of the specific catalog implementation.
	 *
	 * @return The built-in default database name
	 */
	public abstract String getBuiltInDefaultDatabase();

	/**
	 * Create a CatalogDatabase instance by specific catalog implementation.
	 *
	 * @return a CatalogDatabase instance
	 */
	public abstract CatalogDatabase createDb();

	/**
	 * Create another CatalogDatabase instance by specific catalog implementation.
	 *
	 * @return another CatalogDatabase instance
	 */
	public abstract CatalogDatabase createAnotherDb();

	/**
	 * Create a CatalogTable instance by specific catalog implementation.
	 *
	 * @return a CatalogTable instance
	 */
	public abstract CatalogTable createTable();

	/**
	 * Create another CatalogTable instance by specific catalog implementation.
	 *
	 * @return another CatalogTable instance
	 */
	public abstract CatalogTable createAnotherTable();

	/**
	 * Create a streaming CatalogTable instance by specific catalog implementation.
	 *
	 * @return a streaming CatalogTable instance
	 */
	public abstract CatalogTable createStreamingTable();

	/**
	 * Create a partitioned CatalogTable instance by specific catalog implementation.
	 *
	 * @return a streaming CatalogTable instance
	 */
	public abstract CatalogTable createPartitionedTable();

	/**
	 * Create another partitioned CatalogTable instance by specific catalog implementation.
	 *
	 * @return another partitioned CatalogTable instance
	 */
	public abstract CatalogTable createAnotherPartitionedTable();

	protected TableSchema createTableSchema() {
		return new TableSchema(
			new String[] {"first", "second", "third"},
			new TypeInformation[] {
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
			}
		);
	}

	protected TableSchema createAnotherTableSchema() {
		return new TableSchema(
			new String[] {"first2", "second", "third"},
			new TypeInformation[] {
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO
			}
		);
	}

	protected List<String> createPartitionKeys() {
		return Arrays.asList("second", "third");
	}

	protected Map<String, String> getBatchTableProperties() {
		return new HashMap<String, String>() {{
			put(IS_STREAMING, "false");
		}};
	}

	protected Map<String, String> getStreamingTableProperties() {
		return new HashMap<String, String>() {{
			put(IS_STREAMING, "true");
		}};
	}
}
