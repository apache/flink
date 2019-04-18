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
import org.junit.Before;
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
 * Test for GenericInMemoryCatalog.
 */
public class GenericInMemoryCatalogTest {
	private static final String IS_STREAMING = "is_streaming";

	private final String testCatalogName = "test-catalog";
	private final String db1 = "db1";
	private final String db2 = "db2";
	private final String nonExistantDatabase = "non-existant-db";

	private final String t1 = "t1";
	private final String t2 = "t2";
	private final ObjectPath path1 = new ObjectPath(db1, t1);
	private final ObjectPath path2 = new ObjectPath(db2, t2);
	private final ObjectPath path3 = new ObjectPath(db1, t2);
	private final ObjectPath path4 = new ObjectPath(db1, "t3");
	private final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
	private final ObjectPath nonExistObjectPath = ObjectPath.fromString("db1.nonexist");

	private static final String TEST_COMMENT = "test comment";
	private static final String TABLE_COMMENT = "This is my batch table";

	private static ReadableWritableCatalog catalog;

	@Before
	public void setUp() {
		catalog = new GenericInMemoryCatalog(testCatalogName);
		catalog.open();
	}

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@After
	public void close() throws Exception {
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
		catalog.close();
	}

	// ------ tables ------

	@Test
	public void testCreateTable_Streaming() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		GenericCatalogTable table = createStreamingTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(table, (GenericCatalogTable) catalog.getTable(path1));
	}

	@Test
	public void testCreateTable_Batch() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogDatabase database = catalog.getDatabase(db1);
		assertTrue(TEST_COMMENT.equals(database.getDescription().get()));

		GenericCatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		CatalogBaseTable tableCreated = catalog.getTable(path1);
		CatalogTestUtil.checkEquals(table, (GenericCatalogTable) tableCreated);
		assertEquals(TABLE_COMMENT, tableCreated.getDescription().get());

		List<String> tables = catalog.listTables(db1);

		assertEquals(1, tables.size());
		assertEquals(path1.getObjectName(), tables.get(0));

		catalog.dropTable(path1, false);
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
		catalog.createTable(path1,  CatalogTestUtil.createTable(TABLE_COMMENT), false);

		exception.expect(TableAlreadyExistException.class);
		exception.expectMessage("Table (or view) db1.t1 already exists in Catalog");
		catalog.createTable(path1, createTable(), false);
	}

	@Test
	public void testCreateTable_TableAlreadyExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		GenericCatalogTable table =  CatalogTestUtil.createTable(TABLE_COMMENT);
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(table, (GenericCatalogTable) catalog.getTable(path1));

		catalog.createTable(path1, createAnotherTable(), true);

		CatalogTestUtil.checkEquals(table, (GenericCatalogTable) catalog.getTable(path1));
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
	public void testDropTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		// Non-partitioned table
		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));
	}

	@Test
	public void testListTables() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		catalog.createTable(path1, createTable(), false);
		catalog.createTable(path3, createTable(), false);
		catalog.createTable(path4, createView(), false);

		assertEquals(3, catalog.listTables(db1).size());
		assertEquals(1, catalog.listViews(db1).size());

		catalog.dropTable(path1, false);
		catalog.dropTable(path3, false);
		catalog.dropTable(path4, false);
		catalog.dropDatabase(db1, false);
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
		GenericCatalogTable table = CatalogTestUtil.createTable(TABLE_COMMENT);
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(table, (GenericCatalogTable) catalog.getTable(path1));

		GenericCatalogTable newTable = createAnotherTable();
		catalog.alterTable(path1, newTable, false);

		assertNotEquals(table, catalog.getTable(path1));
		CatalogTestUtil.checkEquals(newTable, (GenericCatalogTable) catalog.getTable(path1));

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
		GenericCatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(table, (GenericCatalogTable) catalog.getTable(path1));

		catalog.renameTable(path1, t2, false);

		CatalogTestUtil.checkEquals(table, (GenericCatalogTable) catalog.getTable(path3));
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

	// ------ views ------

	@Test
	public void testCreateView() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		assertFalse(catalog.tableExists(path1));

		CatalogView view = createView();
		catalog.createTable(path1, view, false);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		CatalogTestUtil.checkEquals(view, (GenericCatalogView) catalog.getTable(path1));
	}

	@Test
	public void testCreateView_DatabaseNotExistException() throws Exception {
		assertFalse(catalog.databaseExists(db1));

		exception.expect(DatabaseNotExistException.class);
		exception.expectMessage("Database db1 does not exist in Catalog");
		catalog.createTable(nonExistObjectPath, createView(), false);
	}

	@Test
	public void testCreateView_TableAlreadyExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createView(), false);

		exception.expect(TableAlreadyExistException.class);
		exception.expectMessage("Table (or view) db1.t1 already exists in Catalog");
		catalog.createTable(path1, createView(), false);
	}

	@Test
	public void testCreateView_TableAlreadyExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		CatalogView view = createView();
		catalog.createTable(path1, view, false);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		CatalogTestUtil.checkEquals(view, (GenericCatalogView) catalog.getTable(path1));

		catalog.createTable(path1, createAnotherView(), true);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		CatalogTestUtil.checkEquals(view, (GenericCatalogView) catalog.getTable(path1));
	}

	@Test
	public void testDropView() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createView(), false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));
	}

	@Test
	public void testAlterView() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		CatalogView view = createView();
		catalog.createTable(path1, view, false);

		CatalogTestUtil.checkEquals(view, (GenericCatalogView) catalog.getTable(path1));

		CatalogView newView = createAnotherView();
		catalog.alterTable(path1, newView, false);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		CatalogTestUtil.checkEquals(newView, (GenericCatalogView) catalog.getTable(path1));
	}

	@Test
	public void testAlterView_TableNotExistException() throws Exception {
		exception.expect(TableNotExistException.class);
		exception.expectMessage("Table (or view) non.exist does not exist in Catalog");
		catalog.alterTable(nonExistDbPath, createTable(), false);
	}

	@Test
	public void testAlterView_TableNotExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterTable(nonExistObjectPath, createView(), true);

		assertFalse(catalog.tableExists(nonExistObjectPath));
	}

	@Test
	public void testListView() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.listTables(db1).isEmpty());

		catalog.createTable(path1, createView(), false);
		catalog.createTable(path3, createTable(), false);

		assertEquals(2, catalog.listTables(db1).size());
		assertEquals(new HashSet<>(Arrays.asList(path1.getObjectName(), path3.getObjectName())),
			new HashSet<>(catalog.listTables(db1)));
		assertEquals(Arrays.asList(path1.getObjectName()), catalog.listViews(db1));
	}

	// ------ databases ------

	@Test
	public void testCreateDb() throws Exception {
		catalog.createDatabase(db2, createDb(), false);

		assertEquals(2, catalog.listDatabases().size());
	}

	@Test
	public void testSetCurrentDatabase() throws Exception {
		assertEquals(GenericInMemoryCatalog.DEFAULT_DB, catalog.getCurrentDatabase());
		catalog.createDatabase(db2, createDb(), true);
		catalog.setCurrentDatabase(db2);
		assertEquals(db2, catalog.getCurrentDatabase());
		catalog.setCurrentDatabase(GenericInMemoryCatalog.DEFAULT_DB);
		assertEquals(GenericInMemoryCatalog.DEFAULT_DB, catalog.getCurrentDatabase());
		catalog.dropDatabase(db2, false);
	}

	@Test
	public void testSetCurrentDatabaseNegative() throws Exception {
		exception.expect(DatabaseNotExistException.class);
		exception.expectMessage("Database " + this.nonExistantDatabase + " does not exist in Catalog");
		catalog.setCurrentDatabase(this.nonExistantDatabase);
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
	public void testDropDb_databaseIsNotEmpty() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		exception.expect(DatabaseNotEmptyException.class);
		exception.expectMessage("Database db1 in Catalog test-catalog is not empty");
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

	@Test
	public void testRenameView() throws Exception {
		catalog.createDatabase("db1", new GenericCatalogDatabase(new HashMap<>()), false);
		GenericCatalogView view = new GenericCatalogView("select * from t1",
			"select * from db1.t1", createTableSchema(), new HashMap<>());
		ObjectPath viewPath1 = new ObjectPath(db1, "view1");
		catalog.createTable(viewPath1, view, false);
		assertTrue(catalog.tableExists(viewPath1));
		catalog.renameTable(viewPath1, "view2", false);
		assertFalse(catalog.tableExists(viewPath1));
		ObjectPath viewPath2 = new ObjectPath(db1, "view2");
		assertTrue(catalog.tableExists(viewPath2));
		catalog.dropTable(viewPath2, false);
	}

	// ------ utilities ------

	private GenericCatalogTable createStreamingTable() {
		return CatalogTestUtil.createTable(
			createTableSchema(),
			getStreamingTableProperties(), TABLE_COMMENT);
	}

	private GenericCatalogTable createTable() {
		return CatalogTestUtil.createTable(
			createTableSchema(),
			getBatchTableProperties(), TABLE_COMMENT);
	}

	private GenericCatalogTable createAnotherTable() {
		return CatalogTestUtil.createTable(
			createAnotherTableSchema(),
			getBatchTableProperties(), TABLE_COMMENT);
	}

	private CatalogDatabase createDb() {
		return new GenericCatalogDatabase(new HashMap<String, String>() {{
			put("k1", "v1");
		}}, TEST_COMMENT);
	}

	private Map<String, String> getBatchTableProperties() {
		return new HashMap<String, String>() {{
			put(IS_STREAMING, "false");
		}};
	}

	private Map<String, String> getStreamingTableProperties() {
		return new HashMap<String, String>() {{
			put(IS_STREAMING, "true");
		}};
	}

	private CatalogDatabase createAnotherDb() {
		return new GenericCatalogDatabase(new HashMap<String, String>() {{
			put("k2", "v2");
		}}, "this is another database.");
	}

	private TableSchema createTableSchema() {
		return new TableSchema(
			new String[] {"first", "second", "third"},
			new TypeInformation[] {
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
			}
		);
	}

	private TableSchema createAnotherTableSchema() {
		return new TableSchema(
			new String[] {"first2", "second", "third"},
			new TypeInformation[] {
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO
			}
		);
	}

	private CatalogView createView() {
		return new GenericCatalogView(
			String.format("select * from %s", t1),
			String.format("select * from %s.%s", testCatalogName, path1.getFullName()),
			createTableSchema(),
			new HashMap<>(),
			"This is a view");
	}

	private CatalogView createAnotherView() {
		return new GenericCatalogView(
			String.format("select * from %s", t2),
			String.format("select * from %s.%s", testCatalogName, path2.getFullName()),
			createTableSchema(),
			new HashMap<>(),
			"This is another view");
	}

}
