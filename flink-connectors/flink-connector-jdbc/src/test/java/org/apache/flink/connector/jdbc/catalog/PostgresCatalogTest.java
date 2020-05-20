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

package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link PostgresCatalog}.
 */
public class PostgresCatalogTest extends PostgresCatalogTestBase {

	// ------ databases ------

	@Test
	public void testGetDb_DatabaseNotExistException() throws Exception {
		exception.expect(DatabaseNotExistException.class);
		exception.expectMessage("Database nonexistent does not exist in Catalog");
		catalog.getDatabase("nonexistent");
	}

	@Test
	public void testListDatabases() {
		List<String> actual = catalog.listDatabases();

		assertEquals(
			Arrays.asList("postgres", "test"),
			actual
		);
	}

	@Test
	public void testDbExists() throws Exception {
		assertFalse(catalog.databaseExists("nonexistent"));

		assertTrue(catalog.databaseExists(PostgresCatalog.DEFAULT_DATABASE));
	}

	// ------ tables ------

	@Test
	public void testListTables() throws DatabaseNotExistException {
		List<String> actual = catalog.listTables(PostgresCatalog.DEFAULT_DATABASE);

		assertEquals(Arrays.asList("public.dt", "public.dt2", "public.t1", "public.t4", "public.t5"), actual);

		actual = catalog.listTables(TEST_DB);

		assertEquals(Arrays.asList("public.t2", "test_schema.t3"), actual);
	}

	@Test
	public void testListTables_DatabaseNotExistException() throws DatabaseNotExistException {
		exception.expect(DatabaseNotExistException.class);
		catalog.listTables("postgres/nonexistschema");
	}

	@Test
	public void testTableExists() {
		assertFalse(catalog.tableExists(new ObjectPath(TEST_DB, "nonexist")));

		assertTrue(catalog.tableExists(new ObjectPath(PostgresCatalog.DEFAULT_DATABASE, TABLE1)));
		assertTrue(catalog.tableExists(new ObjectPath(TEST_DB, TABLE2)));
		assertTrue(catalog.tableExists(new ObjectPath(TEST_DB, "test_schema.t3")));
	}

	@Test
	public void testGetTables_TableNotExistException() throws TableNotExistException {
		exception.expect(TableNotExistException.class);
		catalog.getTable(new ObjectPath(TEST_DB, PostgresTablePath.toFlinkTableName(TEST_SCHEMA, "anytable")));
	}

	@Test
	public void testGetTables_TableNotExistException_NoSchema() throws TableNotExistException {
		exception.expect(TableNotExistException.class);
		catalog.getTable(new ObjectPath(TEST_DB, PostgresTablePath.toFlinkTableName("nonexistschema", "anytable")));
	}

	@Test
	public void testGetTables_TableNotExistException_NoDb() throws TableNotExistException {
		exception.expect(TableNotExistException.class);
		catalog.getTable(new ObjectPath("nonexistdb", PostgresTablePath.toFlinkTableName(TEST_SCHEMA, "anytable")));
	}

	@Test
	public void testGetTable() throws org.apache.flink.table.catalog.exceptions.TableNotExistException {
		// test postgres.public.user1
		TableSchema schema = getSimpleTable().schema;

		CatalogBaseTable table = catalog.getTable(new ObjectPath("postgres", TABLE1));

		assertEquals(schema, table.getSchema());

		table = catalog.getTable(new ObjectPath("postgres", "public.t1"));

		assertEquals(schema, table.getSchema());

		// test testdb.public.user2
		table = catalog.getTable(new ObjectPath(TEST_DB, TABLE2));

		assertEquals(schema, table.getSchema());

		table = catalog.getTable(new ObjectPath(TEST_DB, "public.t2"));

		assertEquals(schema, table.getSchema());

		// test testdb.testschema.user2
		table = catalog.getTable(new ObjectPath(TEST_DB, TEST_SCHEMA + ".t3"));

		assertEquals(schema, table.getSchema());

	}

	@Test
	public void testPrimitiveDataTypes() throws TableNotExistException {
		CatalogBaseTable table = catalog.getTable(new ObjectPath(PostgresCatalog.DEFAULT_DATABASE, TABLE_PRIMITIVE_TYPE));

		assertEquals(getPrimitiveTable().schema, table.getSchema());
	}

	@Test
	public void tesArrayDataTypes() throws TableNotExistException {
		CatalogBaseTable table = catalog.getTable(new ObjectPath(PostgresCatalog.DEFAULT_DATABASE, TABLE_ARRAY_TYPE));

		assertEquals(getArrayTable().schema, table.getSchema());
	}
}
