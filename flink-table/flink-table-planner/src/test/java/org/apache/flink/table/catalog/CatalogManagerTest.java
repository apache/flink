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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.catalog.CatalogStructureBuilder.BUILTIN_CATALOG_NAME;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.database;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.root;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.table;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CatalogManager}. See also {@link PathResolutionTest}.
 */
public class CatalogManagerTest extends TestLogger {

	private static final String TEST_CATALOG_NAME = "test";
	private static final String TEST_CATALOG_DEFAULT_DB_NAME = "test";
	private static final String BUILTIN_DEFAULT_DATABASE_NAME = "default";

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testRegisterCatalog() throws Exception {
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME))
			.build();

		assertEquals(1, manager.listCatalogs().size());
		assertFalse(manager.listCatalogs().contains(TEST_CATALOG_NAME));

		manager.registerCatalog(TEST_CATALOG_NAME, new GenericInMemoryCatalog(TEST_CATALOG_NAME));

		assertEquals(2, manager.listCatalogs().size());
		assertTrue(manager.listCatalogs().contains(TEST_CATALOG_NAME));
	}

	@Test
	public void testSetCurrentCatalog() throws Exception {
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME))
			.catalog(
				TEST_CATALOG_NAME,
				database(TEST_CATALOG_DEFAULT_DB_NAME))
			.build();

		assertEquals(CatalogStructureBuilder.BUILTIN_CATALOG_NAME, manager.getCurrentCatalog());
		assertEquals(BUILTIN_DEFAULT_DATABASE_NAME, manager.getCurrentDatabase());

		manager.setCurrentCatalog(TEST_CATALOG_NAME);

		assertEquals(TEST_CATALOG_NAME, manager.getCurrentCatalog());
		assertEquals(TEST_CATALOG_DEFAULT_DB_NAME, manager.getCurrentDatabase());
	}

	@Test
	public void testRegisterCatalogWithExistingName() throws Exception {
		thrown.expect(CatalogException.class);

		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME))
			.catalog(TEST_CATALOG_NAME, database(TEST_CATALOG_DEFAULT_DB_NAME))
			.build();

		manager.registerCatalog(TEST_CATALOG_NAME, new GenericInMemoryCatalog(TEST_CATALOG_NAME));
	}

	@Test
	public void testIgnoreTemporaryTableExists() throws Exception {
		ObjectIdentifier tempIdentifier = ObjectIdentifier.of(
			BUILTIN_CATALOG_NAME,
			BUILTIN_DEFAULT_DATABASE_NAME,
			"temp");
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME))
			.build();

		CatalogTest.TestTable table = new CatalogTest.TestTable();
		manager.createTemporaryTable(table, tempIdentifier, true);
		CatalogTest.TestTable anotherTable = new CatalogTest.TestTable();
		manager.createTemporaryTable(anotherTable, tempIdentifier, true);
		assertThat(manager.getTable(tempIdentifier).get().isTemporary(), equalTo(true));
		assertThat(manager.getTable(tempIdentifier).get().getTable(), equalTo(table));
	}

	@Test
	public void testTemporaryTableExists() throws Exception {
		ObjectIdentifier tempIdentifier = ObjectIdentifier.of(
			BUILTIN_CATALOG_NAME,
			BUILTIN_DEFAULT_DATABASE_NAME,
			"temp");
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME))
			.temporaryTable(tempIdentifier)
			.build();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(String.format("Temporary table '%s' already exists", tempIdentifier));
		manager.createTemporaryTable(new CatalogTest.TestTable(), tempIdentifier, false);
	}

	@Test
	public void testDropTableWhenTemporaryTableExists() throws Exception {
		ObjectIdentifier identifier = ObjectIdentifier.of(BUILTIN_CATALOG_NAME, BUILTIN_DEFAULT_DATABASE_NAME, "test");
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME, table("test")))
			.temporaryTable(identifier)
			.build();

		thrown.expect(ValidationException.class);
		thrown.expectMessage("Temporary table with identifier '`builtin`.`default`.`test`' exists." +
			" Drop it first before removing the permanent table.");
		manager.dropTable(identifier, false);

	}

	@Test(expected = ValidationException.class)
	public void testDropTemporaryNonExistingTable() throws Exception {
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME, table("test")))
			.build();

		manager.dropTemporaryTable(manager.qualifyIdentifier(UnresolvedIdentifier.of("test")), false);
	}

	@Test
	public void testDropTemporaryTable() throws Exception {
		ObjectIdentifier identifier = ObjectIdentifier.of(BUILTIN_CATALOG_NAME, BUILTIN_DEFAULT_DATABASE_NAME, "test");
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME, table("test")))
			.temporaryTable(identifier)
			.build();

		manager.dropTemporaryTable(manager.qualifyIdentifier(UnresolvedIdentifier.of("test")), false);
	}

	@Test
	public void testListTables() throws Exception {
		ObjectIdentifier identifier1 = ObjectIdentifier.of(TEST_CATALOG_NAME, TEST_CATALOG_DEFAULT_DB_NAME, "test1");
		ObjectIdentifier identifier2 = ObjectIdentifier.of(
			BUILTIN_CATALOG_NAME,
			BUILTIN_DEFAULT_DATABASE_NAME,
			"test2");
		ObjectIdentifier viewIdentifier = ObjectIdentifier.of(
			BUILTIN_CATALOG_NAME,
			BUILTIN_DEFAULT_DATABASE_NAME,
			"testView");
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME, table("test_in_builtin")))
			.catalog(TEST_CATALOG_NAME, database(TEST_CATALOG_DEFAULT_DB_NAME, table("test_in_catalog")))
			.temporaryTable(identifier1)
			.temporaryTable(identifier2)
			.temporaryView(viewIdentifier, "SELECT * FROM none")
			.build();

		manager.setCurrentCatalog(BUILTIN_CATALOG_NAME);
		manager.setCurrentDatabase(BUILTIN_DEFAULT_DATABASE_NAME);

		assertThat(
			manager.listTables(),
			equalTo(
				setOf(
					"test2",
					"testView",
					"test_in_builtin"
				)));
	}

	@Test
	public void testListTemporaryTables() throws Exception {
		ObjectIdentifier identifier1 = ObjectIdentifier.of(TEST_CATALOG_NAME, TEST_CATALOG_DEFAULT_DB_NAME, "test1");
		ObjectIdentifier identifier2 = ObjectIdentifier.of(
			BUILTIN_CATALOG_NAME,
			BUILTIN_DEFAULT_DATABASE_NAME,
			"test2");
		ObjectIdentifier viewIdentifier = ObjectIdentifier.of(
			BUILTIN_CATALOG_NAME,
			BUILTIN_DEFAULT_DATABASE_NAME,
			"testView");
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME, table("test_in_builtin")))
			.catalog(TEST_CATALOG_NAME, database(TEST_CATALOG_DEFAULT_DB_NAME, table("test_in_catalog")))
			.temporaryTable(identifier1)
			.temporaryTable(identifier2)
			.temporaryView(viewIdentifier, "SELECT * FROM none")
			.build();

		manager.setCurrentCatalog(BUILTIN_CATALOG_NAME);
		manager.setCurrentDatabase(BUILTIN_DEFAULT_DATABASE_NAME);

		assertThat(
			manager.listTemporaryTables(),
			equalTo(
				setOf(
					"test2",
					"testView"
				)));
	}

	@Test
	public void testListTemporaryViews() throws Exception {
		ObjectIdentifier tableIdentifier = ObjectIdentifier.of(
			TEST_CATALOG_NAME,
			TEST_CATALOG_DEFAULT_DB_NAME,
			"table");
		ObjectIdentifier identifier1 = ObjectIdentifier.of(TEST_CATALOG_NAME, TEST_CATALOG_DEFAULT_DB_NAME, "test1");
		ObjectIdentifier identifier2 = ObjectIdentifier.of(
			BUILTIN_CATALOG_NAME,
			BUILTIN_DEFAULT_DATABASE_NAME,
			"test2");

		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME, table("test_in_builtin")))
			.catalog(TEST_CATALOG_NAME, database(TEST_CATALOG_DEFAULT_DB_NAME, table("test_in_catalog")))
			.temporaryTable(tableIdentifier)
			.temporaryView(identifier1, "SELECT * FROM none")
			.temporaryView(identifier2, "SELECT * FROM none")
			.build();

		manager.setCurrentCatalog(TEST_CATALOG_NAME);
		manager.setCurrentDatabase(TEST_CATALOG_DEFAULT_DB_NAME);

		assertThat(manager.listTemporaryViews(), equalTo(setOf("test1")));
	}

	@Test
	public void testSetNonExistingCurrentCatalog() throws Exception {
		thrown.expect(CatalogException.class);
		thrown.expectMessage("A catalog with name [nonexistent] does not exist.");

		CatalogManager manager = root().build();
		manager.setCurrentCatalog("nonexistent");
	}

	@Test
	public void testSetNonExistingCurrentDatabase() throws Exception {
		thrown.expect(CatalogException.class);
		thrown.expectMessage("A database with name [nonexistent] does not exist in the catalog: [builtin].");

		CatalogManager manager = root().build();
		// This catalog does not exist in the builtin catalog
		manager.setCurrentDatabase("nonexistent");
	}

	private Set<String> setOf(String... element) {
		return Stream.of(element).collect(Collectors.toSet());
	}
}
