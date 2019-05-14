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

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.runtime.utils.CommonTestData;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.flink.table.catalog.CatalogStructureBuilder.database;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.root;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CatalogManager}. See also {@link CatalogManagerPathResolutionTest}.
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

		assertEquals(1, manager.getCatalogs().size());
		assertFalse(manager.getCatalogs().contains(TEST_CATALOG_NAME));

		manager.registerCatalog(TEST_CATALOG_NAME, new GenericInMemoryCatalog(TEST_CATALOG_NAME));

		assertEquals(2, manager.getCatalogs().size());
		assertTrue(manager.getCatalogs().contains(TEST_CATALOG_NAME));
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
	public void testRegisterCatalogWithExistingExternalCatalog() throws Exception {
		thrown.expect(CatalogException.class);

		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME))
			.externalCatalog(TEST_CATALOG_NAME)
			.build();

		manager.registerCatalog(TEST_CATALOG_NAME, new GenericInMemoryCatalog(TEST_CATALOG_NAME));
	}

	@Test
	public void testRegisterExternalCatalogWithExistingName() throws Exception {
		thrown.expect(CatalogException.class);
		thrown.expectMessage("An external catalog named [test] already exists.");

		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME))
			.catalog(TEST_CATALOG_NAME, database(TEST_CATALOG_DEFAULT_DB_NAME))
			.build();

		manager.registerExternalCatalog(TEST_CATALOG_NAME, CommonTestData.getInMemoryTestCatalog(false));
	}

	@Test
	public void testCannotSetExternalCatalogAsDefault() throws Exception {
		thrown.expect(CatalogException.class);
		thrown.expectMessage("An external catalog cannot be set as the default one.");

		CatalogManager manager = root()
			.externalCatalog("ext")
			.build();
		manager.setCurrentCatalog("ext");
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
}
