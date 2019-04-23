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

import org.apache.flink.table.api.CatalogNotExistException;

import org.apache.calcite.schema.SchemaPlus;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for FlinkCatalogManager.
 */
public class FlinkCatalogManagerTest {
	private static final String TEST_CATALOG_NAME = "test";
	private static final ReadableCatalog TEST_CATALOG = new GenericInMemoryCatalog(TEST_CATALOG_NAME);

	private FlinkCatalogManager manager;
	private SchemaPlus rootSchema;

	@Before
	public void init() {
		manager = new FlinkCatalogManager();
		rootSchema = manager.getRootSchema();
	}

	@Test
	public void testRegisterCatalog() throws Exception {
		assertEquals(1, manager.getCatalogNames().size());
		assertFalse(manager.getCatalogNames().contains(TEST_CATALOG_NAME));
		assertFalse(rootSchema.getSubSchemaNames().contains(TEST_CATALOG_NAME));

		manager.registerCatalog(TEST_CATALOG_NAME, TEST_CATALOG);

		assertEquals(2, manager.getCatalogNames().size());
		assertTrue(manager.getCatalogNames().contains(TEST_CATALOG_NAME));
		assertTrue(rootSchema.getSubSchemaNames().contains(TEST_CATALOG_NAME));
	}

	@Test
	public void testSetCurrentCatalog() throws Exception {
		manager.registerCatalog(TEST_CATALOG_NAME, TEST_CATALOG);

		assertEquals(manager.getCatalog(FlinkCatalogManager.BUILTIN_CATALOG_NAME), manager.getCurrentCatalog());

		manager.setCurrentCatalog(TEST_CATALOG_NAME);

		assertEquals(manager.getCatalog(TEST_CATALOG_NAME), manager.getCurrentCatalog());
	}

	@Test(expected = CatalogNotExistException.class)
	public void testSetNonExistCurrentCatalog() throws Exception {
		manager.setCurrentCatalog("nonexist");
	}
}
