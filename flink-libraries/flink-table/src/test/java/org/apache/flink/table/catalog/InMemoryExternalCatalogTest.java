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
import org.apache.flink.table.api.CatalogAlreadyExistException;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.Schema;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for InMemoryExternalCatalog.
 */
public class InMemoryExternalCatalogTest {
	private final String databaseName = "db1";
	private final String tableName = "t1";
	private final String nonExisted = "nonexisted";

	private InMemoryExternalCatalog catalog;

	@Before
	public void setUp() {
		catalog = new InMemoryExternalCatalog(databaseName);
	}

	@Test
	public void testCreateTable() {
		assertTrue(catalog.listTables().isEmpty());
		catalog.createTable(tableName, createTableInstance(), false);
		List<String> tables = catalog.listTables();
		assertEquals(1, tables.size());
		assertEquals(tableName, tables.get(0));
	}

	@Test(expected = TableAlreadyExistException.class)
	public void testCreateExistedTable() {
		catalog.createTable(tableName, createTableInstance(), false);
		catalog.createTable(tableName, createTableInstance(), false);
	}

	@Test
	public void testGetTable() {
		ExternalCatalogTable originTable = createTableInstance();
		catalog.createTable(tableName, originTable, false);
		assertEquals(catalog.getTable(tableName), originTable);
	}

	@Test(expected = TableNotExistException.class)
	public void testGetNotExistTable() {
		catalog.getTable(nonExisted);
	}

	@Test
	public void testAlterTable() {
		ExternalCatalogTable table = createTableInstance();
		catalog.createTable(tableName, table, false);
		assertEquals(catalog.getTable(tableName), table);
		ExternalCatalogTable newTable = createTableInstance(new String[]{"number"}, new TypeInformation[]{Types.INT()});
		catalog.alterTable(tableName, newTable, false);
		ExternalCatalogTable currentTable = catalog.getTable(tableName);
		// validate the table is really replaced after alter table
		assertNotEquals(table, currentTable);
		assertEquals(newTable, currentTable);
	}

	@Test(expected = TableNotExistException.class)
	public void testAlterNotExistTable() {
		catalog.alterTable(nonExisted, createTableInstance(), false);
	}

	@Test
	public void testDropTable() {
		catalog.createTable(tableName, createTableInstance(), false);
		assertTrue(catalog.listTables().contains(tableName));
		catalog.dropTable(tableName, false);
		assertFalse(catalog.listTables().contains(tableName));
	}

	@Test(expected = TableNotExistException.class)
	public void testDropNotExistTable() {
		catalog.dropTable("nonexisted", false);
	}

	@Test(expected = CatalogNotExistException.class)
	public void testGetNotExistDatabase() {
		catalog.getSubCatalog("notexistedDb");
	}

	@Test
	public void testCreateDatabase() {
		catalog.createSubCatalog("db2", new InMemoryExternalCatalog("db2"), false);
		assertEquals(1, catalog.listSubCatalogs().size());
	}

	@Test(expected = CatalogAlreadyExistException.class)
	public void testCreateExistedDatabase() {
		catalog.createSubCatalog("existed", new InMemoryExternalCatalog("existed"), false);

		assertNotNull(catalog.getSubCatalog("existed"));
		List<String> databases = catalog.listSubCatalogs();
		assertEquals(1, databases.size());
		assertEquals("existed", databases.get(0));

		catalog.createSubCatalog("existed", new InMemoryExternalCatalog("existed"), false);
	}

	@Test
	public void testNestedCatalog() {
		CrudExternalCatalog sub = new InMemoryExternalCatalog("sub");
		CrudExternalCatalog sub1 = new InMemoryExternalCatalog("sub1");
		catalog.createSubCatalog("sub", sub, false);
		sub.createSubCatalog("sub1", sub1, false);
		sub1.createTable("table", createTableInstance(), false);
		List<String> tables = catalog.getSubCatalog("sub").getSubCatalog("sub1").listTables();
		assertEquals(1, tables.size());
		assertEquals("table", tables.get(0));
	}

	private ExternalCatalogTable createTableInstance() {
		TestConnectorDesc connDesc = getTestConnectorDesc();
		Schema schemaDesc = new Schema()
			.field("first", BasicTypeInfo.STRING_TYPE_INFO)
			.field("second", BasicTypeInfo.INT_TYPE_INFO);
		return ExternalCatalogTable.builder(connDesc)
			.withSchema(schemaDesc)
			.asTableSource();
	}

	private ExternalCatalogTable createTableInstance(String[] fieldNames, TypeInformation[] fieldTypes) {
		TestConnectorDesc connDesc = getTestConnectorDesc();
		Schema schemaDesc = new Schema();

		for (int i = 0; i < fieldNames.length; i++) {
			schemaDesc.field(fieldNames[i], fieldTypes[i]);
		}

		return ExternalCatalogTable.builder(connDesc)
			.withSchema(schemaDesc)
			.asTableSource();
	}

	private static TestConnectorDesc getTestConnectorDesc() {
		return new TestConnectorDesc("test", 1, false);
	}

	private static class TestConnectorDesc extends ConnectorDescriptor {

		public TestConnectorDesc(String type, int version, boolean formatNeeded) {
			super(type, version, formatNeeded);
		}

		@Override
		protected Map<String, String> toConnectorProperties() {
			return new HashMap<>();
		}
	}
}
