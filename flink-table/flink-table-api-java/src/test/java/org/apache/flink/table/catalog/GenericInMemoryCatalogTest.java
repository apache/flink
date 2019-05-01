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

import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.plan.stats.TableStats;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for GenericInMemoryCatalog.
 */
public class GenericInMemoryCatalogTest extends CatalogTestBase {

	@BeforeClass
	public static void init() {
		catalog = new GenericInMemoryCatalog(TEST_CATALOG_NAME);
		catalog.open();
	}

	@After
	public void close() throws Exception {
		if (catalog.functionExists(path1)) {
			catalog.dropFunction(path1, true);
		}
	}

	// ------ tables ------

	@Test
	public void testDropTable_partitionedTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		CatalogPartition catalogPartition = createPartition();
		CatalogPartitionSpec catalogPartitionSpec = createPartitionSpec();
		catalog.createPartition(path1, catalogPartitionSpec, catalogPartition, false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));
		assertFalse(catalog.partitionExists(path1, catalogPartitionSpec));
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
	public void testRenameTable_partitionedTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createPartitionedTable();
		catalog.createTable(path1, table, false);
		CatalogPartition catalogPartition = createPartition();
		CatalogPartitionSpec catalogPartitionSpec = createPartitionSpec();
		catalog.createPartition(path1, catalogPartitionSpec, catalogPartition, false);

		CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));
		assertTrue(catalog.partitionExists(path1, catalogPartitionSpec));

		catalog.renameTable(path1, t2, false);

		CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path3));
		assertTrue(catalog.partitionExists(path3, catalogPartitionSpec));
		assertFalse(catalog.tableExists(path1));
		assertFalse(catalog.partitionExists(path1, catalogPartitionSpec));
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

	// ------ partitions ------

	@Test
	public void testCreatePartition() throws Exception {
		CatalogTable table = createPartitionedTable();
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, table, false);

		assertTrue(catalog.listPartitions(path1).isEmpty());

		CatalogPartitionSpec partitionSpec = createPartitionSpec();
		catalog.createPartition(path1, partitionSpec, createPartition(), false);

		assertEquals(Arrays.asList(partitionSpec), catalog.listPartitions(path1));
		assertEquals(Arrays.asList(partitionSpec), catalog.listPartitions(path1, createPartitionSpecSubset()));
		CatalogTestUtil.checkEquals(createPartition(), catalog.getPartition(path1, createPartitionSpec()));

		CatalogPartitionSpec anotherPartitionSpec = createAnotherPartitionSpec();
		CatalogPartition anotherPartition = createAnotherPartition();
		catalog.createPartition(path1, anotherPartitionSpec, anotherPartition, false);

		assertEquals(Arrays.asList(partitionSpec, anotherPartitionSpec), catalog.listPartitions(path1));
		assertEquals(Arrays.asList(partitionSpec, anotherPartitionSpec), catalog.listPartitions(path1, createPartitionSpecSubset()));
		CatalogTestUtil.checkEquals(anotherPartition, catalog.getPartition(path1, anotherPartitionSpec));

		CatalogPartitionSpec invalid = createInvalidPartitionSpecSubset();
		assertTrue(catalog.listPartitions(path1, invalid).isEmpty());
	}

	@Test
	public void testCreatePartition_TableNotExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(TableNotExistException.class);
		exception.expectMessage(
			String.format("Table (or view) %s does not exist in Catalog %s.", path1.getFullName(), TEST_CATALOG_NAME));
		catalog.createPartition(path1, createPartitionSpec(), createPartition(), false);
	}

	@Test
	public void testCreatePartition_TableNotPartitionedException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		exception.expect(TableNotPartitionedException.class);
		exception.expectMessage(
			String.format("Table %s in catalog %s is not partitioned.", path1.getFullName(), TEST_CATALOG_NAME));
		catalog.createPartition(path1, createPartitionSpec(), createPartition(), false);
	}

	@Test
	public void testCreatePartition_PartitionSpecInvalidException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createPartitionedTable();
		catalog.createTable(path1, table, false);

		CatalogPartitionSpec partitionSpec = createInvalidPartitionSpecSubset();
		exception.expect(PartitionSpecInvalidException.class);
		exception.expectMessage(
			String.format("PartitionSpec %s does not match partition keys %s of table %s in catalog %s.",
				partitionSpec, table.getPartitionKeys(), path1.getFullName(), TEST_CATALOG_NAME));
		catalog.createPartition(path1, partitionSpec, createPartition(), false);
	}

	@Test
	public void testCreatePartition_PartitionAlreadyExistsException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		CatalogPartition partition = createPartition();
		catalog.createPartition(path1, createPartitionSpec(), partition, false);

		CatalogPartitionSpec partitionSpec = createPartitionSpec();

		exception.expect(PartitionAlreadyExistsException.class);
		exception.expectMessage(
			String.format("Partition %s of table %s in catalog %s already exists.",
				partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
		catalog.createPartition(path1, partitionSpec, createPartition(), false);
	}

	@Test
	public void testCreatePartition_PartitionAlreadyExists_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);

		CatalogPartitionSpec partitionSpec = createPartitionSpec();
		catalog.createPartition(path1, partitionSpec, createPartition(), false);
		catalog.createPartition(path1, partitionSpec, createPartition(), true);
	}

	@Test
	public void testDropPartition() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.createPartition(path1, createPartitionSpec(), createPartition(), false);

		assertEquals(Arrays.asList(createPartitionSpec()), catalog.listPartitions(path1));

		catalog.dropPartition(path1, createPartitionSpec(), false);

		assertEquals(Arrays.asList(), catalog.listPartitions(path1));
	}

	@Test
	public void testDropPartition_PartitionNotExistException_TableNotExist() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogPartitionSpec partitionSpec = createPartitionSpec();

		exception.expect(PartitionNotExistException.class);
		exception.expectMessage(
			String.format("Partition %s of table %s in catalog %s does not exist.",
				partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
		catalog.dropPartition(path1, partitionSpec, false);
	}

	@Test
	public void testDropPartition_PartitionNotExistException_TableNotPartitioned() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);
		CatalogPartitionSpec partitionSpec = createPartitionSpec();

		exception.expect(PartitionNotExistException.class);
		exception.expectMessage(
			String.format("Partition %s of table %s in catalog %s does not exist.",
				partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
		catalog.dropPartition(path1, partitionSpec, false);
	}

	@Test
	public void testDropPartition_PartitionNotExistException_PartitionSpecInvalid() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createPartitionedTable();
		catalog.createTable(path1, table, false);

		CatalogPartitionSpec partitionSpec = createInvalidPartitionSpecSubset();
		exception.expect(PartitionNotExistException.class);
		exception.expectMessage(
			String.format("Partition %s of table %s in catalog %s does not exist.",
				partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
		catalog.dropPartition(path1, partitionSpec, false);
	}

	@Test
	public void testDropPartition_PartitionNotExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);

		CatalogPartitionSpec partitionSpec = createPartitionSpec();
		exception.expect(PartitionNotExistException.class);
		exception.expectMessage(
			String.format("Partition %s of table %s in catalog %s does not exist.", partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
		catalog.dropPartition(path1, partitionSpec, false);
	}

	@Test
	public void testDropPartition_PartitionNotExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.dropPartition(path1, createPartitionSpec(), true);
	}

	@Test
	public void testAlterPartition() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);

		CatalogPartitionSpec partitionSpec = createPartitionSpec();
		catalog.createPartition(path1, partitionSpec, createPartition(), false);

		assertEquals(Arrays.asList(partitionSpec), catalog.listPartitions(path1));

		CatalogPartition cp = catalog.getPartition(path1, createPartitionSpec());
		CatalogTestUtil.checkEquals(createPartition(), cp);

		assertNull(cp.getProperties().get("k"));

		Map<String, String> partitionProperties = getBatchTableProperties();
		partitionProperties.put("k", "v");

		CatalogPartition another = createPartition(partitionProperties);
		catalog.alterPartition(path1, createPartitionSpec(), another, false);

		assertEquals(Arrays.asList(createPartitionSpec()), catalog.listPartitions(path1));

		cp = catalog.getPartition(path1, createPartitionSpec());
		CatalogTestUtil.checkEquals(another, cp);

		assertEquals("v", cp.getProperties().get("k"));
	}

	@Test
	public void testAlterPartition_PartitionNotExistException_TableNotExist() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		CatalogPartitionSpec partitionSpec = createPartitionSpec();
		exception.expect(PartitionNotExistException.class);
		exception.expectMessage(
			String.format("Partition %s of table %s in catalog %s does not exist.",
				partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
		catalog.alterPartition(path1, partitionSpec, createPartition(), false);
	}

	@Test
	public void testAlterPartition_PartitionNotExistException_TableNotPartitioned() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		CatalogPartitionSpec partitionSpec = createPartitionSpec();
		exception.expect(PartitionNotExistException.class);
		exception.expectMessage(
			String.format("Partition %s of table %s in catalog %s does not exist.",
				partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
		catalog.alterPartition(path1, partitionSpec, createPartition(), false);
	}

	@Test
	public void testAlterPartition_PartitionNotExistException_PartitionSpecInvalid() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createPartitionedTable();
		catalog.createTable(path1, table, false);

		CatalogPartitionSpec partitionSpec = createInvalidPartitionSpecSubset();
		exception.expect(PartitionNotExistException.class);
		exception.expectMessage(
			String.format("Partition %s of table %s in catalog %s does not exist.",
				partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
		catalog.alterPartition(path1, partitionSpec, createPartition(), false);
	}

	@Test
	public void testAlterPartition_PartitionNotExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);

		CatalogPartition catalogPartition = createPartition();
		CatalogPartitionSpec partitionSpec = createPartitionSpec();
		exception.expect(PartitionNotExistException.class);
		exception.expectMessage(
			String.format("Partition %s of table %s in catalog %s does not exist.",
				partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
		catalog.alterPartition(path1, partitionSpec, catalogPartition, false);
	}

	@Test
	public void testAlterPartition_PartitionNotExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.alterPartition(path1, createPartitionSpec(), createPartition(), true);
	}

	@Test
	public void testGetPartition_PartitionNotExistException_TableNotExist() throws Exception {
		exception.expect(PartitionNotExistException.class);
		catalog.getPartition(path1, createPartitionSpec());
	}

	@Test
	public void testGetPartition_PartitionNotExistException_TableNotPartitioned() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);
		CatalogPartitionSpec partitionSpec = createPartitionSpec();
		exception.expect(PartitionNotExistException.class);
		exception.expectMessage(
			String.format("Partition %s of table %s in catalog %s does not exist.", partitionSpec,
				path1.getFullName(), TEST_CATALOG_NAME));
		catalog.getPartition(path1, partitionSpec);
	}

	@Test
	public void testGetPartition_PartitionSpecInvalidException_invalidPartitionSpec() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createPartitionedTable();
		catalog.createTable(path1, table, false);

		CatalogPartitionSpec partitionSpec = createInvalidPartitionSpecSubset();
		exception.expect(PartitionNotExistException.class);
		exception.expectMessage(
			String.format("Partition %s of table %s in catalog %s does not exist.",
				partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
		catalog.getPartition(path1, partitionSpec);
	}

	@Test
	public void testGetPartition_PartitionNotExistException_PartitionSpecInvalid_sizeNotEqual() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createPartitionedTable();
		catalog.createTable(path1, table, false);

		CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(
			new HashMap<String, String>() {{
				put("second", "bob");
			}}
		);
		exception.expect(PartitionNotExistException.class);
		exception.expectMessage(
			String.format("Partition %s of table %s in catalog %s does not exist.",
				partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
		catalog.getPartition(path1, partitionSpec);
	}

	@Test
	public void testGetPartition_PartitionNotExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);

		CatalogPartitionSpec partitionSpec = createPartitionSpec();
		exception.expect(PartitionNotExistException.class);
		exception.expectMessage(
			String.format("Partition %s of table %s in catalog %s does not exist.",
				partitionSpec, path1.getFullName(), TEST_CATALOG_NAME));
		catalog.getPartition(path1, partitionSpec);
	}

	@Test
	public void testPartitionExists() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.createPartition(path1, createPartitionSpec(), createPartition(), false);

		assertTrue(catalog.partitionExists(path1, createPartitionSpec()));
		assertFalse(catalog.partitionExists(path2, createPartitionSpec()));
		assertFalse(catalog.partitionExists(ObjectPath.fromString("non.exist"), createPartitionSpec()));
	}

	// ------ functions ------

	@Test
	public void testCreateFunction() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		assertFalse(catalog.functionExists(path1));

		catalog.createFunction(path1, createFunction(), false);

		assertTrue(catalog.functionExists(path1));

		catalog.dropFunction(path1, false);
		catalog.dropDatabase(db1, false);
	}

	@Test
	public void testCreateFunction_DatabaseNotExistException() throws Exception {
		assertFalse(catalog.databaseExists(db1));

		exception.expect(DatabaseNotExistException.class);
		exception.expectMessage("Database db1 does not exist in Catalog");
		catalog.createFunction(path1, createFunction(), false);
	}

	@Test
	public void testCreateFunction_FunctionAlreadyExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createFunction(path1, createFunction(), false);

		exception.expect(FunctionAlreadyExistException.class);
		exception.expectMessage("Function db1.t1 already exists in Catalog");
		catalog.createFunction(path1, createFunction(), false);
	}

	@Test
	public void testCreateFunction_FunctionAlreadyExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		CatalogFunction func = createFunction();
		catalog.createFunction(path1, func, false);

		CatalogTestUtil.checkEquals(func, catalog.getFunction(path1));

		catalog.createFunction(path1, createAnotherFunction(), true);

		CatalogTestUtil.checkEquals(func, catalog.getFunction(path1));

		catalog.dropFunction(path1, false);
		catalog.dropDatabase(db1, false);
	}

	@Test
	public void testAlterFunction() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		CatalogFunction func = createFunction();
		catalog.createFunction(path1, func, false);

		CatalogTestUtil.checkEquals(func, catalog.getFunction(path1));

		CatalogFunction newFunc = createAnotherFunction();
		catalog.alterFunction(path1, newFunc, false);

		assertNotEquals(func, catalog.getFunction(path1));
		CatalogTestUtil.checkEquals(newFunc, catalog.getFunction(path1));

		catalog.dropFunction(path1, false);
		catalog.dropDatabase(db1, false);
	}

	@Test
	public void testAlterFunction_FunctionNotExistException() throws Exception {
		exception.expect(FunctionNotExistException.class);
		exception.expectMessage("Function db1.nonexist does not exist in Catalog");
		catalog.alterFunction(nonExistObjectPath, createFunction(), false);
	}

	@Test
	public void testAlterFunction_FunctionNotExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterFunction(nonExistObjectPath, createFunction(), true);

		assertFalse(catalog.functionExists(nonExistObjectPath));

		catalog.dropDatabase(db1, false);
	}

	@Test
	public void testListFunctions() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		CatalogFunction func = createFunction();
		catalog.createFunction(path1, func, false);

		assertEquals(path1.getObjectName(), catalog.listFunctions(db1).get(0));

		catalog.dropFunction(path1, false);
		catalog.dropDatabase(db1, false);
	}

	@Test
	public void testListFunctions_DatabaseNotExistException() throws Exception{
		exception.expect(DatabaseNotExistException.class);
		exception.expectMessage("Database db1 does not exist in Catalog");
		catalog.listFunctions(db1);
	}

	@Test
	public void testGetFunction_FunctionNotExistException() throws Exception {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(FunctionNotExistException.class);
		exception.expectMessage("Function db1.nonexist does not exist in Catalog");
		catalog.getFunction(nonExistObjectPath);
	}

	@Test
	public void testGetFunction_FunctionNotExistException_NoDb() throws Exception {
		exception.expect(FunctionNotExistException.class);
		exception.expectMessage("Function db1.nonexist does not exist in Catalog");
		catalog.getFunction(nonExistObjectPath);
	}

	@Test
	public void testDropFunction() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createFunction(path1, createFunction(), false);

		assertTrue(catalog.functionExists(path1));

		catalog.dropFunction(path1, false);

		assertFalse(catalog.functionExists(path1));

		catalog.dropDatabase(db1, false);
	}

	@Test
	public void testDropFunction_FunctionNotExistException() throws Exception {
		exception.expect(FunctionNotExistException.class);
		exception.expectMessage("Function non.exist does not exist in Catalog");
		catalog.dropFunction(nonExistDbPath, false);
	}

	@Test
	public void testDropFunction_FunctionNotExist_ignored() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.dropFunction(nonExistObjectPath, true);
		catalog.dropDatabase(db1, false);
	}

	// ------ utilities ------

	@Override
	public String getBuiltInDefaultDatabase() {
		return GenericInMemoryCatalog.DEFAULT_DB;
	}

	@Override
	public CatalogDatabase createDb() {
		return new GenericCatalogDatabase(
			new HashMap<String, String>() {{
				put("k1", "v1");
			}},
			TEST_COMMENT);
	}

	@Override
	public CatalogDatabase createAnotherDb() {
		return new GenericCatalogDatabase(
			new HashMap<String, String>() {{
				put("k2", "v2");
			}},
			"this is another database.");
	}

	@Override
	public GenericCatalogTable createStreamingTable() {
		return new GenericCatalogTable(
			createTableSchema(),
			new TableStats(0),
			getStreamingTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createTable() {
		return new GenericCatalogTable(
			createTableSchema(),
			new TableStats(0),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createAnotherTable() {
		return new GenericCatalogTable(
			createAnotherTableSchema(),
			new TableStats(0),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createPartitionedTable() {
		return new GenericCatalogTable(
			createTableSchema(),
			new TableStats(0),
			createPartitionKeys(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createAnotherPartitionedTable() {
		return new GenericCatalogTable(
			createAnotherTableSchema(),
			new TableStats(0),
			createPartitionKeys(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	private CatalogPartitionSpec createPartitionSpec() {
		return new CatalogPartitionSpec(
			new HashMap<String, String>() {{
				put("third", "2000");
				put("second", "bob");
			}});
	}

	private CatalogPartitionSpec createAnotherPartitionSpec() {
		return new CatalogPartitionSpec(
			new HashMap<String, String>() {{
				put("third", "2010");
				put("second", "bob");
			}});
	}

	private CatalogPartitionSpec createPartitionSpecSubset() {
		return new CatalogPartitionSpec(
			new HashMap<String, String>() {{
				put("second", "bob");
			}});
	}

	private CatalogPartitionSpec createInvalidPartitionSpecSubset() {
		return new CatalogPartitionSpec(
			new HashMap<String, String>() {{
				put("third", "2010");
			}});
	}

	private CatalogPartition createPartition() {
		return new GenericCatalogPartition(getBatchTableProperties());
	}

	private CatalogPartition createAnotherPartition() {
		return new GenericCatalogPartition(getBatchTableProperties());
	}

	private CatalogPartition createPartition(Map<String, String> props) {
		return new GenericCatalogPartition(props);
	}

	private CatalogView createView() {
		return new GenericCatalogView(
			String.format("select * from %s", t1),
			String.format("select * from %s.%s", TEST_CATALOG_NAME, path1.getFullName()),
			createTableSchema(),
			new HashMap<>(),
			"This is a view");
	}

	private CatalogView createAnotherView() {
		return new GenericCatalogView(
			String.format("select * from %s", t2),
			String.format("select * from %s.%s", TEST_CATALOG_NAME, path2.getFullName()),
			createTableSchema(),
			new HashMap<>(),
			"This is another view");
	}

	protected CatalogFunction createFunction() {
		return new GenericCatalogFunction(MyScalarFunction.class.getName());
	}

	protected CatalogFunction createAnotherFunction() {
		return new GenericCatalogFunction(MyOtherScalarFunction.class.getName());
	}

	/**
	 * Test UDF.
	 */
	public static class MyScalarFunction extends ScalarFunction {
		public Integer eval(Integer i) {
			return i + 1;
		}
	}

	/**
	 * Test UDF.
	 */
	public static class MyOtherScalarFunction extends ScalarFunction {
		public String eval(Integer i) {
			return String.valueOf(i);
		}
	}
}
