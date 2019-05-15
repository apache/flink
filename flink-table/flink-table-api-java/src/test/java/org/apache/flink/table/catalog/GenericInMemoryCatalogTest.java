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
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBinary;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.Date;
import org.apache.flink.table.functions.ScalarFunction;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
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
	public void testRenameTable_partitionedTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createPartitionedTable();
		catalog.createTable(path1, table, false);
		CatalogPartition catalogPartition = createPartition();
		CatalogPartitionSpec catalogPartitionSpec = createPartitionSpec();
		catalog.createPartition(path1, catalogPartitionSpec, catalogPartition, false);

		checkEquals(table, (CatalogTable) catalog.getTable(path1));
		assertTrue(catalog.partitionExists(path1, catalogPartitionSpec));

		catalog.renameTable(path1, t2, false);

		checkEquals(table, (CatalogTable) catalog.getTable(path3));
		assertTrue(catalog.partitionExists(path3, catalogPartitionSpec));
		assertFalse(catalog.tableExists(path1));
		assertFalse(catalog.partitionExists(path1, catalogPartitionSpec));
	}

	@Test
	public void testAlterTable_alterTableWithView() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		exception.expect(CatalogException.class);
		exception.expectMessage("Existing table is 'org.apache.flink.table.catalog.GenericCatalogTable' " +
			"and new table is 'org.apache.flink.table.catalog.GenericCatalogView'. They should be of the same class.");
		catalog.alterTable(path1, createView(), false);
	}

	@Test
	public void testAlterTable_alterViewWithTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createView(), false);

		exception.expect(CatalogException.class);
		exception.expectMessage("Existing table is 'org.apache.flink.table.catalog.GenericCatalogView' " +
			"and new table is 'org.apache.flink.table.catalog.GenericCatalogTable'. They should be of the same class.");
		catalog.alterTable(path1, createTable(), false);
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

	// ------ statistics ------

	@Test
	public void testStatistics() throws Exception {
		// Table related
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(catalog.getTableStatistics(path1), CatalogTableStatistics.UNKNOWN);
		CatalogTestUtil.checkEquals(catalog.getTableColumnStatistics(path1), CatalogColumnStatistics.UNKNOWN);

		CatalogTableStatistics tableStatistics = new CatalogTableStatistics(5, 2, 100, 575);
		catalog.alterTableStatistics(path1, tableStatistics, false);
		CatalogTestUtil.checkEquals(tableStatistics, catalog.getTableStatistics(path1));
		CatalogColumnStatistics columnStatistics = createColumnStats();
		catalog.alterTableColumnStatistics(path1, columnStatistics, false);
		CatalogTestUtil.checkEquals(columnStatistics, catalog.getTableColumnStatistics(path1));

		// Partition related
		catalog.createDatabase(db2, createDb(), false);
		CatalogTable table2 = createPartitionedTable();
		catalog.createTable(path2, table2, false);
		CatalogPartitionSpec partitionSpec = createPartitionSpec();
		catalog.createPartition(path2, partitionSpec, createPartition(), false);

		CatalogTestUtil.checkEquals(catalog.getPartitionStatistics(path2, partitionSpec), CatalogTableStatistics.UNKNOWN);
		CatalogTestUtil.checkEquals(catalog.getPartitionColumnStatistics(path2, partitionSpec), CatalogColumnStatistics.UNKNOWN);

		catalog.alterPartitionStatistics(path2, partitionSpec, tableStatistics, false);
		CatalogTestUtil.checkEquals(tableStatistics, catalog.getPartitionStatistics(path2, partitionSpec));
		catalog.alterPartitionColumnStatistics(path2, partitionSpec, columnStatistics, false);
		CatalogTestUtil.checkEquals(columnStatistics, catalog.getPartitionColumnStatistics(path2, partitionSpec));

		// Clean up
		catalog.dropTable(path1, false);
		catalog.dropDatabase(db1, false);
		catalog.dropTable(path2, false);
		catalog.dropDatabase(db2, false);
	}

	// ------ utilities ------

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
			getStreamingTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createTable() {
		return new GenericCatalogTable(
			createTableSchema(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createAnotherTable() {
		return new GenericCatalogTable(
			createAnotherTableSchema(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createPartitionedTable() {
		return new GenericCatalogTable(
			createTableSchema(),
			createPartitionKeys(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createAnotherPartitionedTable() {
		return new GenericCatalogTable(
			createAnotherTableSchema(),
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

	@Override
	public CatalogView createView() {
		return new GenericCatalogView(
			String.format("select * from %s", t1),
			String.format("select * from %s.%s", TEST_CATALOG_NAME, path1.getFullName()),
			createTableSchema(),
			new HashMap<>(),
			"This is a view");
	}

	@Override
	public CatalogView createAnotherView() {
		return new GenericCatalogView(
			String.format("select * from %s", t2),
			String.format("select * from %s.%s", TEST_CATALOG_NAME, path2.getFullName()),
			createAnotherTableSchema(),
			new HashMap<>(),
			"This is another view");
	}

	private CatalogColumnStatistics createColumnStats() {
		CatalogColumnStatisticsDataBoolean booleanColStats = new CatalogColumnStatisticsDataBoolean(55L, 45L, 5L);
		CatalogColumnStatisticsDataLong longColStats = new CatalogColumnStatisticsDataLong(-123L, 763322L, 23L, 79L);
		CatalogColumnStatisticsDataString stringColStats = new CatalogColumnStatisticsDataString(152L, 43.5D, 20L, 0L);
		CatalogColumnStatisticsDataDate dateColStats = new CatalogColumnStatisticsDataDate(new Date(71L),
			new Date(17923L), 1321, 0L);
		CatalogColumnStatisticsDataDouble doubleColStats = new CatalogColumnStatisticsDataDouble(-123.35D, 7633.22D, 23L, 79L);
		CatalogColumnStatisticsDataBinary binaryColStats = new CatalogColumnStatisticsDataBinary(755L, 43.5D, 20L);
		Map<String, CatalogColumnStatisticsDataBase> colStatsMap = new HashMap<>(6);
		colStatsMap.put("b1", booleanColStats);
		colStatsMap.put("l2", longColStats);
		colStatsMap.put("s3", stringColStats);
		colStatsMap.put("d4", dateColStats);
		colStatsMap.put("dd5", doubleColStats);
		colStatsMap.put("bb6", binaryColStats);
		return new CatalogColumnStatistics(colStatsMap);
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
