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

package org.apache.flink.table.functions;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.utils.TestCollectionTableFactory;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for both catalog and system function.
 */
public abstract class FunctionTestBase {
	private static TableEnvironment tableEnv;

	public static void setTableEnv(TableEnvironment e) {
		tableEnv = e;
	}

	public abstract void execute() throws Exception;

	@Test
	public void testCreateCatalogFunctionInDefaultCatalog() {
		String ddl1 = "create function f1 as 'org.apache.flink.function.TestFunction'";
		tableEnv.sqlUpdate(ddl1);
		assertTrue(Arrays.asList(tableEnv.listFunctions()).contains("f1"));

		tableEnv.sqlUpdate("DROP FUNCTION IF EXISTS default_catalog.default_database.f1");
		assertFalse(Arrays.asList(tableEnv.listFunctions()).contains("f1"));
	}

	@Test
	public void testCreateFunctionWithFullPath() {
		String ddl1 = "create function default_catalog.default_database.f2 as" +
			" 'org.apache.flink.function.TestFunction'";
		tableEnv.sqlUpdate(ddl1);
		assertTrue(Arrays.asList(tableEnv.listFunctions()).contains("f2"));

		tableEnv.sqlUpdate("DROP FUNCTION IF EXISTS default_catalog.default_database.f2");
		assertFalse(Arrays.asList(tableEnv.listFunctions()).contains("f2"));
	}

	@Test
	public void testCreateFunctionWithoutCatalogIdentifier() {
		String ddl1 = "create function default_database.f3 as" +
			" 'org.apache.flink.function.TestFunction'";
		tableEnv.sqlUpdate(ddl1);
		assertTrue(Arrays.asList(tableEnv.listFunctions()).contains("f3"));

		tableEnv.sqlUpdate("DROP FUNCTION IF EXISTS default_catalog.default_database.f3");
		assertFalse(Arrays.asList(tableEnv.listFunctions()).contains("f3"));
	}

	@Test
	public void testCreateFunctionCatalogNotExists() {

		String ddl1 = "create function catalog1.database1.f3 as 'org.apache.flink.function.TestFunction'";

		try {
			tableEnv.sqlUpdate(ddl1);
		} catch (Exception e){
			assertTrue(e.getMessage().equals("Catalog catalog1 does not exist"));
		}
	}

	@Test
	public void testCreateFunctionDBNotExists() {
		String ddl1 = "create function default_catalog.database1.f3 as 'org.apache.flink.function.TestFunction'";

		try {
			tableEnv.sqlUpdate(ddl1);
		} catch (Exception e){
			assertEquals(e.getMessage(), "Could not execute CREATE CATALOG FUNCTION:" +
				" (catalogFunction: [Optional[This is a user-defined function]], identifier:" +
				" [`default_catalog`.`database1`.`f3`], ignoreIfExists: [false])");
		}
	}

	@Test
	public void testCreateTemporaryCatalogFunction() {
		String ddl1 = "create temporary function default_catalog.default_database.f4" +
			" as 'org.apache.flink.table.functions.FunctionTestBase$TestUDF'";

		String ddl2 = "create temporary function if not exists default_catalog.default_database.f4" +
			" as 'org.apache.flink.table.functions.FunctionTestBase$TestUDF'";

		String ddl3 = "drop temporary function default_catalog.default_database.f4";

		String ddl4 = "drop temporary function if exists default_catalog.default_database.f4";

		tableEnv.sqlUpdate(ddl1);
		assertTrue(Arrays.asList(tableEnv.listFunctions()).contains("f4"));

		tableEnv.sqlUpdate(ddl2);
		assertTrue(Arrays.asList(tableEnv.listFunctions()).contains("f4"));

		tableEnv.sqlUpdate(ddl3);
		assertFalse(Arrays.asList(tableEnv.listFunctions()).contains("f4"));

		tableEnv.sqlUpdate(ddl1);
		try {
			tableEnv.sqlUpdate(ddl1);
		} catch (Exception e) {
			assertTrue(e instanceof ValidationException);
			assertEquals(e.getMessage(),
				"Temporary catalog function `default_catalog`.`default_database`.`f4`" +
					" is already defined");
		}

		tableEnv.sqlUpdate(ddl3);
		tableEnv.sqlUpdate(ddl4);
		try {
			tableEnv.sqlUpdate(ddl3);
		} catch (Exception e) {
			assertTrue(e instanceof ValidationException);
			assertEquals(e.getMessage(),
				"Temporary catalog function `default_catalog`.`default_database`.`f4`" +
					" doesn't exist");
		}
	}

	@Test
	public void testCreateTemporarySystemFunction() {
		String ddl1 = "create temporary system function default_catalog.default_database.f5" +
			" as 'org.apache.flink.table.functions.FunctionTestBase$TestUDF'";

		String ddl2 = "create temporary system function if not exists default_catalog.default_database.f5" +
			" as 'org.apache.flink.table.functions.CatalogFunctionTestBase$TestUDF'";

		String ddl3 = "drop temporary system function default_catalog.default_database.f5";

		tableEnv.sqlUpdate(ddl1);
		tableEnv.sqlUpdate(ddl2);
		tableEnv.sqlUpdate(ddl3);
	}

	@Test
	public void testAlterFunction() throws Exception {
		String create = "create function f3 as 'org.apache.flink.function.TestFunction'";
		String alter = "alter function f3 as 'org.apache.flink.function.TestFunction2'";

		ObjectPath objectPath = new ObjectPath("default_database", "f3");
		Catalog catalog = tableEnv.getCatalog("default_catalog").get();
		tableEnv.sqlUpdate(create);
		CatalogFunction beforeUpdate = catalog.getFunction(objectPath);
		assertEquals("org.apache.flink.function.TestFunction", beforeUpdate.getClassName());

		tableEnv.sqlUpdate(alter);
		CatalogFunction afterUpdate = catalog.getFunction(objectPath);
		assertEquals("org.apache.flink.function.TestFunction2", afterUpdate.getClassName());
	}

	@Test
	public void testAlterFunctionNonExists() {
		String alterUndefinedFunction = "ALTER FUNCTION default_catalog.default_database.f4" +
			" as 'org.apache.flink.function.TestFunction'";

		String alterFunctionInWrongCatalog = "ALTER FUNCTION catalog1.default_database.f4 " +
			"as 'org.apache.flink.function.TestFunction'";

		String alterFunctionInWrongDB = "ALTER FUNCTION default_catalog.db1.f4 " +
			"as 'org.apache.flink.function.TestFunction'";

		try {
			tableEnv.sqlUpdate(alterUndefinedFunction);
			fail();
		} catch (Exception e){
			assertEquals(e.getMessage(),
				"Function default_database.f4 does not exist in Catalog default_catalog.");
		}

		try {
			tableEnv.sqlUpdate(alterFunctionInWrongCatalog);
			fail();
		} catch (Exception e) {
			assertTrue(e.getMessage().equals("Catalog catalog1 does not exist"));
		}

		try {
			tableEnv.sqlUpdate(alterFunctionInWrongDB);
			fail();
		} catch (Exception e) {
			assertEquals(e.getMessage(), "Function db1.f4 does not exist" +
				" in Catalog default_catalog.");
		}
	}

	@Test
	public void testAlterTemporaryCatalogFunction() {
		String alterTemporary = "ALTER TEMPORARY FUNCTION default_catalog.default_database.f4" +
			" as 'org.apache.flink.function.TestFunction'";

		try {
			tableEnv.sqlUpdate(alterTemporary);
			fail();
		} catch (Exception e) {
			assertTrue(e.getMessage().equals("Alter temporary catalog function is not supported"));
		}
	}

	@Test
	public void testAlterTemporarySystemFunction() {
		String alterTemporary = "ALTER TEMPORARY SYSTEM FUNCTION default_catalog.default_database.f4" +
			" as 'org.apache.flink.function.TestFunction'";

		try {
			tableEnv.sqlUpdate(alterTemporary);
			fail();
		} catch (Exception e) {
			assertTrue(e.getMessage().equals("Alter temporary system function is not supported"));
		}
	}

	@Test
	public void testDropFunctionNonExists() {
		String dropUndefinedFunction = "DROP FUNCTION default_catalog.default_database.f4";

		String dropFunctionInWrongCatalog = "DROP FUNCTION catalog1.default_database.f4";

		String dropFunctionInWrongDB = "DROP FUNCTION default_catalog.db1.f4";

		try {
			tableEnv.sqlUpdate(dropUndefinedFunction);
			fail();
		} catch (Exception e){
			assertEquals(e.getMessage(),
				"Function default_database.f4 does not exist in Catalog default_catalog.");
		}

		try {
			tableEnv.sqlUpdate(dropFunctionInWrongCatalog);
			fail();
		} catch (Exception e) {
			assertTrue(e.getMessage().equals("Catalog catalog1 does not exist"));
		}

		try {
			tableEnv.sqlUpdate(dropFunctionInWrongDB);
			fail();
		} catch (Exception e) {
			assertEquals(e.getMessage(),
				"Function db1.f4 does not exist in Catalog default_catalog.");
		}
	}

	@Test
	public void testDropTemporaryFunctionNonExits() {
		String dropUndefinedFunction = "DROP TEMPORARY FUNCTION default_catalog.default_database.f4";
		String dropFunctionInWrongCatalog = "DROP TEMPORARY FUNCTION catalog1.default_database.f4";
		String dropFunctionInWrongDB = "DROP TEMPORARY FUNCTION default_catalog.db1.f4";

		try {
			tableEnv.sqlUpdate(dropUndefinedFunction);
			fail();
		} catch (Exception e){
			assertEquals(e.getMessage(), "Temporary catalog function" +
				" `default_catalog`.`default_database`.`f4` doesn't exist");
		}

		try {
			tableEnv.sqlUpdate(dropFunctionInWrongCatalog);
			fail();
		} catch (Exception e) {
			assertEquals(e.getMessage(), "Temporary catalog function " +
				"`catalog1`.`default_database`.`f4` doesn't exist");
		}

		try {
			tableEnv.sqlUpdate(dropFunctionInWrongDB);
			fail();
		} catch (Exception e) {
			assertEquals(e.getMessage(), "Temporary catalog function " +
				"`default_catalog`.`db1`.`f4` doesn't exist");
		}
	}

	@Test
	public void testCreateDropTemporaryCatalogFunctionsWithDifferentIdentifier() {
		String createNoCatalogDB = "create temporary function f4" +
			" as 'org.apache.flink.table.functions.FunctionTestBase$TestUDF'";

		String dropNoCatalogDB = "drop temporary function f4";

		tableEnv.sqlUpdate(createNoCatalogDB);
		tableEnv.sqlUpdate(dropNoCatalogDB);

		String createNonExistsCatalog = "create temporary function catalog1.default_database.f4" +
			" as 'org.apache.flink.table.functions.FunctionTestBase$TestUDF'";

		String dropNonExistsCatalog = "drop temporary function catalog1.default_database.f4";

		tableEnv.sqlUpdate(createNonExistsCatalog);
		tableEnv.sqlUpdate(dropNonExistsCatalog);

		String createNonExistsDB = "create temporary function default_catalog.db1.f4" +
			" as 'org.apache.flink.table.functions.FunctionTestBase$TestUDF'";

		String dropNonExistsDB = "drop temporary function default_catalog.db1.f4";

		tableEnv.sqlUpdate(createNonExistsDB);
		tableEnv.sqlUpdate(dropNonExistsDB);
	}

	@Test
	public void testDropTemporarySystemFunction() {
		String ddl1 = "create temporary system function f5" +
			" as 'org.apache.flink.table.functions.FunctionTestBase$TestUDF'";

		String ddl2 = "drop temporary system function f5";

		String ddl3 = "drop temporary system function if exists f5";

		tableEnv.sqlUpdate(ddl1);
		tableEnv.sqlUpdate(ddl2);
		tableEnv.sqlUpdate(ddl3);

		try {
			tableEnv.sqlUpdate(ddl2);
		} catch (Exception e) {
			assertEquals(e.getMessage(), "Temporary system function f5 doesn't exist");
		}
	}

	@Test
	public void testUseDefinedRegularCatalogFunction() throws Exception {
		String functionDDL = "create function addOne as " +
			"'org.apache.flink.table.functions.FunctionTestBase$TestUDF'";

		String dropFunctionDDL = "drop function addOne";
		testUseDefinedCatalogFunction(functionDDL);
		// delete the function
		tableEnv.sqlUpdate(dropFunctionDDL);
	}

	@Test
	public void testUseDefinedTemporaryCatalogFunction() throws Exception {
		String functionDDL = "create temporary function addOne as " +
			"'org.apache.flink.table.functions.FunctionTestBase$TestUDF'";

		String dropFunctionDDL = "drop temporary function addOne";
		testUseDefinedCatalogFunction(functionDDL);
		// delete the function
		tableEnv.sqlUpdate(dropFunctionDDL);
	}

	@Test
	public void testUseDefinedTemporarySystemFunction() throws Exception {
		String functionDDL = "create temporary system function addOne as " +
			"'org.apache.flink.table.functions.FunctionTestBase$TestUDF'";

		String dropFunctionDDL = "drop temporary system function addOne";
		testUseDefinedCatalogFunction(functionDDL);
		// delete the function
		tableEnv.sqlUpdate(dropFunctionDDL);
	}

	private void testUseDefinedCatalogFunction(String createFunctionDDL) throws Exception {
		List<Row> sourceData = Arrays.asList(
			toRow(1, "1000", 2),
			toRow(2, "1", 3),
			toRow(3, "2000", 4),
			toRow(1, "2", 2),
			toRow(2, "3000", 3)
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData, new ArrayList<Row>(), -1);

		String sourceDDL = "create table t1(a int, b varchar, c int) with ('connector' = 'COLLECTION')";
		String sinkDDL = "create table t2(a int, b varchar, c int) with ('connector' = 'COLLECTION')";
		String query = " insert into t2 select t1.a, t1.b, addOne(t1.a, 1) as c from t1";

		tableEnv.sqlUpdate(sourceDDL);
		tableEnv.sqlUpdate(sinkDDL);
		tableEnv.sqlUpdate(createFunctionDDL);
		tableEnv.sqlUpdate(query);
		execute();

		Row[] result = TestCollectionTableFactory.RESULT().toArray(new Row[0]);
		Row[] expected = sourceData.toArray(new Row[0]);
		assertArrayEquals(expected, result);

		tableEnv.sqlUpdate("drop table t1");
		tableEnv.sqlUpdate("drop table t2");
	}

	private Row toRow(Object ... objects) {
		Row row = new Row(objects.length);
		for (int i = 0; i < objects.length; i++) {
			row.setField(i, objects[i]);
		}

		return row;
	}

	/**
	 * Test udf class.
	 */
	public static class TestUDF extends ScalarFunction {

		public Integer eval(Integer a, Integer b) {
			return a + b;
		}
	}
}
