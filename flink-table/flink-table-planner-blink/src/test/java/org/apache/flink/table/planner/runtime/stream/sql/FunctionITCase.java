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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
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
 * Tests for catalog and system in stream table environment.
 */
public class FunctionITCase extends StreamingTestBase {

	private static final String TEST_FUNCTION = TestUDF.class.getName();

	@Test
	public void testCreateCatalogFunctionInDefaultCatalog() {
		String ddl1 = "create function f1 as 'org.apache.flink.function.TestFunction'";
		tEnv().sqlUpdate(ddl1);
		assertTrue(Arrays.asList(tEnv().listFunctions()).contains("f1"));

		tEnv().sqlUpdate("DROP FUNCTION IF EXISTS default_catalog.default_database.f1");
		assertFalse(Arrays.asList(tEnv().listFunctions()).contains("f1"));
	}

	@Test
	public void testCreateFunctionWithFullPath() {
		String ddl1 = "create function default_catalog.default_database.f2 as" +
			" 'org.apache.flink.function.TestFunction'";
		tEnv().sqlUpdate(ddl1);
		assertTrue(Arrays.asList(tEnv().listFunctions()).contains("f2"));

		tEnv().sqlUpdate("DROP FUNCTION IF EXISTS default_catalog.default_database.f2");
		assertFalse(Arrays.asList(tEnv().listFunctions()).contains("f2"));
	}

	@Test
	public void testCreateFunctionWithoutCatalogIdentifier() {
		String ddl1 = "create function default_database.f3 as" +
			" 'org.apache.flink.function.TestFunction'";
		tEnv().sqlUpdate(ddl1);
		assertTrue(Arrays.asList(tEnv().listFunctions()).contains("f3"));

		tEnv().sqlUpdate("DROP FUNCTION IF EXISTS default_catalog.default_database.f3");
		assertFalse(Arrays.asList(tEnv().listFunctions()).contains("f3"));
	}

	@Test
	public void testCreateFunctionCatalogNotExists() {
		String ddl1 = "create function catalog1.database1.f3 as 'org.apache.flink.function.TestFunction'";

		try {
			tEnv().sqlUpdate(ddl1);
		} catch (Exception e){
			assertEquals("Catalog catalog1 does not exist", e.getMessage());
		}
	}

	@Test
	public void testCreateFunctionDBNotExists() {
		String ddl1 = "create function default_catalog.database1.f3 as 'org.apache.flink.function.TestFunction'";

		try {
			tEnv().sqlUpdate(ddl1);
		} catch (Exception e){
			assertEquals(e.getMessage(), "Could not execute CREATE CATALOG FUNCTION:" +
				" (catalogFunction: [Optional[This is a user-defined function]], identifier:" +
				" [`default_catalog`.`database1`.`f3`], ignoreIfExists: [false])");
		}
	}

	@Test
	public void testCreateTemporaryCatalogFunction() {
		String ddl1 = "create temporary function default_catalog.default_database.f4" +
			" as '" + TEST_FUNCTION + "'";

		String ddl2 = "create temporary function if not exists default_catalog.default_database.f4" +
			" as '" + TEST_FUNCTION + "'";

		String ddl3 = "drop temporary function default_catalog.default_database.f4";

		String ddl4 = "drop temporary function if exists default_catalog.default_database.f4";

		tEnv().sqlUpdate(ddl1);
		assertTrue(Arrays.asList(tEnv().listFunctions()).contains("f4"));

		tEnv().sqlUpdate(ddl2);
		assertTrue(Arrays.asList(tEnv().listFunctions()).contains("f4"));

		tEnv().sqlUpdate(ddl3);
		assertFalse(Arrays.asList(tEnv().listFunctions()).contains("f4"));

		tEnv().sqlUpdate(ddl1);
		try {
			tEnv().sqlUpdate(ddl1);
		} catch (Exception e) {
			assertTrue(e instanceof ValidationException);
			assertEquals(e.getMessage(),
				"Temporary catalog function `default_catalog`.`default_database`.`f4`" +
					" is already defined");
		}

		tEnv().sqlUpdate(ddl3);
		tEnv().sqlUpdate(ddl4);
		try {
			tEnv().sqlUpdate(ddl3);
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
			" as '" + TEST_FUNCTION + "'";

		String ddl2 = "create temporary system function if not exists default_catalog.default_database.f5" +
			" as '" + TEST_FUNCTION + "'";

		String ddl3 = "drop temporary system function default_catalog.default_database.f5";

		tEnv().sqlUpdate(ddl1);
		tEnv().sqlUpdate(ddl2);
		tEnv().sqlUpdate(ddl3);
	}

	@Test
	public void testAlterFunction() throws Exception {
		String create = "create function f3 as 'org.apache.flink.function.TestFunction'";
		String alter = "alter function f3 as 'org.apache.flink.function.TestFunction2'";

		ObjectPath objectPath = new ObjectPath("default_database", "f3");
		assertTrue(tEnv().getCatalog("default_catalog").isPresent());
		Catalog catalog = tEnv().getCatalog("default_catalog").get();
		tEnv().sqlUpdate(create);
		CatalogFunction beforeUpdate = catalog.getFunction(objectPath);
		assertEquals("org.apache.flink.function.TestFunction", beforeUpdate.getClassName());

		tEnv().sqlUpdate(alter);
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
			tEnv().sqlUpdate(alterUndefinedFunction);
			fail();
		} catch (Exception e){
			assertEquals(e.getMessage(),
				"Function default_database.f4 does not exist in Catalog default_catalog.");
		}

		try {
			tEnv().sqlUpdate(alterFunctionInWrongCatalog);
			fail();
		} catch (Exception e) {
			assertEquals("Catalog catalog1 does not exist", e.getMessage());
		}

		try {
			tEnv().sqlUpdate(alterFunctionInWrongDB);
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
			tEnv().sqlUpdate(alterTemporary);
			fail();
		} catch (Exception e) {
			assertEquals("Alter temporary catalog function is not supported", e.getMessage());
		}
	}

	@Test
	public void testAlterTemporarySystemFunction() {
		String alterTemporary = "ALTER TEMPORARY SYSTEM FUNCTION default_catalog.default_database.f4" +
			" as 'org.apache.flink.function.TestFunction'";

		try {
			tEnv().sqlUpdate(alterTemporary);
			fail();
		} catch (Exception e) {
			assertEquals("Alter temporary system function is not supported", e.getMessage());
		}
	}

	@Test
	public void testDropFunctionNonExists() {
		String dropUndefinedFunction = "DROP FUNCTION default_catalog.default_database.f4";

		String dropFunctionInWrongCatalog = "DROP FUNCTION catalog1.default_database.f4";

		String dropFunctionInWrongDB = "DROP FUNCTION default_catalog.db1.f4";

		try {
			tEnv().sqlUpdate(dropUndefinedFunction);
			fail();
		} catch (Exception e){
			assertEquals(e.getMessage(),
				"Function default_database.f4 does not exist in Catalog default_catalog.");
		}

		try {
			tEnv().sqlUpdate(dropFunctionInWrongCatalog);
			fail();
		} catch (Exception e) {
			assertEquals("Catalog catalog1 does not exist", e.getMessage());
		}

		try {
			tEnv().sqlUpdate(dropFunctionInWrongDB);
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
			tEnv().sqlUpdate(dropUndefinedFunction);
			fail();
		} catch (Exception e){
			assertEquals(e.getMessage(), "Temporary catalog function" +
				" `default_catalog`.`default_database`.`f4` doesn't exist");
		}

		try {
			tEnv().sqlUpdate(dropFunctionInWrongCatalog);
			fail();
		} catch (Exception e) {
			assertEquals(e.getMessage(), "Temporary catalog function " +
				"`catalog1`.`default_database`.`f4` doesn't exist");
		}

		try {
			tEnv().sqlUpdate(dropFunctionInWrongDB);
			fail();
		} catch (Exception e) {
			assertEquals(e.getMessage(), "Temporary catalog function " +
				"`default_catalog`.`db1`.`f4` doesn't exist");
		}
	}

	@Test
	public void testCreateDropTemporaryCatalogFunctionsWithDifferentIdentifier() {
		String createNoCatalogDB = "create temporary function f4" +
			" as '" + TEST_FUNCTION + "'";

		String dropNoCatalogDB = "drop temporary function f4";

		tEnv().sqlUpdate(createNoCatalogDB);
		tEnv().sqlUpdate(dropNoCatalogDB);

		String createNonExistsCatalog = "create temporary function catalog1.default_database.f4" +
			" as '" + TEST_FUNCTION + "'";

		String dropNonExistsCatalog = "drop temporary function catalog1.default_database.f4";

		tEnv().sqlUpdate(createNonExistsCatalog);
		tEnv().sqlUpdate(dropNonExistsCatalog);

		String createNonExistsDB = "create temporary function default_catalog.db1.f4" +
			" as '" + TEST_FUNCTION + "'";

		String dropNonExistsDB = "drop temporary function default_catalog.db1.f4";

		tEnv().sqlUpdate(createNonExistsDB);
		tEnv().sqlUpdate(dropNonExistsDB);
	}

	@Test
	public void testDropTemporarySystemFunction() {
		String ddl1 = "create temporary system function f5 as '" + TEST_FUNCTION + "'";

		String ddl2 = "drop temporary system function f5";

		String ddl3 = "drop temporary system function if exists f5";

		tEnv().sqlUpdate(ddl1);
		tEnv().sqlUpdate(ddl2);
		tEnv().sqlUpdate(ddl3);

		try {
			tEnv().sqlUpdate(ddl2);
		} catch (Exception e) {
			assertEquals(e.getMessage(), "Temporary system function f5 doesn't exist");
		}
	}

	@Test
	public void testUserDefinedRegularCatalogFunction() throws Exception {
		String functionDDL = "create function addOne as '" + TEST_FUNCTION + "'";

		String dropFunctionDDL = "drop function addOne";
		testUserDefinedCatalogFunction(functionDDL);
		// delete the function
		tEnv().sqlUpdate(dropFunctionDDL);
	}

	@Test
	public void testUserDefinedTemporaryCatalogFunction() throws Exception {
		String functionDDL = "create temporary function addOne as '" + TEST_FUNCTION + "'";

		String dropFunctionDDL = "drop temporary function addOne";
		testUserDefinedCatalogFunction(functionDDL);
		// delete the function
		tEnv().sqlUpdate(dropFunctionDDL);
	}

	@Test
	public void testUserDefinedTemporarySystemFunction() throws Exception {
		String functionDDL = "create temporary system function addOne as '" + TEST_FUNCTION + "'";

		String dropFunctionDDL = "drop temporary system function addOne";
		testUserDefinedCatalogFunction(functionDDL);
		// delete the function
		tEnv().sqlUpdate(dropFunctionDDL);
	}

	/**
	 * Test udf class.
	 */
	public static class TestUDF extends ScalarFunction {

		public Integer eval(Integer a, Integer b) {
			return a + b;
		}
	}

	private void testUserDefinedCatalogFunction(String createFunctionDDL) throws Exception {
		List<Row> sourceData = Arrays.asList(
			Row.of(1, "1000", 2),
			Row.of(2, "1", 3),
			Row.of(3, "2000", 4),
			Row.of(1, "2", 2),
			Row.of(2, "3000", 3)
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData, new ArrayList<>(), -1);

		String sourceDDL = "create table t1(a int, b varchar, c int) with ('connector' = 'COLLECTION')";
		String sinkDDL = "create table t2(a int, b varchar, c int) with ('connector' = 'COLLECTION')";

		String query = "select t1.a, t1.b, addOne(t1.a, 1) as c from t1";

		tEnv().sqlUpdate(sourceDDL);
		tEnv().sqlUpdate(sinkDDL);
		tEnv().sqlUpdate(createFunctionDDL);
		Table t2 = tEnv().sqlQuery(query);
		tEnv().insertInto("t2", t2);
		tEnv().execute("job1");

		Row[] result = TestCollectionTableFactory.RESULT().toArray(new Row[0]);
		Row[] expected = sourceData.toArray(new Row[0]);
		assertArrayEquals(expected, result);

		tEnv().sqlUpdate("drop table t1");
		tEnv().sqlUpdate("drop table t2");
	}
}
