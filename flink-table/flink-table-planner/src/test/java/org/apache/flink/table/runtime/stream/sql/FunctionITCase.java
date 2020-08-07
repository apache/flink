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

package org.apache.flink.table.runtime.stream.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
public class FunctionITCase extends AbstractTestBase {

	private static final String TEST_FUNCTION = TestUDF.class.getName();

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testCreateCatalogFunctionInDefaultCatalog() {
		TableEnvironment tableEnv = getTableEnvironment();
		String ddl1 = "create function f1 as 'org.apache.flink.function.TestFunction'";
		tableEnv.sqlUpdate(ddl1);
		assertTrue(Arrays.asList(tableEnv.listFunctions()).contains("f1"));

		tableEnv.sqlUpdate("DROP FUNCTION IF EXISTS default_catalog.default_database.f1");
		assertFalse(Arrays.asList(tableEnv.listFunctions()).contains("f1"));
	}

	@Test
	public void testCreateFunctionWithFullPath() {
		TableEnvironment tableEnv = getTableEnvironment();
		String ddl1 = "create function default_catalog.default_database.f2 as" +
			" 'org.apache.flink.function.TestFunction'";
		tableEnv.sqlUpdate(ddl1);
		assertTrue(Arrays.asList(tableEnv.listFunctions()).contains("f2"));

		tableEnv.sqlUpdate("DROP FUNCTION IF EXISTS default_catalog.default_database.f2");
		assertFalse(Arrays.asList(tableEnv.listFunctions()).contains("f2"));
	}

	@Test
	public void testCreateFunctionWithoutCatalogIdentifier() {
		TableEnvironment tableEnv = getTableEnvironment();
		String ddl1 = "create function default_database.f3 as" +
			" 'org.apache.flink.function.TestFunction'";
		tableEnv.sqlUpdate(ddl1);
		assertTrue(Arrays.asList(tableEnv.listFunctions()).contains("f3"));

		tableEnv.sqlUpdate("DROP FUNCTION IF EXISTS default_catalog.default_database.f3");
		assertFalse(Arrays.asList(tableEnv.listFunctions()).contains("f3"));
	}

	@Test
	public void testCreateFunctionCatalogNotExists() {
		TableEnvironment tableEnv = getTableEnvironment();
		String ddl1 = "create function catalog1.database1.f3 as 'org.apache.flink.function.TestFunction'";

		try {
			tableEnv.sqlUpdate(ddl1);
		} catch (Exception e){
			assertEquals("Catalog catalog1 does not exist", e.getMessage());
		}
	}

	@Test
	public void testCreateFunctionDBNotExists() {
		TableEnvironment tableEnv = getTableEnvironment();
		String ddl1 = "create function default_catalog.database1.f3 as 'org.apache.flink.function.TestFunction'";

		try {
			tableEnv.sqlUpdate(ddl1);
		} catch (Exception e){
			assertEquals(e.getMessage(), "Could not execute CREATE CATALOG FUNCTION:" +
				" (catalogFunction: [Optional[This is a user-defined function]], identifier:" +
				" [`default_catalog`.`database1`.`f3`], ignoreIfExists: [false], isTemporary: [false])");
		}
	}

	@Test
	public void testCreateTemporaryCatalogFunction() {
		TableEnvironment tableEnv = getTableEnvironment();
		String ddl1 = "create temporary function default_catalog.default_database.f4" +
			" as '" + TEST_FUNCTION + "'";

		String ddl2 = "create temporary function if not exists default_catalog.default_database.f4" +
			" as '" + TEST_FUNCTION + "'";

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
			assertEquals("Could not register temporary catalog function. A function 'default_catalog.default_database.f4' does already exist.",
					e.getMessage());
		}

		tableEnv.sqlUpdate(ddl3);
		tableEnv.sqlUpdate(ddl4);
		try {
			tableEnv.sqlUpdate(ddl3);
		} catch (Exception e) {
			assertTrue(e instanceof ValidationException);
			assertEquals("Temporary catalog function `default_catalog`.`default_database`.`f4`" +
					" doesn't exist",
					e.getMessage());
		}
	}

	@Test
	public void testCreateTemporarySystemFunction() {
		TableEnvironment tableEnv = getTableEnvironment();
		String ddl1 = "create temporary system function f5" +
			" as '" + TEST_FUNCTION + "'";

		String ddl2 = "create temporary system function if not exists f5" +
			" as 'org.apache.flink.table.runtime.stream.sql.FunctionITCase$TestUDF'";

		String ddl3 = "drop temporary system function f5";

		tableEnv.sqlUpdate(ddl1);
		tableEnv.sqlUpdate(ddl2);
		tableEnv.sqlUpdate(ddl3);
	}

	@Test
	public void testAlterFunction() throws Exception {
		TableEnvironment tableEnv = getTableEnvironment();
		String create = "create function f3 as 'org.apache.flink.function.TestFunction'";
		String alter = "alter function f3 as 'org.apache.flink.function.TestFunction2'";

		ObjectPath objectPath = new ObjectPath("default_database", "f3");
		assertTrue(tableEnv.getCatalog("default_catalog").isPresent());
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
		TableEnvironment tableEnv = getTableEnvironment();
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
			assertEquals("Catalog catalog1 does not exist", e.getMessage());
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
		TableEnvironment tableEnv = getTableEnvironment();
		String alterTemporary = "ALTER TEMPORARY FUNCTION default_catalog.default_database.f4" +
			" as 'org.apache.flink.function.TestFunction'";

		try {
			tableEnv.sqlUpdate(alterTemporary);
			fail();
		} catch (Exception e) {
			assertEquals("Alter temporary catalog function is not supported", e.getMessage());
		}
	}

	@Test
	public void testAlterTemporarySystemFunction() {
		TableEnvironment tableEnv = getTableEnvironment();
		String alterTemporary = "ALTER TEMPORARY SYSTEM FUNCTION default_catalog.default_database.f4" +
			" as 'org.apache.flink.function.TestFunction'";

		try {
			tableEnv.sqlUpdate(alterTemporary);
			fail();
		} catch (Exception e) {
			assertEquals("Alter temporary system function is not supported", e.getMessage());
		}
	}

	@Test
	public void testDropFunctionNonExists() {
		TableEnvironment tableEnv = getTableEnvironment();
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
			assertEquals("Catalog catalog1 does not exist", e.getMessage());
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
		TableEnvironment tableEnv = getTableEnvironment();
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
		TableEnvironment tableEnv = getTableEnvironment();
		String createNoCatalogDB = "create temporary function f4" +
			" as '" + TEST_FUNCTION + "'";

		String dropNoCatalogDB = "drop temporary function f4";

		tableEnv.sqlUpdate(createNoCatalogDB);
		tableEnv.sqlUpdate(dropNoCatalogDB);

		String createNonExistsCatalog = "create temporary function catalog1.default_database.f4" +
			" as '" + TEST_FUNCTION + "'";

		String dropNonExistsCatalog = "drop temporary function catalog1.default_database.f4";

		tableEnv.sqlUpdate(createNonExistsCatalog);
		tableEnv.sqlUpdate(dropNonExistsCatalog);

		String createNonExistsDB = "create temporary function default_catalog.db1.f4" +
			" as '" + TEST_FUNCTION + "'";

		String dropNonExistsDB = "drop temporary function default_catalog.db1.f4";

		tableEnv.sqlUpdate(createNonExistsDB);
		tableEnv.sqlUpdate(dropNonExistsDB);
	}

	@Test
	public void testDropTemporarySystemFunction() {
		TableEnvironment tableEnv = getTableEnvironment();
		String ddl1 = "create temporary system function f5" +
			" as '" + TEST_FUNCTION + "'";

		String ddl2 = "drop temporary system function f5";

		String ddl3 = "drop temporary system function if exists f5";

		tableEnv.sqlUpdate(ddl1);
		tableEnv.sqlUpdate(ddl2);
		tableEnv.sqlUpdate(ddl3);

		try {
			tableEnv.sqlUpdate(ddl2);
		} catch (Exception e) {
			assertEquals(
				"Could not drop temporary system function. A function named 'f5' doesn't exist.",
				e.getMessage());
		}
	}

	@Test
	public void testUserDefinedRegularCatalogFunction() throws Exception {
		TableEnvironment tableEnv = getTableEnvironment();
		String functionDDL = "create function addOne as " +
			"'" + TEST_FUNCTION + "'";

		String dropFunctionDDL = "drop function addOne";
		testUserDefinedCatalogFunction(tableEnv, functionDDL);
		// delete the function
		tableEnv.sqlUpdate(dropFunctionDDL);
	}

	@Test
	public void testUserDefinedTemporaryCatalogFunction() throws Exception {
		TableEnvironment tableEnv = getTableEnvironment();
		String functionDDL = "create temporary function addOne as " +
			"'" + TEST_FUNCTION + "'";

		String dropFunctionDDL = "drop temporary function addOne";
		testUserDefinedCatalogFunction(tableEnv, functionDDL);
		// delete the function
		tableEnv.sqlUpdate(dropFunctionDDL);
	}

	@Test
	public void testUserDefinedTemporarySystemFunction() throws Exception {
		TableEnvironment tableEnv = getTableEnvironment();
		String functionDDL = "create temporary system function addOne as " +
			"'" + TEST_FUNCTION + "'";

		String dropFunctionDDL = "drop temporary system function addOne";
		testUserDefinedCatalogFunction(tableEnv, functionDDL);
		// delete the function
		tableEnv.sqlUpdate(dropFunctionDDL);
	}

	/**
	 * Test udf class.
	 */
	public static class TestUDF extends ScalarFunction {

		public Integer eval(Integer a, Integer b) {
			return a + b;
		}
	}

	private void testUserDefinedCatalogFunction(TableEnvironment tableEnv, String createFunctionDDL) throws Exception {
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
		String query = " insert into t2 select t1.a, t1.b, addOne(t1.a, 1) as c from t1";

		tableEnv.sqlUpdate(sourceDDL);
		tableEnv.sqlUpdate(sinkDDL);
		tableEnv.sqlUpdate(createFunctionDDL);
		tableEnv.sqlUpdate(query);
		tableEnv.execute("Test job");

		Row[] result = TestCollectionTableFactory.RESULT().toArray(new Row[0]);
		Row[] expected = sourceData.toArray(new Row[0]);
		assertArrayEquals(expected, result);

		tableEnv.sqlUpdate("drop table t1");
		tableEnv.sqlUpdate("drop table t2");
	}

	/**
	 * Simple scalar function.
	 */
	public static class SimpleScalarFunction extends ScalarFunction {
		public long eval(Integer i) {
			return i;
		}
	}

	private TableEnvironment getTableEnvironment() {
		StreamExecutionEnvironment streamExecEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().build();
		return StreamTableEnvironment.create(streamExecEnvironment, settings);
	}
}
