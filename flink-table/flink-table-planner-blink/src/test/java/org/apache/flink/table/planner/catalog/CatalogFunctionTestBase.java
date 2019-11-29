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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link CatalogFunction}.
 */
public abstract class CatalogFunctionTestBase {
	protected static TableEnvironment tableEnv;

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
			assertEquals(e.getMessage(), "Could not execute CREATE FUNCTION:" +
				" (catalogFunction: [Optional[This is a user-defined function]], identifier:" +
				" [`default_catalog`.`database1`.`f3`], ignoreIfExists: [false])");
		}
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
			assertEquals(e.getMessage(), "Could not execute ALTER FUNCTION: " +
				"(catalogFunction: [Optional[This is a user-defined function]], " +
				"identifier: [`default_catalog`.`default_database`.`f4`], ifExists: [false])");
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
			assertEquals(e.getMessage(), "Could not execute ALTER FUNCTION: " +
				"(catalogFunction: [Optional[This is a user-defined function]], " +
				"identifier: [`default_catalog`.`db1`.`f4`], ifExists: [false])");
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
			assertEquals(e.getMessage(), "Could not execute DROP FUNCTION:" +
				" (identifier: [`default_catalog`.`default_database`.`f4`], IfExists: [false])");
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
			assertEquals(e.getMessage(), "Could not execute DROP FUNCTION:" +
				" (identifier: [`default_catalog`.`db1`.`f4`], IfExists: [false])");
		}
	}

	protected Row toRow(Object ... objects) {
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

