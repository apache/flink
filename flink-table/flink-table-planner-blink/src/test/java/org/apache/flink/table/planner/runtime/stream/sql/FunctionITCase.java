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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.codegen.CodeGenException;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.DayOfWeek;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

/**
 * Tests for catalog and system in stream table environment.
 */
public class FunctionITCase extends StreamingTestBase {

	private static final String TEST_FUNCTION = TestUDF.class.getName();

	@Test
	public void testCreateCatalogFunctionInDefaultCatalog() {
		String ddl1 = "create function f1 as 'org.apache.flink.function.TestFunction'";
		tEnv().executeSql(ddl1);
		assertTrue(Arrays.asList(tEnv().listFunctions()).contains("f1"));

		tEnv().executeSql("DROP FUNCTION IF EXISTS default_catalog.default_database.f1");
		assertFalse(Arrays.asList(tEnv().listFunctions()).contains("f1"));
	}

	@Test
	public void testCreateFunctionWithFullPath() {
		String ddl1 = "create function default_catalog.default_database.f2 as" +
			" 'org.apache.flink.function.TestFunction'";
		tEnv().executeSql(ddl1);
		assertTrue(Arrays.asList(tEnv().listFunctions()).contains("f2"));

		tEnv().executeSql("DROP FUNCTION IF EXISTS default_catalog.default_database.f2");
		assertFalse(Arrays.asList(tEnv().listFunctions()).contains("f2"));
	}

	@Test
	public void testCreateFunctionWithoutCatalogIdentifier() {
		String ddl1 = "create function default_database.f3 as" +
			" 'org.apache.flink.function.TestFunction'";
		tEnv().executeSql(ddl1);
		assertTrue(Arrays.asList(tEnv().listFunctions()).contains("f3"));

		tEnv().executeSql("DROP FUNCTION IF EXISTS default_catalog.default_database.f3");
		assertFalse(Arrays.asList(tEnv().listFunctions()).contains("f3"));
	}

	@Test
	public void testCreateFunctionCatalogNotExists() {
		String ddl1 = "create function catalog1.database1.f3 as 'org.apache.flink.function.TestFunction'";

		try {
			tEnv().executeSql(ddl1);
		} catch (Exception e){
			assertEquals("Catalog catalog1 does not exist", e.getMessage());
		}
	}

	@Test
	public void testCreateFunctionDBNotExists() {
		String ddl1 = "create function default_catalog.database1.f3 as 'org.apache.flink.function.TestFunction'";

		try {
			tEnv().executeSql(ddl1);
		} catch (Exception e){
			assertEquals(e.getMessage(), "Could not execute CREATE CATALOG FUNCTION:" +
				" (catalogFunction: [Optional[This is a user-defined function]], identifier:" +
				" [`default_catalog`.`database1`.`f3`], ignoreIfExists: [false], isTemporary: [false])");
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

		tEnv().executeSql(ddl1);
		assertTrue(Arrays.asList(tEnv().listFunctions()).contains("f4"));

		tEnv().executeSql(ddl2);
		assertTrue(Arrays.asList(tEnv().listFunctions()).contains("f4"));

		tEnv().executeSql(ddl3);
		assertFalse(Arrays.asList(tEnv().listFunctions()).contains("f4"));

		tEnv().executeSql(ddl1);
		try {
			tEnv().executeSql(ddl1);
		} catch (Exception e) {
			assertTrue(e instanceof ValidationException);
			assertEquals(e.getMessage(),
				"Temporary catalog function `default_catalog`.`default_database`.`f4`" +
					" is already defined");
		}

		tEnv().executeSql(ddl3);
		tEnv().executeSql(ddl4);
		try {
			tEnv().executeSql(ddl3);
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

		tEnv().executeSql(ddl1);
		tEnv().executeSql(ddl2);
		tEnv().executeSql(ddl3);
	}

	@Test
	public void testAlterFunction() throws Exception {
		String create = "create function f3 as 'org.apache.flink.function.TestFunction'";
		String alter = "alter function f3 as 'org.apache.flink.function.TestFunction2'";

		ObjectPath objectPath = new ObjectPath("default_database", "f3");
		assertTrue(tEnv().getCatalog("default_catalog").isPresent());
		Catalog catalog = tEnv().getCatalog("default_catalog").get();
		tEnv().executeSql(create);
		CatalogFunction beforeUpdate = catalog.getFunction(objectPath);
		assertEquals("org.apache.flink.function.TestFunction", beforeUpdate.getClassName());

		tEnv().executeSql(alter);
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
			tEnv().executeSql(alterUndefinedFunction);
			fail();
		} catch (Exception e){
			assertEquals(e.getMessage(),
				"Function default_database.f4 does not exist in Catalog default_catalog.");
		}

		try {
			tEnv().executeSql(alterFunctionInWrongCatalog);
			fail();
		} catch (Exception e) {
			assertEquals("Catalog catalog1 does not exist", e.getMessage());
		}

		try {
			tEnv().executeSql(alterFunctionInWrongDB);
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
			tEnv().executeSql(alterTemporary);
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
			tEnv().executeSql(alterTemporary);
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
			tEnv().executeSql(dropUndefinedFunction);
			fail();
		} catch (Exception e){
			assertEquals(e.getMessage(),
				"Function default_database.f4 does not exist in Catalog default_catalog.");
		}

		try {
			tEnv().executeSql(dropFunctionInWrongCatalog);
			fail();
		} catch (Exception e) {
			assertEquals("Catalog catalog1 does not exist", e.getMessage());
		}

		try {
			tEnv().executeSql(dropFunctionInWrongDB);
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
			tEnv().executeSql(dropUndefinedFunction);
			fail();
		} catch (Exception e){
			assertEquals(e.getMessage(), "Temporary catalog function" +
				" `default_catalog`.`default_database`.`f4` doesn't exist");
		}

		try {
			tEnv().executeSql(dropFunctionInWrongCatalog);
			fail();
		} catch (Exception e) {
			assertEquals(e.getMessage(), "Temporary catalog function " +
				"`catalog1`.`default_database`.`f4` doesn't exist");
		}

		try {
			tEnv().executeSql(dropFunctionInWrongDB);
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

		tEnv().executeSql(createNoCatalogDB);
		tEnv().executeSql(dropNoCatalogDB);

		String createNonExistsCatalog = "create temporary function catalog1.default_database.f4" +
			" as '" + TEST_FUNCTION + "'";

		String dropNonExistsCatalog = "drop temporary function catalog1.default_database.f4";

		tEnv().executeSql(createNonExistsCatalog);
		tEnv().executeSql(dropNonExistsCatalog);

		String createNonExistsDB = "create temporary function default_catalog.db1.f4" +
			" as '" + TEST_FUNCTION + "'";

		String dropNonExistsDB = "drop temporary function default_catalog.db1.f4";

		tEnv().executeSql(createNonExistsDB);
		tEnv().executeSql(dropNonExistsDB);
	}

	@Test
	public void testDropTemporarySystemFunction() {
		String ddl1 = "create temporary system function f5 as '" + TEST_FUNCTION + "'";

		String ddl2 = "drop temporary system function f5";

		String ddl3 = "drop temporary system function if exists f5";

		tEnv().executeSql(ddl1);
		tEnv().executeSql(ddl2);
		tEnv().executeSql(ddl3);

		try {
			tEnv().executeSql(ddl2);
		} catch (Exception e) {
			assertEquals(
				e.getMessage(),
				"Could not drop temporary system function. A function named 'f5' doesn't exist.");
		}
	}

	@Test
	public void testUserDefinedRegularCatalogFunction() throws Exception {
		String functionDDL = "create function addOne as '" + TEST_FUNCTION + "'";

		String dropFunctionDDL = "drop function addOne";
		testUserDefinedCatalogFunction(functionDDL);
		// delete the function
		tEnv().executeSql(dropFunctionDDL);
	}

	@Test
	public void testUserDefinedTemporaryCatalogFunction() throws Exception {
		String functionDDL = "create temporary function addOne as '" + TEST_FUNCTION + "'";

		String dropFunctionDDL = "drop temporary function addOne";
		testUserDefinedCatalogFunction(functionDDL);
		// delete the function
		tEnv().executeSql(dropFunctionDDL);
	}

	@Test
	public void testUserDefinedTemporarySystemFunction() throws Exception {
		String functionDDL = "create temporary system function addOne as '" + TEST_FUNCTION + "'";

		String dropFunctionDDL = "drop temporary system function addOne";
		testUserDefinedCatalogFunction(functionDDL);
		// delete the function
		tEnv().executeSql(dropFunctionDDL);
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
		TestCollectionTableFactory.initData(sourceData);

		String sourceDDL = "create table t1(a int, b varchar, c int) with ('connector' = 'COLLECTION')";
		String sinkDDL = "create table t2(a int, b varchar, c int) with ('connector' = 'COLLECTION')";

		String query = "select t1.a, t1.b, addOne(t1.a, 1) as c from t1";

		tEnv().executeSql(sourceDDL);
		tEnv().executeSql(sinkDDL);
		tEnv().executeSql(createFunctionDDL);
		Table t2 = tEnv().sqlQuery(query);
		TableResult tableResult = t2.executeInsert("t2");
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		Row[] result = TestCollectionTableFactory.RESULT().toArray(new Row[0]);
		Row[] expected = sourceData.toArray(new Row[0]);
		assertArrayEquals(expected, result);

		tEnv().executeSql("drop table t1");
		tEnv().executeSql("drop table t2");
	}

	@Test
	public void testPrimitiveScalarFunction() throws Exception {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, "-"),
			Row.of(2, 2L, "--"),
			Row.of(3, 3L, "---")
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, 3L, "-"),
			Row.of(2, 6L, "--"),
			Row.of(3, 9L, "---")
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");

		tEnv().createTemporarySystemFunction("PrimitiveScalarFunction", PrimitiveScalarFunction.class);
		execInsertSqlAndWaitResult("INSERT INTO TestTable SELECT i, PrimitiveScalarFunction(i, b, s), s FROM TestTable");

		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
	}

	@Test
	public void testNullScalarFunction() throws Exception {
		final List<Row> sinkData = Collections.singletonList(
			Row.of("Boolean", "String", "<<unknown>>", "String", "Object", "Boolean"));

		TestCollectionTableFactory.reset();

		tEnv().executeSql(
			"CREATE TABLE TestTable(s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING) " +
			"WITH ('connector' = 'COLLECTION')");

		tEnv().createTemporarySystemFunction("ClassNameScalarFunction", ClassNameScalarFunction.class);
		tEnv().createTemporarySystemFunction("ClassNameOrUnknownScalarFunction", ClassNameOrUnknownScalarFunction.class);
		tEnv().createTemporarySystemFunction("WildcardClassNameScalarFunction", WildcardClassNameScalarFunction.class);
		execInsertSqlAndWaitResult("INSERT INTO TestTable SELECT " +
			"ClassNameScalarFunction(NULL), " +
			"ClassNameScalarFunction(CAST(NULL AS STRING)), " +
			"ClassNameOrUnknownScalarFunction(NULL), " +
			"ClassNameOrUnknownScalarFunction(CAST(NULL AS STRING)), " +
			"WildcardClassNameScalarFunction(NULL), " +
			"WildcardClassNameScalarFunction(CAST(NULL AS BOOLEAN))");

		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
	}

	@Test
	public void testRowScalarFunction() throws Exception {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, Row.of(1, "1")),
			Row.of(2, Row.of(2, "2")),
			Row.of(3, Row.of(3, "3"))
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql(
			"CREATE TABLE TestTable(i INT, r ROW<i INT, s STRING>) " +
			"WITH ('connector' = 'COLLECTION')");

		tEnv().createTemporarySystemFunction("RowScalarFunction", RowScalarFunction.class);
		// the names of the function input and r differ
		execInsertSqlAndWaitResult("INSERT INTO TestTable SELECT i, RowScalarFunction(r) FROM TestTable");

		assertThat(TestCollectionTableFactory.getResult(), equalTo(sourceData));
	}

	@Test
	public void testComplexScalarFunction() throws Exception {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, new byte[]{1, 2, 3}),
			Row.of(2, new byte[]{2, 3, 4}),
			Row.of(3, new byte[]{3, 4, 5}),
			Row.of(null, null)
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(
				1,
				"1+2012-12-12 12:12:12.123456789",
				"[1, 2, 3]+2012-12-12 12:12:12.123456789",
				new BigDecimal("123.40"),
				ByteBuffer.wrap(new byte[]{1, 2, 3})),
			Row.of(
				2,
				"2+2012-12-12 12:12:12.123456789",
				"[2, 3, 4]+2012-12-12 12:12:12.123456789",
				new BigDecimal("123.40"),
				ByteBuffer.wrap(new byte[]{2, 3, 4})),
			Row.of(
				3,
				"3+2012-12-12 12:12:12.123456789",
				"[3, 4, 5]+2012-12-12 12:12:12.123456789",
				new BigDecimal("123.40"),
				ByteBuffer.wrap(new byte[]{3, 4, 5})),
			Row.of(
				null,
				"null+2012-12-12 12:12:12.123456789",
				"null+2012-12-12 12:12:12.123456789",
				new BigDecimal("123.40"),
				null)
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		final RawType<Object> rawType = new RawType<>(
			Object.class,
			new KryoSerializer<>(Object.class, new ExecutionConfig()));

		tEnv().executeSql(
			"CREATE TABLE SourceTable(i INT, b BYTES) " +
			"WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql(
			"CREATE TABLE SinkTable(" +
			"  i INT, " +
			"  s1 STRING, " +
			"  s2 STRING, " +
			"  d DECIMAL(5, 2)," +
			"  r " + rawType.asSerializableString() +
			") " +
			"WITH ('connector' = 'COLLECTION')");

		tEnv().createTemporarySystemFunction("ComplexScalarFunction", ComplexScalarFunction.class);
		execInsertSqlAndWaitResult(
			"INSERT INTO SinkTable " +
			"SELECT " +
			"  i, " +
			"  ComplexScalarFunction(i, TIMESTAMP '2012-12-12 12:12:12.123456789'), " +
			"  ComplexScalarFunction(b, TIMESTAMP '2012-12-12 12:12:12.123456789')," +
			"  ComplexScalarFunction(), " +
			"  ComplexScalarFunction(b) " +
			"FROM SourceTable");

		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
	}

	@Test
	public void testCustomScalarFunction() throws Exception {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1),
			Row.of(2),
			Row.of(3),
			Row.of((Integer) null)
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, 1, 5),
			Row.of(2, 2, 5),
			Row.of(3, 3, 5),
			Row.of(null, null, 5)
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE SourceTable(i INT) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE SinkTable(i1 INT, i2 INT, i3 INT) WITH ('connector' = 'COLLECTION')");

		tEnv().createTemporarySystemFunction("CustomScalarFunction", CustomScalarFunction.class);
		execInsertSqlAndWaitResult(
			"INSERT INTO SinkTable " +
			"SELECT " +
			"  i, " +
			"  CustomScalarFunction(i), " +
			"  CustomScalarFunction(CAST(NULL AS INT), 5, i, i) " +
			"FROM SourceTable");

		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
	}

	@Test
	public void testRawLiteralScalarFunction() throws Exception {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, DayOfWeek.MONDAY),
			Row.of(2, DayOfWeek.FRIDAY),
			Row.of(null, null)
		);

		final Row[] sinkData = new Row[]{
			Row.of(
				1,
				"MONDAY",
				DayOfWeek.MONDAY),
			Row.of(
				1,
				"MONDAY",
				DayOfWeek.MONDAY),
			Row.of(
				2,
				"FRIDAY",
				DayOfWeek.FRIDAY),
			Row.of(
				2,
				"FRIDAY",
				DayOfWeek.FRIDAY),
			Row.of(
				null,
				null,
				null),
			Row.of(
				null,
				null,
				null)
		};

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		final RawType<DayOfWeek> rawType = new RawType<>(
			DayOfWeek.class,
			new KryoSerializer<>(DayOfWeek.class, new ExecutionConfig()));

		tEnv().executeSql(
			"CREATE TABLE SourceTable(" +
			"  i INT, " +
			"  r " + rawType.asSerializableString() +
			") " +
			"WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql(
			"CREATE TABLE SinkTable(" +
			"  i INT, " +
			"  s STRING, " +
			"  r " + rawType.asSerializableString() +
			") " +
			"WITH ('connector' = 'COLLECTION')");

		tEnv().createTemporarySystemFunction("RawLiteralScalarFunction", RawLiteralScalarFunction.class);
		execInsertSqlAndWaitResult(
			"INSERT INTO SinkTable " +
			"  (SELECT " +
			"    i, " +
			"    RawLiteralScalarFunction(r, TRUE), " +
			"    RawLiteralScalarFunction(r, FALSE) " +
			"   FROM SourceTable)" +
			"UNION ALL " +
			"  (SELECT " +
			"    i, " +
			"    RawLiteralScalarFunction(r, TRUE), " +
			"    RawLiteralScalarFunction(r, FALSE) " +
			"  FROM SourceTable)");

		assertThat(TestCollectionTableFactory.getResult(), containsInAnyOrder(sinkData));
	}

	@Test
	public void testInvalidCustomScalarFunction() {
		tEnv().executeSql("CREATE TABLE SinkTable(s STRING) WITH ('connector' = 'COLLECTION')");

		tEnv().createTemporarySystemFunction("CustomScalarFunction", CustomScalarFunction.class);
		try {
			execInsertSqlAndWaitResult(
				"INSERT INTO SinkTable " +
				"SELECT CustomScalarFunction('test')");
			fail();
		} catch (CodeGenException e) {
			assertThat(
				e,
				hasMessage(
					equalTo(
						"Could not find an implementation method in class '" + CustomScalarFunction.class.getCanonicalName() +
						"' for function 'CustomScalarFunction' that matches the following signature: \n" +
						"java.lang.String eval(java.lang.String)")));
		}
	}

	@Test
	public void testRowTableFunction() throws Exception {
		final List<Row> sourceData = Arrays.asList(
			Row.of("1,2,3"),
			Row.of("2,3,4"),
			Row.of("3,4,5"),
			Row.of((String) null)
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of("1,2,3", new String[]{"1", "2", "3"}),
			Row.of("2,3,4", new String[]{"2", "3", "4"}),
			Row.of("3,4,5", new String[]{"3", "4", "5"})
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().executeSql("CREATE TABLE SourceTable(s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().executeSql("CREATE TABLE SinkTable(s STRING, sa ARRAY<STRING> NOT NULL) WITH ('connector' = 'COLLECTION')");

		tEnv().createTemporarySystemFunction("RowTableFunction", RowTableFunction.class);
		execInsertSqlAndWaitResult("INSERT INTO SinkTable SELECT t.s, t.sa FROM SourceTable, LATERAL TABLE(RowTableFunction(s)) t");

		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
	}

	@Test
	public void testDynamicTableFunction() throws Exception {
		final Row[] sinkData = new Row[]{
			Row.of("Test is a string"),
			Row.of("42"),
			Row.of((String) null)
		};

		TestCollectionTableFactory.reset();

		tEnv().executeSql("CREATE TABLE SinkTable(s STRING) WITH ('connector' = 'COLLECTION')");

		tEnv().createTemporarySystemFunction("DynamicTableFunction", DynamicTableFunction.class);
		execInsertSqlAndWaitResult(
			"INSERT INTO SinkTable " +
			"SELECT T1.s FROM TABLE(DynamicTableFunction('Test')) AS T1(s) " +
			"UNION ALL " +
			"SELECT CAST(T2.i AS STRING) FROM TABLE(DynamicTableFunction(42)) AS T2(i)" +
			"UNION ALL " +
			"SELECT CAST(T3.i AS STRING) FROM TABLE(DynamicTableFunction(CAST(NULL AS INT))) AS T3(i)");

		assertThat(TestCollectionTableFactory.getResult(), containsInAnyOrder(sinkData));
	}

	@Test
	public void testInvalidUseOfScalarFunction() {
		tEnv().executeSql("CREATE TABLE SinkTable(s STRING) WITH ('connector' = 'COLLECTION')");

		tEnv().createTemporarySystemFunction("PrimitiveScalarFunction", PrimitiveScalarFunction.class);
		try {
			tEnv().executeSql(
				"INSERT INTO SinkTable " +
				"SELECT * FROM TABLE(PrimitiveScalarFunction(1, 2, '3'))");
			fail();
		} catch (ValidationException e) {
			assertThat(
				e,
				hasMessage(
					containsString(
						"No match found for function signature PrimitiveScalarFunction(<NUMERIC>, <NUMERIC>, <CHARACTER>)")));
		}
	}

	@Test
	public void testInvalidUseOfSystemScalarFunction() {
		tEnv().executeSql("CREATE TABLE SinkTable(s STRING) WITH ('connector' = 'COLLECTION')");

		try {
			tEnv().explainSql(
				"INSERT INTO SinkTable " +
				"SELECT * FROM TABLE(MD5('3'))");
			fail();
		} catch (ValidationException e) {
			assertThat(
				e,
				hasMessage(
					containsString(
						"Currently, only table functions can emit rows.")));
		}
	}

	@Test
	public void testInvalidUseOfTableFunction() {
		tEnv().executeSql("CREATE TABLE SinkTable(s STRING) WITH ('connector' = 'COLLECTION')");

		tEnv().createTemporarySystemFunction("RowTableFunction", RowTableFunction.class);
		try {
			tEnv().executeSql(
				"INSERT INTO SinkTable " +
				"SELECT RowTableFunction('test')");
			fail();
		} catch (ValidationException e) {
			assertThat(
				e,
				hasMessage(
					containsString(
						"No match found for function signature RowTableFunction(<CHARACTER>)")));
		}
	}

	// --------------------------------------------------------------------------------------------
	// Test functions
	// --------------------------------------------------------------------------------------------

	/**
	 * Function that takes and returns primitives.
	 */
	public static class PrimitiveScalarFunction extends ScalarFunction {
		public long eval(int i, long l, String s) {
			return i + l + s.length();
		}
	}

	/**
	 * Function that takes and returns rows.
	 */
	public static class RowScalarFunction extends ScalarFunction {
		public @DataTypeHint("ROW<f0 INT, f1 STRING>") Row eval(
				@DataTypeHint("ROW<f0 INT, f1 STRING>") Row row) {
			return row;
		}
	}

	/**
	 * Function that is overloaded and takes use of annotations.
	 */
	public static class ComplexScalarFunction extends ScalarFunction {
		public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o, java.sql.Timestamp t) {
			return StringUtils.arrayAwareToString(o) + "+" + t.toString();
		}

		public @DataTypeHint("DECIMAL(5, 2)") BigDecimal eval() {
			return new BigDecimal("123.4"); // 1 digit is missing
		}

		public @DataTypeHint("RAW") ByteBuffer eval(byte[] bytes) {
			if (bytes == null) {
				return null;
			}
			return ByteBuffer.wrap(bytes);
		}
	}

	/**
	 * A function that returns either STRING or RAW type depending on a literal.
	 */
	public static class RawLiteralScalarFunction extends ScalarFunction {
		public Object eval(DayOfWeek dayOfWeek, Boolean asString) {
			if (dayOfWeek == null) {
				return null;
			}
			if (asString) {
				return dayOfWeek.toString();
			}
			return dayOfWeek;
		}

		@Override
		public TypeInference getTypeInference(DataTypeFactory typeFactory) {
			final DataType dayOfWeekDataType =
				DataTypes.RAW(DayOfWeek.class).toDataType(typeFactory);
			return TypeInference.newBuilder()
				.typedArguments(
					dayOfWeekDataType,
					DataTypes.BOOLEAN().notNull())
				.outputTypeStrategy((callContext -> {
					final boolean asString = callContext.getArgumentValue(1, Boolean.class)
						.orElse(false);
					if (asString) {
						return Optional.of(DataTypes.STRING());
					}
					return Optional.of(dayOfWeekDataType);
				}))
				.build();
		}
	}

	/**
	 * Function that has a custom type inference that is broader than the actual implementation.
	 */
	public static class CustomScalarFunction extends ScalarFunction {
		public Integer eval(Integer... args) {
			for (Integer o : args) {
				if (o != null) {
					return o;
				}
			}
			return null;
		}

		@Override
		public TypeInference getTypeInference(DataTypeFactory typeFactory) {
			return TypeInference.newBuilder()
				.outputTypeStrategy(TypeStrategies.argument(0))
				.build();
		}
	}

	/**
	 * Function that returns a row.
	 */
	@FunctionHint(output = @DataTypeHint("ROW<s STRING, sa ARRAY<STRING> NOT NULL>"))
	public static class RowTableFunction extends TableFunction<Row> {
		public void eval(String s) {
			if (s == null) {
				collect(null);
			} else {
				collect(Row.of(s, s.split(",")));
			}
		}
	}

	/**
	 * Function that returns a string or integer.
	 */
	public static class DynamicTableFunction extends TableFunction<Object> {
		@FunctionHint(output = @DataTypeHint("STRING"))
		public void eval(String s) {
			if (s == null) {
				fail();
			} else {
				collect(s + " is a string");
			}
		}

		@FunctionHint(output = @DataTypeHint("INT"))
		public void eval(Integer i) {
			if (i == null) {
				collect(null);
			} else {
				collect(i);
			}
		}
	}

	/**
	 * Function that returns which method has been called.
	 *
	 * <p>{@code f(NULL)} is determined by alphabetical method signature order.
	 */
	@SuppressWarnings("unused")
	public static class ClassNameScalarFunction extends ScalarFunction {

		public String eval(Integer i) {
			return "Integer";
		}

		public String eval(Boolean b) {
			return "Boolean";
		}

		public String eval(String s) {
			return "String";
		}
	}

	/**
	 * Function that returns which method has been called including {@code unknown}.
	 */
	@SuppressWarnings("unused")
	public static class ClassNameOrUnknownScalarFunction extends ClassNameScalarFunction {

		public String eval(@DataTypeHint("NULL") Object o) {
			return "<<unknown>>";
		}
	}

	/**
	 * Function that returns which method has been called but with default input type inference.
	 */
	@SuppressWarnings("unused")
	public static class WildcardClassNameScalarFunction extends ClassNameScalarFunction {

		public String eval(Object o) {
			return "Object";
		}

		@Override
		public TypeInference getTypeInference(DataTypeFactory typeFactory) {
			return TypeInference.newBuilder()
				.outputTypeStrategy(TypeStrategies.explicit(DataTypes.STRING()))
				.build();
		}
	}
}
