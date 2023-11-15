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
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.HintFlag;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.UserClassLoaderJarTestUtils;

import org.junit.Before;
import org.junit.Test;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CLASS;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CODE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests for catalog and system functions in a table environment.
 *
 * <p>Note: This class is meant for testing the core function support. Use {@code
 * org.apache.flink.table.planner.functions.BuiltInFunctionTestBase} for testing individual function
 * implementations.
 */
public class FunctionITCase extends StreamingTestBase {

    private static final String TEST_FUNCTION = TestUDF.class.getName();

    private static final Random random = new Random();
    private String udfClassName;
    private String jarPath;

    @Before
    @Override
    public void before() throws Exception {
        super.before();
        udfClassName = GENERATED_LOWER_UDF_CLASS + random.nextInt(50);
        jarPath =
                UserClassLoaderJarTestUtils.createJarFile(
                                TEMPORARY_FOLDER.newFolder(
                                        String.format("test-jar-%s", UUID.randomUUID())),
                                "test-classloader-udf.jar",
                                udfClassName,
                                String.format(GENERATED_LOWER_UDF_CODE, udfClassName))
                        .toURI()
                        .toString();
    }

    @Test
    public void testCreateCatalogFunctionInDefaultCatalog() {
        String ddl1 = "create function f1 as 'org.apache.flink.function.TestFunction'";
        tEnv().executeSql(ddl1);
        assertThat(Arrays.asList(tEnv().listFunctions())).contains("f1");

        tEnv().executeSql("DROP FUNCTION IF EXISTS default_catalog.default_database.f1");
        assertThat(Arrays.asList(tEnv().listFunctions())).doesNotContain("f1");
    }

    @Test
    public void testCreateFunctionWithFullPath() {
        String ddl1 =
                "create function default_catalog.default_database.f2 as"
                        + " 'org.apache.flink.function.TestFunction'";
        tEnv().executeSql(ddl1);
        assertThat(Arrays.asList(tEnv().listFunctions())).contains("f2");

        tEnv().executeSql("DROP FUNCTION IF EXISTS default_catalog.default_database.f2");
        assertThat(Arrays.asList(tEnv().listFunctions())).doesNotContain("f2");
    }

    @Test
    public void testCreateFunctionWithoutCatalogIdentifier() {
        String ddl1 =
                "create function default_database.f3 as"
                        + " 'org.apache.flink.function.TestFunction'";
        tEnv().executeSql(ddl1);
        assertThat(Arrays.asList(tEnv().listFunctions())).contains("f3");

        tEnv().executeSql("DROP FUNCTION IF EXISTS default_catalog.default_database.f3");
        assertThat(Arrays.asList(tEnv().listFunctions())).doesNotContain("f3");
    }

    @Test
    public void testCreateFunctionCatalogNotExists() {
        String ddl1 =
                "create function catalog1.database1.f3 as 'org.apache.flink.function.TestFunction'";

        try {
            tEnv().executeSql(ddl1);
        } catch (Exception e) {
            assertThat(e).hasMessage("Catalog catalog1 does not exist");
        }
    }

    @Test
    public void testCreateFunctionDBNotExists() {
        String ddl1 =
                "create function default_catalog.database1.f3 as 'org.apache.flink.function.TestFunction'";

        assertThatThrownBy(() -> tEnv().executeSql(ddl1))
                .hasMessage(
                        "Could not execute CREATE CATALOG FUNCTION:"
                                + " (catalogFunction: [Optional[This is a user-defined function]], identifier:"
                                + " [`default_catalog`.`database1`.`f3`], ignoreIfExists: [false], isTemporary: [false])");
    }

    @Test
    public void testCreateTemporaryCatalogFunction() {
        String ddl1 =
                "create temporary function default_catalog.default_database.f4"
                        + " as '"
                        + TEST_FUNCTION
                        + "'";

        String ddl2 =
                "create temporary function if not exists default_catalog.default_database.f4"
                        + " as '"
                        + TEST_FUNCTION
                        + "'";

        String ddl3 = "drop temporary function default_catalog.default_database.f4";

        String ddl4 = "drop temporary function if exists default_catalog.default_database.f4";

        tEnv().executeSql(ddl1);
        assertThat(Arrays.asList(tEnv().listFunctions())).contains("f4");

        tEnv().executeSql(ddl2);
        assertThat(Arrays.asList(tEnv().listFunctions())).contains("f4");

        tEnv().executeSql(ddl3);
        assertThat(Arrays.asList(tEnv().listFunctions())).doesNotContain("f4");

        tEnv().executeSql(ddl1);
        assertThatThrownBy(() -> tEnv().executeSql(ddl1))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Could not register temporary catalog function. A function 'default_catalog.default_database.f4' does already exist.");

        tEnv().executeSql(ddl3);
        tEnv().executeSql(ddl4);
        assertThatThrownBy(() -> tEnv().executeSql(ddl3))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Temporary catalog function `default_catalog`.`default_database`.`f4`"
                                + " doesn't exist");
    }

    @Test
    public void testCreateTemporarySystemFunction() {
        String ddl1 = "create temporary system function f5" + " as '" + TEST_FUNCTION + "'";

        String ddl2 =
                "create temporary system function if not exists f5" + " as '" + TEST_FUNCTION + "'";

        String ddl3 = "drop temporary system function f5";

        tEnv().executeSql(ddl1);
        tEnv().executeSql(ddl2);
        tEnv().executeSql(ddl3);
    }

    @Test
    public void testCreateTemporarySystemFunctionByUsingJar() throws Exception {
        String ddl =
                String.format(
                        "CREATE TEMPORARY SYSTEM FUNCTION f10 AS '%s' USING JAR '%s'",
                        udfClassName, jarPath);
        tEnv().executeSql(ddl);
        assertThat(Arrays.asList(tEnv().listFunctions())).contains("f10");

        try (CloseableIterator<Row> itor = tEnv().executeSql("SHOW JARS").collect()) {
            assertThat(itor.hasNext()).isFalse();
        }

        tEnv().executeSql("DROP TEMPORARY SYSTEM FUNCTION f10");
        assertThat(Arrays.asList(tEnv().listFunctions())).doesNotContain("f10");
    }

    @Test
    public void testCreateTemporarySystemFunctionWithTableAPI() {
        ResourceUri resourceUri = new ResourceUri(ResourceType.JAR, jarPath);
        tEnv().createTemporarySystemFunction("f10", udfClassName, Arrays.asList(resourceUri));
        assertThat(Arrays.asList(tEnv().listFunctions())).contains("f10");

        tEnv().executeSql("DROP TEMPORARY SYSTEM FUNCTION f10");
        assertThat(Arrays.asList(tEnv().listFunctions())).doesNotContain("f10");
    }

    @Test
    public void testUserDefinedTemporarySystemFunctionWithTableAPI() throws Exception {
        ResourceUri resourceUri = new ResourceUri(ResourceType.JAR, jarPath);
        String dropFunctionSql = "DROP TEMPORARY SYSTEM FUNCTION lowerUdf";
        testUserDefinedFunctionByUsingJar(
                environment ->
                        environment.createTemporarySystemFunction(
                                "lowerUdf", udfClassName, Arrays.asList(resourceUri)),
                dropFunctionSql);
    }

    @Test
    public void testCreateCatalogFunctionByUsingJar() {
        String ddl =
                String.format(
                        "CREATE FUNCTION default_database.f11 AS '%s' USING JAR '%s'",
                        udfClassName, jarPath);
        tEnv().executeSql(ddl);
        assertThat(Arrays.asList(tEnv().listFunctions())).contains("f11");

        tEnv().executeSql("DROP FUNCTION default_database.f11");
        assertThat(Arrays.asList(tEnv().listFunctions())).doesNotContain("f11");
    }

    @Test
    public void testCreateCatalogFunctionWithTableAPI() {
        ResourceUri resourceUri = new ResourceUri(ResourceType.JAR, jarPath);
        tEnv().createFunction("f11", udfClassName, Arrays.asList(resourceUri));
        assertThat(Arrays.asList(tEnv().listFunctions())).contains("f11");

        tEnv().executeSql("DROP FUNCTION default_database.f11");
        assertThat(Arrays.asList(tEnv().listFunctions())).doesNotContain("f11");
    }

    @Test
    public void testUserDefinedCatalogFunctionWithTableAPI() throws Exception {
        ResourceUri resourceUri = new ResourceUri(ResourceType.JAR, jarPath);
        String dropFunctionSql = "DROP FUNCTION default_database.lowerUdf";
        testUserDefinedFunctionByUsingJar(
                environment ->
                        environment.createFunction(
                                "lowerUdf", udfClassName, Arrays.asList(resourceUri)),
                dropFunctionSql);
    }

    @Test
    public void testCreateTemporaryCatalogFunctionByUsingJar() {
        String ddl =
                String.format(
                        "CREATE TEMPORARY FUNCTION default_database.f12 AS '%s' USING JAR '%s'",
                        udfClassName, jarPath);
        tEnv().executeSql(ddl);
        assertThat(Arrays.asList(tEnv().listFunctions())).contains("f12");

        tEnv().executeSql("DROP TEMPORARY FUNCTION default_database.f12");
        assertThat(Arrays.asList(tEnv().listFunctions())).doesNotContain("f12");
    }

    @Test
    public void testCreateTemporaryCatalogFunctionWithTableAPI() {
        ResourceUri resourceUri = new ResourceUri(ResourceType.JAR, jarPath);
        tEnv().createTemporaryFunction("f12", udfClassName, Arrays.asList(resourceUri));
        assertThat(Arrays.asList(tEnv().listFunctions())).contains("f12");

        tEnv().executeSql("DROP TEMPORARY FUNCTION default_database.f12");
        assertThat(Arrays.asList(tEnv().listFunctions())).doesNotContain("f12");
    }

    @Test
    public void testUserDefinedTemporaryCatalogFunctionWithTableAPI() throws Exception {
        ResourceUri resourceUri = new ResourceUri(ResourceType.JAR, jarPath);
        String dropFunctionSql = "DROP TEMPORARY FUNCTION default_database.lowerUdf";
        testUserDefinedFunctionByUsingJar(
                environment ->
                        environment.createTemporaryFunction(
                                "lowerUdf", udfClassName, Arrays.asList(resourceUri)),
                dropFunctionSql);
    }

    @Test
    public void testAlterFunction() throws Exception {
        String create = "create function f3 as 'org.apache.flink.function.TestFunction'";
        String alter = "alter function f3 as 'org.apache.flink.function.TestFunction2'";

        ObjectPath objectPath = new ObjectPath("default_database", "f3");
        assertThat(tEnv().getCatalog("default_catalog")).isPresent();
        Catalog catalog = tEnv().getCatalog("default_catalog").get();
        tEnv().executeSql(create);
        CatalogFunction beforeUpdate = catalog.getFunction(objectPath);
        assertThat(beforeUpdate.getClassName()).isEqualTo("org.apache.flink.function.TestFunction");

        tEnv().executeSql(alter);
        CatalogFunction afterUpdate = catalog.getFunction(objectPath);
        assertThat(afterUpdate.getClassName()).isEqualTo("org.apache.flink.function.TestFunction2");
    }

    @Test
    public void testAlterFunctionNonExists() {
        String alterUndefinedFunction =
                "ALTER FUNCTION default_catalog.default_database.f4"
                        + " as 'org.apache.flink.function.TestFunction'";

        String alterFunctionInWrongCatalog =
                "ALTER FUNCTION catalog1.default_database.f4 "
                        + "as 'org.apache.flink.function.TestFunction'";

        String alterFunctionInWrongDB =
                "ALTER FUNCTION default_catalog.db1.f4 "
                        + "as 'org.apache.flink.function.TestFunction'";

        assertThatThrownBy(() -> tEnv().executeSql(alterUndefinedFunction))
                .hasMessage(
                        "Function default_database.f4 does not exist in Catalog default_catalog.");

        assertThatThrownBy(() -> tEnv().executeSql(alterFunctionInWrongCatalog))
                .hasMessage("Catalog catalog1 does not exist");

        assertThatThrownBy(() -> tEnv().executeSql(alterFunctionInWrongDB))
                .hasMessage("Function db1.f4 does not exist in Catalog default_catalog.");
    }

    @Test
    public void testAlterTemporaryCatalogFunction() {
        String alterTemporary =
                "ALTER TEMPORARY FUNCTION default_catalog.default_database.f4"
                        + " as 'org.apache.flink.function.TestFunction'";

        assertThatThrownBy(() -> tEnv().executeSql(alterTemporary))
                .hasMessage("Alter temporary catalog function is not supported");
    }

    @Test
    public void testAlterTemporarySystemFunction() {
        String alterTemporary =
                "ALTER TEMPORARY SYSTEM FUNCTION default_catalog.default_database.f4"
                        + " as 'org.apache.flink.function.TestFunction'";

        assertThatThrownBy(() -> tEnv().executeSql(alterTemporary))
                .hasMessage("Alter temporary system function is not supported");
    }

    @Test
    public void testDropFunctionNonExists() {
        String dropUndefinedFunction = "DROP FUNCTION default_catalog.default_database.f4";

        String dropFunctionInWrongCatalog = "DROP FUNCTION catalog1.default_database.f4";

        String dropFunctionInWrongDB = "DROP FUNCTION default_catalog.db1.f4";

        assertThatThrownBy(() -> tEnv().executeSql(dropUndefinedFunction))
                .hasMessage(
                        "Function default_database.f4 does not exist in Catalog default_catalog.");

        assertThatThrownBy(() -> tEnv().executeSql(dropFunctionInWrongCatalog))
                .hasMessage("Catalog catalog1 does not exist");

        assertThatThrownBy(() -> tEnv().executeSql(dropFunctionInWrongDB))
                .hasMessage("Function db1.f4 does not exist in Catalog default_catalog.");
    }

    @Test
    public void testDropTemporaryFunctionNonExits() {
        String dropUndefinedFunction =
                "DROP TEMPORARY FUNCTION default_catalog.default_database.f4";
        String dropFunctionInWrongCatalog = "DROP TEMPORARY FUNCTION catalog1.default_database.f4";
        String dropFunctionInWrongDB = "DROP TEMPORARY FUNCTION default_catalog.db1.f4";

        assertThatThrownBy(() -> tEnv().executeSql(dropUndefinedFunction))
                .hasMessage(
                        "Temporary catalog function `default_catalog`.`default_database`.`f4` doesn't exist");

        assertThatThrownBy(() -> tEnv().executeSql(dropFunctionInWrongCatalog))
                .hasMessage(
                        "Temporary catalog function `catalog1`.`default_database`.`f4` doesn't exist");

        assertThatThrownBy(() -> tEnv().executeSql(dropFunctionInWrongDB))
                .hasMessage(
                        "Temporary catalog function "
                                + "`default_catalog`.`db1`.`f4` doesn't exist");
    }

    @Test
    public void testCreateDropTemporaryCatalogFunctionsWithDifferentIdentifier() {
        String createNoCatalogDB = "create temporary function f4" + " as '" + TEST_FUNCTION + "'";

        String dropNoCatalogDB = "drop temporary function f4";

        tEnv().executeSql(createNoCatalogDB);
        tEnv().executeSql(dropNoCatalogDB);

        String createNonExistsCatalog =
                "create temporary function catalog1.default_database.f4"
                        + " as '"
                        + TEST_FUNCTION
                        + "'";

        String dropNonExistsCatalog = "drop temporary function catalog1.default_database.f4";

        tEnv().executeSql(createNonExistsCatalog);
        tEnv().executeSql(dropNonExistsCatalog);

        String createNonExistsDB =
                "create temporary function default_catalog.db1.f4" + " as '" + TEST_FUNCTION + "'";

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

        assertThatThrownBy(() -> tEnv().executeSql(ddl2))
                .hasMessage(
                        "Could not drop temporary system function. A function named 'f5' doesn't exist.");
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
    public void testUserDefinedTemporarySystemFunctionByUsingJar() throws Exception {
        String functionDDL =
                String.format(
                        "create temporary system function lowerUdf as '%s' using jar '%s'",
                        udfClassName, jarPath);

        String dropFunctionDDL = "drop temporary system function lowerUdf";
        testUserDefinedFunctionByUsingJar(env -> env.executeSql(functionDDL), dropFunctionDDL);
    }

    @Test
    public void testUserDefinedRegularCatalogFunctionByUsingJar() throws Exception {
        String functionDDL =
                String.format(
                        "create function lowerUdf as '%s' using jar '%s'", udfClassName, jarPath);

        String dropFunctionDDL = "drop function lowerUdf";
        testUserDefinedFunctionByUsingJar(env -> env.executeSql(functionDDL), dropFunctionDDL);
    }

    @Test
    public void testUserDefinedTemporaryCatalogFunctionByUsingJar() throws Exception {
        String functionDDL =
                String.format(
                        "create temporary function lowerUdf as '%s' using jar '%s'",
                        udfClassName, jarPath);

        String dropFunctionDDL = "drop temporary function lowerUdf";
        testUserDefinedFunctionByUsingJar(env -> env.executeSql(functionDDL), dropFunctionDDL);
    }

    @Test
    public void testUserDefinedTemporarySystemFunction() throws Exception {
        String functionDDL = "create temporary system function addOne as '" + TEST_FUNCTION + "'";

        String dropFunctionDDL = "drop temporary system function addOne";
        testUserDefinedCatalogFunction(functionDDL);
        // delete the function
        tEnv().executeSql(dropFunctionDDL);
    }

    @Test
    public void testExpressionReducerByUsingJar() {
        String functionDDL =
                String.format(
                        "create temporary function lowerUdf as '%s' using jar '%s'",
                        udfClassName, jarPath);
        tEnv().executeSql(functionDDL);

        TableResult tableResult = tEnv().executeSql("SELECT lowerUdf('HELLO')");

        List<Row> actualRows = CollectionUtil.iteratorToList(tableResult.collect());
        assertThat(actualRows).isEqualTo(Arrays.asList(Row.of("hello")));

        tEnv().executeSql("drop temporary function lowerUdf");
    }

    /** Test udf class. */
    public static class TestUDF extends ScalarFunction {

        public Integer eval(Integer a, Integer b) {
            return a + b;
        }
    }

    private void testUserDefinedCatalogFunction(String createFunctionDDL) throws Exception {
        List<Row> sourceData =
                Arrays.asList(
                        Row.of(1, "1000", 2),
                        Row.of(2, "1", 3),
                        Row.of(3, "2000", 4),
                        Row.of(1, "2", 2),
                        Row.of(2, "3000", 3));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        String sourceDDL =
                "create table t1(a int, b varchar, c int) with ('connector' = 'COLLECTION')";
        String sinkDDL =
                "create table t2(a int, b varchar, c int) with ('connector' = 'COLLECTION')";

        String query = "select t1.a, t1.b, addOne(t1.a, 1) as c from t1";

        tEnv().executeSql(sourceDDL);
        tEnv().executeSql(sinkDDL);
        tEnv().executeSql(createFunctionDDL);
        Table t2 = tEnv().sqlQuery(query);
        t2.executeInsert("t2").await();

        List<Row> result = TestCollectionTableFactory.RESULT();
        assertThat(result).isEqualTo(sourceData);

        tEnv().executeSql("drop table t1");
        tEnv().executeSql("drop table t2");
    }

    private void testUserDefinedFunctionByUsingJar(FunctionCreator creator, String dropFunctionDDL)
            throws Exception {
        List<Row> sourceData =
                Arrays.asList(
                        Row.of(1, "JARK"),
                        Row.of(2, "RON"),
                        Row.of(3, "LeoNard"),
                        Row.of(1, "FLINK"),
                        Row.of(2, "CDC"));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        String sourceDDL = "create table t1(a int, b varchar) with ('connector' = 'COLLECTION')";
        String sinkDDL = "create table t2(a int, b varchar) with ('connector' = 'COLLECTION')";

        String query = "select a, lowerUdf(b) from t1";

        tEnv().executeSql(sourceDDL);
        tEnv().executeSql(sinkDDL);
        creator.createFunction(tEnv());
        Table t2 = tEnv().sqlQuery(query);
        t2.executeInsert("t2").await();

        List<Row> result = TestCollectionTableFactory.RESULT();
        List<Row> expected =
                Arrays.asList(
                        Row.of(1, "jark"),
                        Row.of(2, "ron"),
                        Row.of(3, "leonard"),
                        Row.of(1, "flink"),
                        Row.of(2, "cdc"));
        assertThat(result).isEqualTo(expected);

        tEnv().executeSql("drop table t1");
        tEnv().executeSql("drop table t2");
        // delete the function
        tEnv().executeSql(dropFunctionDDL);
    }

    @Test
    public void testPrimitiveScalarFunction() throws Exception {
        final List<Row> sourceData =
                Arrays.asList(Row.of(1, 1L, "-"), Row.of(2, 2L, "--"), Row.of(3, 3L, "---"));

        final List<Row> sinkData =
                Arrays.asList(Row.of(1, 3L, "-"), Row.of(2, 6L, "--"), Row.of(3, 9L, "---"));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE TestTable(i INT NOT NULL, b BIGINT NOT NULL, s STRING) WITH ('connector' = 'COLLECTION')");

        tEnv().createTemporarySystemFunction(
                        "PrimitiveScalarFunction", PrimitiveScalarFunction.class);
        tEnv().executeSql(
                        "INSERT INTO TestTable SELECT i, PrimitiveScalarFunction(i, b, s), s FROM TestTable")
                .await();

        assertThat(TestCollectionTableFactory.getResult()).isEqualTo(sinkData);
    }

    @Test
    public void testNullScalarFunction() throws Exception {
        final List<Row> sinkData =
                Collections.singletonList(
                        Row.of("Boolean", "String", "<<unknown>>", "String", "Object", "Boolean"));

        TestCollectionTableFactory.reset();

        tEnv().executeSql(
                        "CREATE TABLE TestTable(s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING) "
                                + "WITH ('connector' = 'COLLECTION')");

        tEnv().createTemporarySystemFunction(
                        "ClassNameScalarFunction", ClassNameScalarFunction.class);
        tEnv().createTemporarySystemFunction(
                        "ClassNameOrUnknownScalarFunction", ClassNameOrUnknownScalarFunction.class);
        tEnv().createTemporarySystemFunction(
                        "WildcardClassNameScalarFunction", WildcardClassNameScalarFunction.class);
        tEnv().executeSql(
                        "INSERT INTO TestTable SELECT "
                                + "ClassNameScalarFunction(NULL), "
                                + "ClassNameScalarFunction(CAST(NULL AS STRING)), "
                                + "ClassNameOrUnknownScalarFunction(NULL), "
                                + "ClassNameOrUnknownScalarFunction(CAST(NULL AS STRING)), "
                                + "WildcardClassNameScalarFunction(NULL), "
                                + "WildcardClassNameScalarFunction(CAST(NULL AS BOOLEAN))")
                .await();

        assertThat(TestCollectionTableFactory.getResult()).isEqualTo(sinkData);
    }

    @Test
    public void testRowScalarFunction() throws Exception {
        final List<Row> sourceData =
                Arrays.asList(
                        Row.of(1, Row.of(1, "1")),
                        Row.of(2, Row.of(2, "2")),
                        Row.of(3, Row.of(3, "3")));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE TestTable(i INT, r ROW<i INT, s STRING>) "
                                + "WITH ('connector' = 'COLLECTION')");

        tEnv().createTemporarySystemFunction("RowScalarFunction", RowScalarFunction.class);
        // the names of the function input and r differ
        tEnv().executeSql("INSERT INTO TestTable SELECT i, RowScalarFunction(r) FROM TestTable")
                .await();

        assertThat(TestCollectionTableFactory.getResult()).isEqualTo(sourceData);
    }

    @Test
    public void testComplexScalarFunction() throws Exception {
        final List<Row> sourceData =
                Arrays.asList(
                        Row.of(1, new byte[] {1, 2, 3}),
                        Row.of(2, new byte[] {2, 3, 4}),
                        Row.of(3, new byte[] {3, 4, 5}),
                        Row.of(null, null));

        final List<Row> sinkData =
                Arrays.asList(
                        Row.of(
                                1,
                                "1+2012-12-12 12:12:12.123456789",
                                "[1, 2, 3]+2012-12-12 12:12:12.123456789",
                                new BigDecimal("123.40"),
                                ByteBuffer.wrap(new byte[] {1, 2, 3})),
                        Row.of(
                                2,
                                "2+2012-12-12 12:12:12.123456789",
                                "[2, 3, 4]+2012-12-12 12:12:12.123456789",
                                new BigDecimal("123.40"),
                                ByteBuffer.wrap(new byte[] {2, 3, 4})),
                        Row.of(
                                3,
                                "3+2012-12-12 12:12:12.123456789",
                                "[3, 4, 5]+2012-12-12 12:12:12.123456789",
                                new BigDecimal("123.40"),
                                ByteBuffer.wrap(new byte[] {3, 4, 5})),
                        Row.of(
                                null,
                                "null+2012-12-12 12:12:12.123456789",
                                "null+2012-12-12 12:12:12.123456789",
                                new BigDecimal("123.40"),
                                null));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        final RawType<Object> rawType =
                new RawType<>(
                        Object.class, new KryoSerializer<>(Object.class, new ExecutionConfig()));

        tEnv().executeSql(
                        "CREATE TABLE SourceTable(i INT, b BYTES) "
                                + "WITH ('connector' = 'COLLECTION')");
        tEnv().executeSql(
                        "CREATE TABLE SinkTable("
                                + "  i INT, "
                                + "  s1 STRING, "
                                + "  s2 STRING, "
                                + "  d DECIMAL(5, 2),"
                                + "  r "
                                + rawType.asSerializableString()
                                + ") "
                                + "WITH ('connector' = 'COLLECTION')");

        tEnv().createTemporarySystemFunction("ComplexScalarFunction", ComplexScalarFunction.class);
        tEnv().executeSql(
                        "INSERT INTO SinkTable "
                                + "SELECT "
                                + "  i, "
                                + "  ComplexScalarFunction(i, TIMESTAMP '2012-12-12 12:12:12.123456789'), "
                                + "  ComplexScalarFunction(b, TIMESTAMP '2012-12-12 12:12:12.123456789'),"
                                + "  ComplexScalarFunction(), "
                                + "  ComplexScalarFunction(b) "
                                + "FROM SourceTable")
                .await();

        assertThat(TestCollectionTableFactory.getResult()).isEqualTo(sinkData);
    }

    @Test
    public void testCustomScalarFunction() throws Exception {
        final List<Row> sourceData =
                Arrays.asList(Row.of(1), Row.of(2), Row.of(3), Row.of((Integer) null));

        final List<Row> sinkData =
                Arrays.asList(
                        Row.of(1, 1, 5), Row.of(2, 2, 5), Row.of(3, 3, 5), Row.of(null, null, 5));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql("CREATE TABLE SourceTable(i INT) WITH ('connector' = 'COLLECTION')");
        tEnv().executeSql(
                        "CREATE TABLE SinkTable(i1 INT, i2 INT, i3 INT) WITH ('connector' = 'COLLECTION')");

        tEnv().createTemporarySystemFunction("CustomScalarFunction", CustomScalarFunction.class);
        tEnv().executeSql(
                        "INSERT INTO SinkTable "
                                + "SELECT "
                                + "  i, "
                                + "  CustomScalarFunction(i), "
                                + "  CustomScalarFunction(CAST(NULL AS INT), 5, i, i) "
                                + "FROM SourceTable")
                .await();

        assertThat(TestCollectionTableFactory.getResult()).isEqualTo(sinkData);
    }

    @Test
    public void testVarArgScalarFunction() {
        final List<Row> sourceData = Arrays.asList(Row.of("Bob", 1), Row.of("Alice", 2));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE SourceTable("
                                + "  s STRING, "
                                + "  i INT"
                                + ")"
                                + "WITH ("
                                + "  'connector' = 'COLLECTION'"
                                + ")");

        tEnv().createTemporarySystemFunction("VarArgScalarFunction", VarArgScalarFunction.class);

        final TableResult result =
                tEnv().executeSql(
                                "SELECT "
                                        + "  VarArgScalarFunction(), "
                                        + "  VarArgScalarFunction(i), "
                                        + "  VarArgScalarFunction(i, i), "
                                        + "  VarArgScalarFunction(s), "
                                        + "  VarArgScalarFunction(s, i) "
                                        + "FROM SourceTable");

        final List<Row> actual = CollectionUtil.iteratorToList(result.collect());
        final List<Row> expected =
                Arrays.asList(
                        Row.of(
                                "(INT...)",
                                "(INT...)",
                                "(INT...)",
                                "(STRING, INT...)",
                                "(STRING, INT...)"),
                        Row.of(
                                "(INT...)",
                                "(INT...)",
                                "(INT...)",
                                "(STRING, INT...)",
                                "(STRING, INT...)"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testRawLiteralScalarFunction() throws Exception {
        final List<Row> sourceData =
                Arrays.asList(
                        Row.of(1, DayOfWeek.MONDAY),
                        Row.of(2, DayOfWeek.FRIDAY),
                        Row.of(null, null));

        final Row[] sinkData =
                new Row[] {
                    Row.of(1, "MONDAY", DayOfWeek.MONDAY),
                    Row.of(1, "MONDAY", DayOfWeek.MONDAY),
                    Row.of(2, "FRIDAY", DayOfWeek.FRIDAY),
                    Row.of(2, "FRIDAY", DayOfWeek.FRIDAY),
                    Row.of(null, null, null),
                    Row.of(null, null, null)
                };

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        final RawType<DayOfWeek> rawType =
                new RawType<>(
                        DayOfWeek.class,
                        new KryoSerializer<>(DayOfWeek.class, new ExecutionConfig()));

        tEnv().executeSql(
                        "CREATE TABLE SourceTable("
                                + "  i INT, "
                                + "  r "
                                + rawType.asSerializableString()
                                + ") "
                                + "WITH ('connector' = 'COLLECTION')");
        tEnv().executeSql(
                        "CREATE TABLE SinkTable("
                                + "  i INT, "
                                + "  s STRING, "
                                + "  r "
                                + rawType.asSerializableString()
                                + ") "
                                + "WITH ('connector' = 'COLLECTION')");

        tEnv().createTemporarySystemFunction(
                        "RawLiteralScalarFunction", RawLiteralScalarFunction.class);
        tEnv().executeSql(
                        "INSERT INTO SinkTable "
                                + "  (SELECT "
                                + "    i, "
                                + "    RawLiteralScalarFunction(r, TRUE), "
                                + "    RawLiteralScalarFunction(r, FALSE) "
                                + "   FROM SourceTable)"
                                + "UNION ALL "
                                + "  (SELECT "
                                + "    i, "
                                + "    RawLiteralScalarFunction(r, TRUE), "
                                + "    RawLiteralScalarFunction(r, FALSE) "
                                + "  FROM SourceTable)")
                .await();

        assertThat(TestCollectionTableFactory.getResult()).containsExactlyInAnyOrder(sinkData);
    }

    @Test
    public void testStructuredScalarFunction() throws Exception {
        final List<Row> sourceData =
                Arrays.asList(Row.of("Bob", 42), Row.of("Alice", 12), Row.of(null, 0));

        final List<Row> sinkData =
                Arrays.asList(
                        Row.of("Bob 42", "Tyler"),
                        Row.of("Alice 12", "Tyler"),
                        Row.of("<<null>>", "Tyler"));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE SourceTable(s STRING, i INT NOT NULL) WITH ('connector' = 'COLLECTION')");
        tEnv().executeSql(
                        "CREATE TABLE SinkTable(s1 STRING, s2 STRING) WITH ('connector' = 'COLLECTION')");

        tEnv().createTemporarySystemFunction(
                        "StructuredScalarFunction", StructuredScalarFunction.class);
        tEnv().executeSql(
                        "INSERT INTO SinkTable "
                                + "SELECT "
                                + "  StructuredScalarFunction(StructuredScalarFunction(s, i)), "
                                + "  StructuredScalarFunction('Tyler', 27).name "
                                + "FROM SourceTable")
                .await();

        assertThat(TestCollectionTableFactory.getResult()).isEqualTo(sinkData);
    }

    @Test
    public void testInvalidCustomScalarFunction() {
        tEnv().executeSql("CREATE TABLE SinkTable(s STRING) WITH ('connector' = 'COLLECTION')");

        tEnv().createTemporarySystemFunction("CustomScalarFunction", CustomScalarFunction.class);
        assertThatThrownBy(
                        () ->
                                tEnv().executeSql(
                                                "INSERT INTO SinkTable SELECT CustomScalarFunction('test')")
                                        .await())
                .hasMessage(
                        "Could not find an implementation method 'eval' in class '"
                                + CustomScalarFunction.class.getName()
                                + "' for function 'CustomScalarFunction' that matches the following signature:\n"
                                + "java.lang.String eval(java.lang.String)");
    }

    @Test
    public void testRowTableFunction() throws Exception {
        final List<Row> sourceData =
                Arrays.asList(
                        Row.of("1,2,3"), Row.of("2,3,4"), Row.of("3,4,5"), Row.of((String) null));

        final List<Row> sinkData =
                Arrays.asList(
                        Row.of("1,2,3", new String[] {"1", "2", "3"}),
                        Row.of("2,3,4", new String[] {"2", "3", "4"}),
                        Row.of("3,4,5", new String[] {"3", "4", "5"}));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql("CREATE TABLE SourceTable(s STRING) WITH ('connector' = 'COLLECTION')");
        tEnv().executeSql(
                        "CREATE TABLE SinkTable(s STRING, sa ARRAY<STRING> NOT NULL) WITH ('connector' = 'COLLECTION')");

        tEnv().createTemporarySystemFunction("RowTableFunction", RowTableFunction.class);
        tEnv().executeSql(
                        "INSERT INTO SinkTable SELECT t.s, t.sa FROM SourceTable source, "
                                + "LATERAL TABLE(RowTableFunction(source.s)) t")
                .await();

        assertThat(TestCollectionTableFactory.getResult()).isEqualTo(sinkData);
    }

    @Test
    public void testStructuredTableFunction() throws Exception {
        final List<Row> sourceData =
                Arrays.asList(Row.of("Bob", 42), Row.of("Alice", 12), Row.of(null, 0));

        final List<Row> sinkData =
                Arrays.asList(Row.of("Bob", 42), Row.of("Alice", 12), Row.of(null, 0));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE SourceTable(s STRING, i INT NOT NULL) WITH ('connector' = 'COLLECTION')");
        tEnv().executeSql(
                        "CREATE TABLE SinkTable(s STRING, i INT NOT NULL) WITH ('connector' = 'COLLECTION')");

        tEnv().createTemporarySystemFunction(
                        "StructuredTableFunction", StructuredTableFunction.class);
        tEnv().executeSql(
                        "INSERT INTO SinkTable SELECT t.name, t.age FROM SourceTable, LATERAL TABLE(StructuredTableFunction(s, i)) t")
                .await();

        assertThat(TestCollectionTableFactory.getResult()).isEqualTo(sinkData);
    }

    @Test
    public void testDynamicCatalogTableFunction() throws Exception {
        final Row[] sinkData =
                new Row[] {Row.of("Test is a string"), Row.of("42"), Row.of((String) null)};

        TestCollectionTableFactory.reset();

        tEnv().executeSql("CREATE TABLE SinkTable(s STRING) WITH ('connector' = 'COLLECTION')");

        tEnv().createFunction("DynamicTableFunction", DynamicTableFunction.class);
        tEnv().executeSql(
                        "INSERT INTO SinkTable "
                                + "SELECT T1.s FROM TABLE(DynamicTableFunction('Test')) AS T1(s) "
                                + "UNION ALL "
                                + "SELECT CAST(T2.i AS STRING) FROM TABLE(DynamicTableFunction(42)) AS T2(i)"
                                + "UNION ALL "
                                + "SELECT CAST(T3.i AS STRING) FROM TABLE(DynamicTableFunction(CAST(NULL AS INT))) AS T3(i)")
                .await();

        assertThat(TestCollectionTableFactory.getResult()).containsExactlyInAnyOrder(sinkData);
    }

    @Test
    public void testInvalidUseOfScalarFunction() {
        tEnv().executeSql(
                        "CREATE TABLE SinkTable(s BIGINT NOT NULL) WITH ('connector' = 'COLLECTION')");

        tEnv().createTemporarySystemFunction(
                        "PrimitiveScalarFunction", PrimitiveScalarFunction.class);
        assertThatThrownBy(
                        () ->
                                tEnv().executeSql(
                                                "INSERT INTO SinkTable "
                                                        + "SELECT * FROM TABLE(PrimitiveScalarFunction(1, 2, '3'))"))
                .hasMessageContaining(
                        "SQL validation failed. Function 'PrimitiveScalarFunction' cannot be used as a table function.");
    }

    @Test
    public void testInvalidUseOfSystemScalarFunction() {
        tEnv().executeSql("CREATE TABLE SinkTable(s STRING) WITH ('connector' = 'COLLECTION')");

        assertThatThrownBy(
                        () ->
                                tEnv().explainSql(
                                                "INSERT INTO SinkTable "
                                                        + "SELECT * FROM TABLE(MD5('3'))"))
                .hasMessageContaining("Argument must be a table function: MD5");
    }

    @Test
    public void testInvalidUseOfTableFunction() {
        TestCollectionTableFactory.reset();

        tEnv().executeSql(
                        "CREATE TABLE SinkTable(s ROW<s STRING, sa ARRAY<STRING> NOT NULL>) WITH ('connector' = 'COLLECTION')");

        tEnv().createTemporarySystemFunction("RowTableFunction", RowTableFunction.class);

        assertThatThrownBy(
                        () ->
                                tEnv().explainSql(
                                                "INSERT INTO SinkTable "
                                                        + "SELECT RowTableFunction('test')"))
                .hasMessageContaining("Cannot call table function here: 'RowTableFunction'");
    }

    @Test
    public void testAggregateFunction() throws Exception {
        final List<Row> sourceData =
                Arrays.asList(
                        Row.of(LocalDateTime.parse("2007-12-03T10:15:30"), "Bob"),
                        Row.of(LocalDateTime.parse("2007-12-03T10:15:30"), "Alice"),
                        Row.of(LocalDateTime.parse("2007-12-03T10:15:30"), null),
                        Row.of(LocalDateTime.parse("2007-12-03T10:15:30"), "Jonathan"),
                        Row.of(LocalDateTime.parse("2007-12-03T10:15:32"), "Bob"),
                        Row.of(LocalDateTime.parse("2007-12-03T10:15:32"), "Alice"));

        final List<Row> sinkData =
                Arrays.asList(
                        Row.of(
                                "Jonathan",
                                "Alice=(Alice, 5), Bob=(Bob, 3), Jonathan=(Jonathan, 8)"),
                        Row.of("Alice", "Alice=(Alice, 5), Bob=(Bob, 3)"));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE SourceTable(ts TIMESTAMP(3), s STRING, WATERMARK FOR ts AS ts - INTERVAL '1' SECOND) "
                                + "WITH ('connector' = 'COLLECTION')");
        tEnv().executeSql(
                        "CREATE TABLE SinkTable(s1 STRING, s2 STRING) WITH ('connector' = 'COLLECTION')");

        tEnv().createTemporarySystemFunction(
                        "LongestStringAggregateFunction", LongestStringAggregateFunction.class);
        tEnv().createTemporarySystemFunction(
                        "RawMapViewAggregateFunction", RawMapViewAggregateFunction.class);

        tEnv().executeSql(
                        "INSERT INTO SinkTable "
                                + "SELECT LongestStringAggregateFunction(s), RawMapViewAggregateFunction(s) "
                                + "FROM SourceTable "
                                + "GROUP BY TUMBLE(ts, INTERVAL '1' SECOND)")
                .await();

        assertThat(TestCollectionTableFactory.getResult()).isEqualTo(sinkData);
    }

    @Test
    public void testLookupTableFunction() throws ExecutionException, InterruptedException {
        final List<Row> sourceData = Arrays.asList(Row.of("Bob"), Row.of("Alice"));

        final List<Row> sinkData =
                Arrays.asList(
                        Row.of("Bob", new byte[0]),
                        Row.of("Bob", new byte[] {66, 111, 98}),
                        Row.of("Alice", new byte[0]),
                        Row.of("Alice", new byte[] {65, 108, 105, 99, 101}));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE SourceTable1("
                                + "  s STRING, "
                                + "  proctime AS PROCTIME()"
                                + ")"
                                + "WITH ("
                                + "  'connector' = 'COLLECTION'"
                                + ")");

        tEnv().executeSql(
                        "CREATE TABLE SourceTable2("
                                + "  s STRING,"
                                + "  b BYTES"
                                + ")"
                                + "WITH ("
                                + "  'connector' = 'values',"
                                + "  'lookup-function-class' = '"
                                + LookupTableFunction.class.getName()
                                + "'"
                                + ")");

        tEnv().executeSql(
                        "CREATE TABLE SinkTable(s STRING, b BYTES) WITH ('connector' = 'COLLECTION')");

        tEnv().executeSql(
                        "INSERT INTO SinkTable "
                                + "SELECT SourceTable1.s, SourceTable2.b "
                                + "FROM SourceTable1 "
                                + "JOIN SourceTable2 FOR SYSTEM_TIME AS OF SourceTable1.proctime"
                                + "  ON SourceTable1.s = SourceTable2.s")
                .await();

        assertThat(TestCollectionTableFactory.getResult()).isEqualTo(sinkData);
    }

    @Test
    public void testSpecializedFunction() {
        final List<Row> sourceData =
                Arrays.asList(
                        Row.of("Bob", 1, new BigDecimal("123.45")),
                        Row.of("Alice", 2, new BigDecimal("123.456")));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE SourceTable("
                                + "  s STRING, "
                                + "  i INT,"
                                + "  d DECIMAL(6, 3)"
                                + ")"
                                + "WITH ("
                                + "  'connector' = 'COLLECTION'"
                                + ")");

        tEnv().createTemporarySystemFunction("TypeOfScalarFunction", TypeOfScalarFunction.class);

        final TableResult result =
                tEnv().executeSql(
                                "SELECT "
                                        + "  TypeOfScalarFunction('LITERAL'), "
                                        + "  TypeOfScalarFunction(s), "
                                        + "  TypeOfScalarFunction(i), "
                                        + "  TypeOfScalarFunction(d) "
                                        + "FROM SourceTable");

        final List<Row> actual = CollectionUtil.iteratorToList(result.collect());
        final List<Row> expected =
                Arrays.asList(
                        Row.of("CHAR(7) NOT NULL", "STRING", "INT", "DECIMAL(6, 3)"),
                        Row.of("CHAR(7) NOT NULL", "STRING", "INT", "DECIMAL(6, 3)"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testSpecializedFunctionWithExpressionEvaluation() {
        final List<Row> sourceData =
                Arrays.asList(
                        Row.of("Bob", new Integer[] {1, 2, 3}, new BigDecimal("123.000")),
                        Row.of("Bob", new Integer[] {4, 5, 6}, new BigDecimal("123.456")),
                        Row.of("Alice", new Integer[] {1, 2, 3}, null),
                        Row.of("Alice", null, new BigDecimal("123.456")));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE SourceTable("
                                + "  s STRING, "
                                + "  a ARRAY<INT>,"
                                + "  d DECIMAL(6, 3)"
                                + ")"
                                + "WITH ("
                                + "  'connector' = 'COLLECTION'"
                                + ")");

        tEnv().createTemporarySystemFunction(
                        "RowEqualityScalarFunction", RowEqualityScalarFunction.class);

        final TableResult result =
                tEnv().executeSql(
                                "SELECT "
                                        + "  s, "
                                        + "  RowEqualityScalarFunction((a, d), (a, 123.456)), "
                                        + "  RowEqualityScalarFunction((a, 123.456), (a, d))"
                                        + "FROM SourceTable");

        final List<Row> actual = CollectionUtil.iteratorToList(result.collect());
        final List<Row> expected =
                Arrays.asList(
                        Row.of("Bob", null, null),
                        Row.of(
                                "Bob",
                                Row.of(new Long[] {4L, 5L, 6L}, 123.456),
                                Row.of(new Long[] {4L, 5L, 6L}, 123.456)),
                        Row.of("Alice", null, null),
                        Row.of("Alice", Row.of(null, 123.456), Row.of(null, 123.456)));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testTimestampNotNull() {
        List<Row> sourceData = Arrays.asList(Row.of(1), Row.of(2));
        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE SourceTable(i INT, ts AS CAST(LOCALTIMESTAMP AS TIMESTAMP(3)), WATERMARK FOR ts AS ts) "
                                + "WITH ('connector' = 'COLLECTION')");
        tEnv().executeSql("CREATE FUNCTION MyYear AS '" + MyYear.class.getName() + "'");
        CollectionUtil.iteratorToList(
                tEnv().executeSql("SELECT MyYear(ts) FROM SourceTable").collect());
    }

    @Test
    public void testIsNullType() {
        List<Row> sourceData = Arrays.asList(Row.of(1), Row.of((Object) null));
        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql("CREATE TABLE SourceTable(i INT) WITH ('connector' = 'COLLECTION')");
        tEnv().executeSql("CREATE FUNCTION BoolToInt AS '" + BoolToInt.class.getName() + "'");
        CollectionUtil.iteratorToList(
                tEnv().executeSql(
                                "SELECT BoolToInt(i is null), BoolToInt(i is not null) FROM SourceTable")
                        .collect());
    }

    @Test
    public void testWithBoolNotNullTypeHint() {
        List<Row> sourceData = Arrays.asList(Row.of(1, 2), Row.of(2, 3));
        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE SourceTable(x INT NOT NULL,y INT) WITH ('connector' = 'COLLECTION')");
        tEnv().executeSql("CREATE FUNCTION BoolEcho AS '" + BoolEcho.class.getName() + "'");
        CollectionUtil.iteratorToList(
                tEnv().executeSql("SELECT BoolEcho(x=1 and y is null) FROM SourceTable").collect());
    }

    @Test
    public void testUsingAddJar() throws Exception {
        tEnv().executeSql(String.format("ADD JAR '%s'", jarPath));

        TableResult tableResult = tEnv().executeSql("SHOW JARS");
        assertThat(
                        CollectionUtil.iteratorToList(tableResult.collect())
                                .equals(
                                        Collections.singletonList(
                                                Row.of(new Path(jarPath).getPath()))))
                .isTrue();

        testUserDefinedFunctionByUsingJar(
                env ->
                        env.executeSql(
                                String.format(
                                        "create function lowerUdf as '%s' LANGUAGE JAVA",
                                        udfClassName)),
                "drop function lowerUdf");
    }

    @Test
    public void testArrayWithPrimitiveType() {
        List<Row> sourceData = Arrays.asList(Row.of(1, 2), Row.of(3, 4));
        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE SourceTable(i INT NOT NULL, j INT NOT NULL) WITH ('connector' = 'COLLECTION')");
        tEnv().executeSql(
                        "CREATE FUNCTION row_of_array AS '"
                                + RowOfArrayWithIntFunction.class.getName()
                                + "'");
        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql("SELECT row_of_array(i, j) FROM SourceTable").collect());
        assertThat(rows)
                .isEqualTo(
                        Arrays.asList(
                                Row.of(Row.of((Object) new int[] {1, 2})),
                                Row.of(Row.of((Object) new int[] {3, 4}))));
    }

    @Test
    public void testArrayWithPrimitiveBoxedType() {
        List<Row> sourceData = Arrays.asList(Row.of(1, null), Row.of(3, null));
        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE SourceTable(i INT NOT NULL, j INT) WITH ('connector' = 'COLLECTION')");
        tEnv().executeSql(
                        "CREATE FUNCTION row_of_array AS '"
                                + RowOfArrayWithIntegerFunction.class.getName()
                                + "'");
        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql("SELECT row_of_array(i, j) FROM SourceTable").collect());
        assertThat(rows)
                .isEqualTo(
                        Arrays.asList(
                                Row.of(Row.of((Object) new Integer[] {1, null})),
                                Row.of(Row.of((Object) new Integer[] {3, null}))));
    }

    // --------------------------------------------------------------------------------------------
    // Test functions
    // --------------------------------------------------------------------------------------------

    /** A function to convert boolean to int. */
    public static class BoolToInt extends ScalarFunction {
        public int eval(boolean b) {
            return b ? 1 : 0;
        }
    }

    /** A YEAR function that takes a NOT NULL parameter. */
    public static class MyYear extends ScalarFunction {
        public int eval(@DataTypeHint("TIMESTAMP(3) NOT NULL") LocalDateTime timestamp) {
            return timestamp.getYear();
        }
    }

    /** Function that takes and returns primitives. */
    public static class PrimitiveScalarFunction extends ScalarFunction {
        public long eval(int i, long l, String s) {
            return i + l + s.length();
        }
    }

    /** Function that takes and returns rows. */
    public static class RowScalarFunction extends ScalarFunction {
        public @DataTypeHint("ROW<f0 INT, f1 STRING>") Row eval(
                @DataTypeHint("ROW<f0 INT, f1 STRING>") Row row) {
            return row;
        }
    }

    /** Function that is overloaded and takes use of annotations. */
    public static class ComplexScalarFunction extends ScalarFunction {
        public String eval(
                @DataTypeHint(inputGroup = InputGroup.ANY) Object o, java.sql.Timestamp t) {
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

    /** A function that returns either STRING or RAW type depending on a literal. */
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
                    .typedArguments(dayOfWeekDataType, DataTypes.BOOLEAN().notNull())
                    .outputTypeStrategy(
                            (callContext -> {
                                final boolean asString =
                                        callContext
                                                .getArgumentValue(1, Boolean.class)
                                                .orElse(false);
                                if (asString) {
                                    return Optional.of(DataTypes.STRING());
                                }
                                return Optional.of(dayOfWeekDataType);
                            }))
                    .build();
        }
    }

    /** Function that has a custom type inference that is broader than the actual implementation. */
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

    /** Function for testing variable arguments. */
    @SuppressWarnings("unused")
    public static class VarArgScalarFunction extends ScalarFunction {
        public String eval(Integer... i) {
            return "(INT...)";
        }

        public String eval(String s, Integer... i2) {
            return "(STRING, INT...)";
        }
    }

    /** Function that returns a row. */
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

    /** Function that returns a string or integer. */
    public static class DynamicTableFunction extends TableFunction<Object> {
        @FunctionHint(output = @DataTypeHint("STRING"))
        public void eval(String s) {
            if (s == null) {
                fail("unknown failure");
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

    /** Function that returns which method has been called including {@code unknown}. */
    @SuppressWarnings("unused")
    public static class ClassNameOrUnknownScalarFunction extends ClassNameScalarFunction {

        public String eval(@DataTypeHint("NULL") Object o) {
            return "<<unknown>>";
        }
    }

    /** Function that returns which method has been called but with default input type inference. */
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

    /** Function that creates and consumes structured types. */
    public static class StructuredScalarFunction extends ScalarFunction {
        public StructuredUser eval(String name, int age) {
            if (name == null) {
                return null;
            }
            return new StructuredUser(name, age);
        }

        public String eval(StructuredUser user) {
            if (user == null) {
                return "<<null>>";
            }
            return user.toString();
        }
    }

    /** Table function that returns a structured type. */
    public static class StructuredTableFunction extends TableFunction<StructuredUser> {
        public void eval(String name, int age) {
            if (name == null) {
                collect(null);
            }
            collect(new StructuredUser(name, age));
        }
    }

    /** Example POJO for structured type. */
    public static class StructuredUser {
        public final String name;
        public final int age;

        public StructuredUser(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return name + " " + age;
        }
    }

    /** Function that aggregates strings and finds the longest string. */
    @FunctionHint(accumulator = @DataTypeHint("ROW<longestString STRING>"))
    public static class LongestStringAggregateFunction extends AggregateFunction<String, Row> {

        @Override
        public Row createAccumulator() {
            return Row.of((String) null);
        }

        public void accumulate(Row acc, String value) {
            if (value == null) {
                return;
            }
            final String longestString = (String) acc.getField(0);
            if (longestString == null || longestString.length() < value.length()) {
                acc.setField(0, value);
            }
        }

        @Override
        public String getValue(Row acc) {
            return (String) acc.getField(0);
        }
    }

    /** Aggregate function that tests raw types in map views. */
    public static class RawMapViewAggregateFunction
            extends AggregateFunction<String, RawMapViewAggregateFunction.AccWithRawView> {

        /** POJO is invalid and needs to be treated as raw type. */
        public static class RawPojo {
            public String a;
            public int b;

            public RawPojo(String s) {
                this.a = s;
                this.b = s.length();
            }

            @Override
            public String toString() {
                return "(" + a + ", " + b + ')';
            }
        }

        /** Accumulator with view that maps to raw type. */
        public static class AccWithRawView {
            @DataTypeHint(allowRawGlobally = HintFlag.TRUE)
            public MapView<String, RawPojo> view = new MapView<>();
        }

        @Override
        public AccWithRawView createAccumulator() {
            return new AccWithRawView();
        }

        public void accumulate(AccWithRawView acc, String value) throws Exception {
            if (value != null) {
                acc.view.put(value, new RawPojo(value));
            }
        }

        @Override
        public String getValue(AccWithRawView acc) {
            return acc.view.getMap().entrySet().stream()
                    .map(Objects::toString)
                    .sorted()
                    .collect(Collectors.joining(", "));
        }
    }

    /**
     * Synchronous table function that uses regular type inference for {@link LookupTableSource}.
     */
    @DataTypeHint("ROW<s STRING, b BYTES>")
    public static class LookupTableFunction extends TableFunction<Object> {
        public void eval(@DataTypeHint("STRING") StringData s) {
            collect(Row.of(s.toString(), new byte[0]));
            collect(Row.of(s.toString(), s.toBytes()));
        }
    }

    /** A specialized "compile time" function for returning the argument's data type. */
    public static class TypeOfScalarFunction extends ScalarFunction implements SpecializedFunction {

        private final String typeString;

        public TypeOfScalarFunction() {
            this("UNKNOWN");
        }

        public TypeOfScalarFunction(String typeString) {
            this.typeString = typeString;
        }

        @SuppressWarnings("unused")
        public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object unused) {
            return typeString;
        }

        @Override
        public TypeOfScalarFunction specialize(SpecializedContext context) {
            final List<DataType> dataTypes = context.getCallContext().getArgumentDataTypes();
            return new TypeOfScalarFunction(dataTypes.get(0).toString());
        }
    }

    /** A specialized "compile time" function for evaluating expressions. */
    public static class RowEqualityScalarFunction extends ScalarFunction
            implements SpecializedFunction {

        private static final DataType IN_ROW_TYPE =
                DataTypes.ROW(
                        DataTypes.FIELD("nested0", DataTypes.ARRAY(DataTypes.INT())),
                        DataTypes.FIELD("nested1", DataTypes.DECIMAL(6, 3)));

        private static final DataType OUT_ROW_TYPE =
                DataTypes.ROW(
                        DataTypes.FIELD("result0", DataTypes.ARRAY(DataTypes.BIGINT())),
                        DataTypes.FIELD("result1", DataTypes.DOUBLE()));

        private final ExpressionEvaluator rowEqualizer;
        private final ExpressionEvaluator rowCaster;
        private transient MethodHandle rowEqualizerHandle;
        private transient MethodHandle rowCasterHandle;

        public RowEqualityScalarFunction(
                ExpressionEvaluator rowEqualizer, ExpressionEvaluator rowCaster) {
            this.rowEqualizer = rowEqualizer;
            this.rowCaster = rowCaster;
        }

        public RowEqualityScalarFunction() {
            this(null, null); // filled during specialization
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .typedArguments(IN_ROW_TYPE, IN_ROW_TYPE)
                    .outputTypeStrategy(call -> Optional.of(OUT_ROW_TYPE))
                    .build();
        }

        @Override
        public RowEqualityScalarFunction specialize(SpecializedContext context) {
            final ExpressionEvaluator rowEqualizer =
                    context.createEvaluator(
                            $("a").isEqual($("b")).ifNull($("on_null")),
                            DataTypes.BOOLEAN().notNull().bridgedTo(boolean.class),
                            DataTypes.FIELD("a", IN_ROW_TYPE),
                            DataTypes.FIELD("b", IN_ROW_TYPE),
                            DataTypes.FIELD(
                                    "on_null",
                                    DataTypes.BOOLEAN().notNull().bridgedTo(boolean.class)));
            final ExpressionEvaluator rowCaster =
                    context.createEvaluator(
                            BuiltInFunctionDefinitions.CAST, OUT_ROW_TYPE, IN_ROW_TYPE);
            return new RowEqualityScalarFunction(rowEqualizer, rowCaster);
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            Preconditions.checkNotNull(rowEqualizer);
            Preconditions.checkNotNull(rowCaster);
            rowEqualizerHandle = rowEqualizer.open(context);
            rowCasterHandle = rowCaster.open(context);
        }

        public Row eval(Row a, Row b) {
            try {
                final boolean isEqual = (boolean) rowEqualizerHandle.invokeExact(a, b, true);
                if (isEqual) {
                    return (Row) rowCasterHandle.invokeExact(a);
                }
                return null;
            } catch (Throwable t) {
                throw new FlinkRuntimeException(t);
            }
        }
    }

    /** A function that takes BOOLEAN NOT NULL. */
    public static class BoolEcho extends ScalarFunction {
        public Boolean eval(@DataTypeHint("BOOLEAN NOT NULL") Boolean b) {
            return b;
        }
    }

    /** A function with Row of array with int as return type for test FLINK-31835. */
    public static class RowOfArrayWithIntFunction extends ScalarFunction {
        @DataTypeHint("Row<t ARRAY<INT NOT NULL>>")
        public Row eval(int... v) {
            return Row.of((Object) v);
        }
    }

    /** A function with Row of array with integer as return type for test FLINK-31835. */
    public static class RowOfArrayWithIntegerFunction extends ScalarFunction {
        @DataTypeHint("Row<t ARRAY<INT>>")
        public Row eval(Integer... v) {
            return Row.of((Object) v);
        }
    }

    private interface FunctionCreator {
        void createFunction(TableEnvironment environment);
    }
}
