/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.resource.ResourceManagerTest;
import org.apache.flink.types.Row;
import org.apache.flink.util.UserClassLoaderJarTestUtils;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for catalog and system functions in a table environment. */
public class FunctionITCase extends BatchTestBase {

    private static String jarPath;

    @BeforeClass
    public static void setup() throws Exception {
        File jarFile =
                UserClassLoaderJarTestUtils.createJarFile(
                        TEMPORARY_FOLDER.newFolder("test-jar"),
                        "test-classloader-udf.jar",
                        ResourceManagerTest.LOWER_UDF_CLASS,
                        ResourceManagerTest.LOWER_UDF_CODE);
        jarPath = jarFile.toURI().toString();
    }

    @Test
    public void testCreateTemporarySystemFunctionByUsingJar() {
        String ddl =
                String.format(
                        "CREATE TEMPORARY SYSTEM FUNCTION f10 AS '%s' USING JAR '%s'",
                        ResourceManagerTest.LOWER_UDF_CLASS, jarPath);
        tEnv().executeSql(ddl);
        assertThat(Arrays.asList(tEnv().listFunctions())).contains("f10");

        tEnv().executeSql("DROP TEMPORARY SYSTEM FUNCTION f10");
        assertThat(Arrays.asList(tEnv().listFunctions())).doesNotContain("f10");
    }

    @Test
    public void testCreateCatalogFunctionByUsingJar() {
        String ddl =
                String.format(
                        "CREATE FUNCTION default_database.f11 AS '%s' USING JAR '%s'",
                        ResourceManagerTest.LOWER_UDF_CLASS, jarPath);
        tEnv().executeSql(ddl);
        assertThat(Arrays.asList(tEnv().listFunctions())).contains("f11");

        tEnv().executeSql("DROP FUNCTION default_database.f11");
        assertThat(Arrays.asList(tEnv().listFunctions())).doesNotContain("f11");
    }

    @Test
    public void testCreateTemporaryCatalogFunctionByUsingJar() {
        String ddl =
                String.format(
                        "CREATE TEMPORARY FUNCTION default_database.f12 AS '%s' USING JAR '%s'",
                        ResourceManagerTest.LOWER_UDF_CLASS, jarPath);
        tEnv().executeSql(ddl);
        assertThat(Arrays.asList(tEnv().listFunctions())).contains("f12");

        tEnv().executeSql("DROP TEMPORARY FUNCTION default_database.f12");
        assertThat(Arrays.asList(tEnv().listFunctions())).doesNotContain("f12");
    }

    @Test
    public void testUserDefinedTemporarySystemFunctionByUsingJar() throws Exception {
        String functionDDL =
                String.format(
                        "create temporary system function lowerUdf as '%s' using jar '%s'",
                        ResourceManagerTest.LOWER_UDF_CLASS, jarPath);

        String dropFunctionDDL = "drop temporary system function lowerUdf";
        testUserDefinedFunctionByUsingJar(functionDDL);
        // delete the function
        tEnv().executeSql(dropFunctionDDL);
    }

    @Test
    public void testUserDefinedRegularCatalogFunctionByUsingJar() throws Exception {
        String functionDDL =
                String.format(
                        "create function lowerUdf as '%s' using jar '%s'",
                        ResourceManagerTest.LOWER_UDF_CLASS, jarPath);

        String dropFunctionDDL = "drop function lowerUdf";
        testUserDefinedFunctionByUsingJar(functionDDL);
        // delete the function
        tEnv().executeSql(dropFunctionDDL);
    }

    @Test
    public void testUserDefinedTemporaryCatalogFunctionByUsingJar() throws Exception {
        String functionDDL =
                String.format(
                        "create temporary function lowerUdf as '%s' using jar '%s'",
                        ResourceManagerTest.LOWER_UDF_CLASS, jarPath);

        String dropFunctionDDL = "drop temporary function lowerUdf";
        testUserDefinedFunctionByUsingJar(functionDDL);
        // delete the function
        tEnv().executeSql(dropFunctionDDL);
    }

    private void testUserDefinedFunctionByUsingJar(String createFunctionDDL) throws Exception {
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
        tEnv().executeSql(createFunctionDDL);
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
    }
}
