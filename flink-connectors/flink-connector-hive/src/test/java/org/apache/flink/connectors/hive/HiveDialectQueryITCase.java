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

package org.apache.flink.connectors.hive;

import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.BeforeClass;
import org.junit.ComparisonFailure;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** Test hive query compatibility. */
public class HiveDialectQueryITCase {

    private static final String QTEST_DIR =
            Thread.currentThread().getContextClassLoader().getResource("query-test").getPath();
    private static final String SORT_QUERY_RESULTS = "SORT_QUERY_RESULTS";

    private static HiveCatalog hiveCatalog;
    private static TableEnvironment tableEnv;

    @BeforeClass
    public static void setup() throws Exception {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        // required by query like "src.`[k].*` from src"
        hiveCatalog.getHiveConf().setVar(HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT, "none");
        hiveCatalog.open();
        tableEnv = getTableEnvWithHiveCatalog();

        // create tables
        tableEnv.executeSql("create table foo (x int, y int)");
        tableEnv.executeSql("create table bar(i int, s string)");
        tableEnv.executeSql("create table baz(ai array<int>, d double)");
        tableEnv.executeSql(
                "create table employee(id int,name string,dep string,salary int,age int)");
        tableEnv.executeSql("create table dest (x int, y int)");
        tableEnv.executeSql("create table destp (x int) partitioned by (p string, q string)");
        tableEnv.executeSql("alter table destp add partition (p='-1',q='-1')");
        tableEnv.executeSql("CREATE TABLE src (key STRING, value STRING)");
        tableEnv.executeSql(
                "CREATE TABLE srcpart (key STRING, `value` STRING) PARTITIONED BY (ds STRING, hr STRING)");
        tableEnv.executeSql(
                "CREATE TABLE nested (\n"
                        + "  a int,\n"
                        + "  s1 struct<f1: boolean, f2: string, f3: struct<f4: int, f5: double>, f6: int>,\n"
                        + "  s2 struct<f7: string, f8: struct<f9 : boolean, f10: array<int>, f11: map<string, boolean>>>,\n"
                        + "  s3 struct<f12: array<struct<f13:string, f14:int>>>,\n"
                        + "  s4 map<string, struct<f15:int>>,\n"
                        + "  s5 struct<f16: array<struct<f17:string, f18:struct<f19:int>>>>,\n"
                        + "  s6 map<string, struct<f20:array<struct<f21:struct<f22:int>>>>>\n"
                        + ")");
        HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "foo")
                .addRow(new Object[] {1, 1})
                .addRow(new Object[] {2, 2})
                .addRow(new Object[] {3, 3})
                .addRow(new Object[] {4, 4})
                .addRow(new Object[] {5, 5})
                .commit();
        HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "bar")
                .addRow(new Object[] {1, "a"})
                .addRow(new Object[] {1, "aa"})
                .addRow(new Object[] {2, "b"})
                .commit();
        HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "baz")
                .addRow(new Object[] {Arrays.asList(1, 2, 3), 3.0})
                .commit();
        HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "src")
                .addRow(new Object[] {"1", "val1"})
                .addRow(new Object[] {"2", "val2"})
                .addRow(new Object[] {"3", "val3"})
                .commit();
        HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "employee")
                .addRow(new Object[] {1, "A", "Management", 4500, 55})
                .addRow(new Object[] {2, "B", "Management", 4400, 61})
                .addRow(new Object[] {3, "C", "Management", 4000, 42})
                .addRow(new Object[] {4, "D", "Production", 3700, 35})
                .addRow(new Object[] {5, "E", "Production", 3500, 24})
                .addRow(new Object[] {6, "F", "Production", 3600, 28})
                .addRow(new Object[] {7, "G", "Production", 3800, 35})
                .addRow(new Object[] {8, "H", "Production", 4000, 52})
                .addRow(new Object[] {9, "I", "Service", 4100, 40})
                .addRow(new Object[] {10, "J", "Sales", 4300, 36})
                .addRow(new Object[] {11, "K", "Sales", 4100, 38})
                .commit();

        // create functions
        tableEnv.executeSql(
                "create function hiveudf as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs'");
        tableEnv.executeSql(
                "create function hiveudtf as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode'");
        tableEnv.executeSql("create function myudtf as '" + MyUDTF.class.getName() + "'");

        // create temp functions
        tableEnv.executeSql(
                "create temporary function temp_abs as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs'");
    }

    @Test
    public void testQueries() throws Exception {
        File[] qfiles = new File(QTEST_DIR).listFiles();
        for (File qfile : qfiles) {
            runQFile(qfile);
        }
    }

    @Test
    public void testAdditionalQueries() throws Exception {
        List<String> toRun =
                new ArrayList<>(
                        Arrays.asList(
                                "select avg(salary) over (partition by dep) as avgsal from employee",
                                "select dep,name,salary from (select dep,name,salary,rank() over "
                                        + "(partition by dep order by salary desc) as rnk from employee) a where rnk=1",
                                "select salary,sum(cnt) over (order by salary)/sum(cnt) over "
                                        + "(order by salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from"
                                        + " (select salary,count(*) as cnt from employee group by salary) a"));
        if (HiveVersionTestUtil.HIVE_220_OR_LATER) {
            toRun.add(
                    "select weekofyear(current_timestamp()), dayofweek(current_timestamp()) from src limit 1");
        }
        for (String query : toRun) {
            runQuery(query);
        }
    }

    private void runQFile(File qfile) throws Exception {
        QTest qTest = extractQTest(qfile);
        for (int i = 0; i < qTest.statements.size(); i++) {
            String statement = qTest.statements.get(i);
            final String expectedResult = qTest.results.get(i);
            boolean isQuery = statement.toLowerCase().startsWith("select");
            // get rid of the trailing ;
            statement = statement.substring(0, statement.length() - 1);
            try {
                List<String> result =
                        CollectionUtil.iteratorToList(tableEnv.executeSql(statement).collect())
                                .stream()
                                .map(Row::toString)
                                .collect(Collectors.toList());
                if (isQuery && qTest.sortResults) {
                    Collections.sort(result);
                }
                String actualResult = result.toString();
                if (!actualResult.equals(expectedResult)) {
                    System.out.println();
                    throw new ComparisonFailure(
                            "Query output diff for qtest " + qfile.getName(),
                            expectedResult,
                            actualResult);
                }
            } catch (Exception e) {
                System.out.printf(
                        "Failed to run statement %s in qfile %s%n", statement, qfile.getName());
                throw e;
            }
        }
    }

    private static QTest extractQTest(File qfile) throws Exception {
        boolean sortResults = false;
        StringBuilder builder = new StringBuilder();
        int openBrackets = 0;
        boolean expectSqlStatement = true;
        List<String> sqlStatements = new ArrayList<>();
        List<String> results = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(qfile))) {
            String line = reader.readLine();
            while (line != null) {
                if (expectSqlStatement) {
                    line = line.trim();
                    if (!line.isEmpty()) {
                        if (line.startsWith("--")) {
                            String comment = line.substring("--".length());
                            sortResults = comment.trim().equalsIgnoreCase(SORT_QUERY_RESULTS);
                        } else {
                            if (builder.length() > 0) {
                                builder.append(" ");
                            }
                            builder.append(line);
                            if (line.endsWith(";")) {
                                // end of statement
                                sqlStatements.add(builder.toString());
                                builder = new StringBuilder();
                                expectSqlStatement = false;
                            }
                        }
                    }
                } else if (openBrackets > 0 || line.startsWith("[")) {
                    // we're in the results
                    if (builder.length() > 0) {
                        builder.append("\n");
                    }
                    builder.append(line);
                    for (int i = 0; i < line.length(); i++) {
                        if (line.charAt(i) == '[') {
                            openBrackets++;
                        }
                        if (line.charAt(i) == ']') {
                            openBrackets--;
                        }
                    }
                    if (openBrackets == 0) {
                        results.add(builder.toString());
                        builder = new StringBuilder();
                        expectSqlStatement = true;
                    }
                }

                line = reader.readLine();
            }
        }
        return new QTest(sqlStatements, results, sortResults);
    }

    private void runQuery(String query) throws Exception {
        try {
            CollectionUtil.iteratorToList(tableEnv.executeSql(query).collect());
        } catch (Exception e) {
            System.out.println("Failed to run " + query);
            throw e;
        }
    }

    private static TableEnvironment getTableEnvWithHiveCatalog() {
        TableEnvironment tableEnv =
                HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(SqlDialect.HIVE);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
        // automatically load hive module in hive-compatible mode
        HiveModule hiveModule = new HiveModule(hiveCatalog.getHiveVersion());
        CoreModule coreModule = CoreModule.INSTANCE;
        for (String loaded : tableEnv.listModules()) {
            tableEnv.unloadModule(loaded);
        }
        tableEnv.loadModule("hive", hiveModule);
        tableEnv.loadModule("core", coreModule);
        return tableEnv;
    }

    /** A test UDTF that takes multiple parameters. */
    public static class MyUDTF extends GenericUDTF {

        @Override
        public StructObjectInspector initialize(ObjectInspector[] argOIs)
                throws UDFArgumentException {
            return ObjectInspectorFactory.getStandardStructObjectInspector(
                    Collections.singletonList("col1"),
                    Collections.singletonList(
                            PrimitiveObjectInspectorFactory.javaIntObjectInspector));
        }

        @Override
        public void process(Object[] args) throws HiveException {
            int x = (int) args[0];
            for (int i = 0; i < x; i++) {
                forward(i);
            }
        }

        @Override
        public void close() throws HiveException {}
    }

    private static class QTest {
        final List<String> statements;
        final List<String> results;
        final boolean sortResults;

        private QTest(List<String> statements, List<String> results, boolean sortResults) {
            this.statements = statements;
            this.results = results;
            this.sortResults = sortResults;
            assertEquals(statements.size(), results.size());
        }
    }
}
