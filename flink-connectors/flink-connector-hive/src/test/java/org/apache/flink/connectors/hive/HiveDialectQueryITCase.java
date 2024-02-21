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

import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.flink.table.planner.delegation.hive.HiveParserUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FileUtils;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.ComparisonFailure;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.utils.TableTestUtil.readFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test hive query compatibility. */
public class HiveDialectQueryITCase {

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

    private static final String QTEST_DIR =
            Thread.currentThread().getContextClassLoader().getResource("query-test").getPath();
    private static final String SORT_QUERY_RESULTS = "SORT_QUERY_RESULTS";

    private static HiveCatalog hiveCatalog;
    private static TableEnvironment tableEnv;
    private static String warehouse;

    @BeforeClass
    public static void setup() throws Exception {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        // required by query like "src.`[k].*` from src"
        hiveCatalog.getHiveConf().setVar(HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT, "none");
        hiveCatalog.open();
        tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.getConfig().set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED, false);
        warehouse = hiveCatalog.getHiveConf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);

        // create tables
        tableEnv.executeSql("create table foo (x int, y int)");
        tableEnv.executeSql("create table bar(I int, s string)");
        tableEnv.executeSql("create table baz(ai array<int>, d double)");
        tableEnv.executeSql(
                "create table employee(id int,name string,dep string,salary int,age int)");
        tableEnv.executeSql("create table dest (x int, y int)");
        tableEnv.executeSql("create table destp (x int) partitioned by (p string, q string)");
        tableEnv.executeSql("alter table destp add partition (p='-1',q='-1')");
        tableEnv.executeSql("CREATE TABLE src (key STRING, value STRING)");
        tableEnv.executeSql("CREATE TABLE t_sub_query (x int)");
        tableEnv.executeSql(
                "CREATE TABLE srcpart (key STRING, `value` STRING) PARTITIONED BY (ds STRING, hr STRING)");
        tableEnv.executeSql("create table binary_t (a int, ab array<binary>)");

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
        HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "t_sub_query")
                .addRow(new Object[] {2})
                .addRow(new Object[] {3})
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
                                        + " (select salary,count(*) as cnt from employee group by salary) a",
                                "select a, one from binary_t lateral view explode(ab) abs as one where a > 0",
                                "select /*+ mapjoin(dest) */ foo.x from foo join dest on foo.x = dest.x union"
                                        + " all select /*+ mapjoin(dest) */ foo.x from foo join dest on foo.y = dest.y",
                                "with cte as (select * from src) select * from cte",
                                "select 1 / 0"));
        if (HiveVersionTestUtil.HIVE_230_OR_LATER) {
            toRun.add(
                    "select weekofyear(current_timestamp()), dayofweek(current_timestamp()) from src limit 1");
        }
        for (String query : toRun) {
            runQuery(query);
        }
    }

    @Test
    public void testGroupingSets() throws Exception {
        List<String> results1 =
                CollectionUtil.iteratorToList(
                                tableEnv.executeSql(
                                                "select x,y,grouping__id,sum(1) from foo group by x,y grouping sets ((x,y),(x))")
                                        .collect())
                        .stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> results2 =
                CollectionUtil.iteratorToList(
                                tableEnv.executeSql(
                                                "select x,y,grouping(x),sum(1) from foo group by x,y grouping sets ((x,y),(x))")
                                        .collect())
                        .stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        if (HiveParserUtils.legacyGrouping(hiveCatalog.getHiveConf())) {
            assertThat(results1.toString())
                    .isEqualTo(
                            "["
                                    + "+I[1, 1, 3, 1],"
                                    + " +I[1, null, 1, 1],"
                                    + " +I[2, 2, 3, 1],"
                                    + " +I[2, null, 1, 1],"
                                    + " +I[3, 3, 3, 1],"
                                    + " +I[3, null, 1, 1],"
                                    + " +I[4, 4, 3, 1],"
                                    + " +I[4, null, 1, 1],"
                                    + " +I[5, 5, 3, 1],"
                                    + " +I[5, null, 1, 1]]");
            assertThat(results2.toString())
                    .isEqualTo(
                            "["
                                    + "+I[1, 1, 1, 1],"
                                    + " +I[1, null, 1, 1],"
                                    + " +I[2, 2, 1, 1],"
                                    + " +I[2, null, 1, 1],"
                                    + " +I[3, 3, 1, 1],"
                                    + " +I[3, null, 1, 1],"
                                    + " +I[4, 4, 1, 1],"
                                    + " +I[4, null, 1, 1],"
                                    + " +I[5, 5, 1, 1],"
                                    + " +I[5, null, 1, 1]]");
        } else {
            assertThat(results1.toString())
                    .isEqualTo(
                            "["
                                    + "+I[1, 1, 0, 1],"
                                    + " +I[1, null, 1, 1],"
                                    + " +I[2, 2, 0, 1],"
                                    + " +I[2, null, 1, 1],"
                                    + " +I[3, 3, 0, 1],"
                                    + " +I[3, null, 1, 1],"
                                    + " +I[4, 4, 0, 1],"
                                    + " +I[4, null, 1, 1],"
                                    + " +I[5, 5, 0, 1],"
                                    + " +I[5, null, 1, 1]]");
            assertThat(results2.toString())
                    .isEqualTo(
                            "["
                                    + "+I[1, 1, 0, 1],"
                                    + " +I[1, null, 0, 1],"
                                    + " +I[2, 2, 0, 1],"
                                    + " +I[2, null, 0, 1],"
                                    + " +I[3, 3, 0, 1],"
                                    + " +I[3, null, 0, 1],"
                                    + " +I[4, 4, 0, 1],"
                                    + " +I[4, null, 0, 1],"
                                    + " +I[5, 5, 0, 1],"
                                    + " +I[5, null, 0, 1]]");
        }
    }

    @Test
    public void testGroupingID() throws Exception {
        tableEnv.executeSql("create table temp(x int,y int,z int)");
        try {
            tableEnv.executeSql("insert into temp values (1,2,3)").await();
            List<String> results =
                    CollectionUtil.iteratorToList(
                                    tableEnv.executeSql(
                                                    "select x,y,z,grouping__id,grouping(x),grouping(z) from temp group by x,y,z with cube")
                                            .collect())
                            .stream()
                            .map(Row::toString)
                            .sorted()
                            .collect(Collectors.toList());
            if (HiveParserUtils.legacyGrouping(hiveCatalog.getHiveConf())) {
                // the grouping function in older version (2.2.0) hive has some serious bug and is
                // barely usable, therefore we only care about the group__id here
                assertThat(results.toString())
                        .isEqualTo(
                                "["
                                        + "+I[1, 2, 3, 7, 1, 1],"
                                        + " +I[1, 2, null, 3, 1, 0],"
                                        + " +I[1, null, 3, 5, 1, 1],"
                                        + " +I[1, null, null, 1, 1, 0],"
                                        + " +I[null, 2, 3, 6, 0, 1],"
                                        + " +I[null, 2, null, 2, 0, 0],"
                                        + " +I[null, null, 3, 4, 0, 1],"
                                        + " +I[null, null, null, 0, 0, 0]]");
            } else {
                assertThat(results.toString())
                        .isEqualTo(
                                "["
                                        + "+I[1, 2, 3, 0, 0, 0],"
                                        + " +I[1, 2, null, 1, 0, 1],"
                                        + " +I[1, null, 3, 2, 0, 0],"
                                        + " +I[1, null, null, 3, 0, 1],"
                                        + " +I[null, 2, 3, 4, 1, 0],"
                                        + " +I[null, 2, null, 5, 1, 1],"
                                        + " +I[null, null, 3, 6, 1, 0],"
                                        + " +I[null, null, null, 7, 1, 1]]");
            }
        } finally {
            tableEnv.executeSql("drop table temp");
        }
    }

    @Test
    public void testValues() throws Exception {
        tableEnv.executeSql(
                "create table test_values("
                        + "t tinyint,s smallint,i int,b bigint,f float,d double,de decimal(10,5),ts timestamp,dt date,"
                        + "str string,ch char(3),vch varchar(3),bl boolean)");
        try {
            tableEnv.executeSql(
                            "insert into table test_values values "
                                    + "(1,-2,3,4,1.1,1.1,1.1,'2021-08-04 16:26:33.4','2021-08-04',null,'1234','56',false)")
                    .await();
            List<Row> result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from test_values").collect());
            assertThat(result.toString())
                    .isEqualTo(
                            "[+I[1, -2, 3, 4, 1.1, 1.1, 1.10000, 2021-08-04T16:26:33.400, 2021-08-04, null, 123, 56, false]]");
        } finally {
            tableEnv.executeSql("drop table test_values");
        }
    }

    @Test
    public void testJoinInvolvingComplexType() throws Exception {
        tableEnv.executeSql("CREATE TABLE test2a (a ARRAY<INT>)");
        tableEnv.executeSql("CREATE TABLE test2b (a INT)");
        try {
            tableEnv.executeSql("insert into test2a SELECT ARRAY(1, 2)").await();
            tableEnv.executeSql("insert into test2b values (2), (3), (4)").await();
            List<Row> result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql(
                                            "select *  from test2b join test2a on test2b.a = test2a.a[1]")
                                    .collect());
            assertThat(result.toString()).isEqualTo("[+I[2, [1, 2]]]");
        } finally {
            tableEnv.executeSql("drop table test2a");
            tableEnv.executeSql("drop table test2b");
        }
    }

    @Test
    public void testWindowWithGrouping() throws Exception {
        tableEnv.executeSql("create table t(category int, live int, comments int)");
        try {
            tableEnv.executeSql("insert into table t values (1, 0, 2), (2, 0, 2), (3, 0, 2)")
                    .await();
            List<Row> result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql(
                                            "select grouping(category),"
                                                    + " lag(live) over(partition by grouping(category)) "
                                                    + "from t group by category, live")
                                    .collect());
            assertThat(result.toString()).isEqualTo("[+I[0, null], +I[0, 0], +I[0, 0]]");
            // test grouping with multiple parameters
            result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql(
                                            "select grouping(category, live),"
                                                    + " lead(live) over(partition by grouping(category, live)) "
                                                    + "from t group by category, live")
                                    .collect());
            assertThat(result.toString()).isEqualTo("[+I[0, 0], +I[0, 0], +I[0, null]]");
        } finally {
            tableEnv.executeSql("drop table t");
        }
    }

    @Test
    public void testCurrentDatabase() {
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select current_database()").collect());
        assertThat(result.toString()).isEqualTo("[+I[default]]");
        tableEnv.executeSql("create database db1");
        tableEnv.executeSql("use db1");
        result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select current_database()").collect());
        assertThat(result.toString()).isEqualTo("[+I[db1]]");
        // switch to default database for following test use default database
        tableEnv.executeSql("use default");
        tableEnv.executeSql("drop database db1");
    }

    @Test
    public void testDistinctFrom() throws Exception {
        try {
            tableEnv.executeSql("create table test(x string, y string)");
            tableEnv.executeSql(
                            "insert into test values ('q', 'q'), ('q', 'w'), (NULL, 'q'), ('q', NULL), (NULL, NULL)")
                    .await();
            List<Row> result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select x <=> y, (x <=> y) = false from test")
                                    .collect());
            assertThat(result.toString())
                    .isEqualTo(
                            "[+I[true, false], +I[false, true], +I[false, true], +I[false, true], +I[true, false]]");
        } finally {
            tableEnv.executeSql("drop table test");
        }
    }

    @Test
    public void testTableSample() throws Exception {
        tableEnv.executeSql("create table test_sample(a int)");
        try {
            tableEnv.executeSql("insert into test_sample values (2), (1), (3)").await();
            List<Row> result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from test_sample tablesample (2 rows)")
                                    .collect());
            assertThat(result.toString()).isEqualTo("[+I[2], +I[1]]");
            // test unsupported table sample
            String expectedMessage = "Only TABLESAMPLE (n ROWS) is supported.";
            assertSqlException(
                    "select * from test_sample tablesample (0.1 PERCENT)",
                    UnsupportedOperationException.class,
                    expectedMessage);
            assertSqlException(
                    "select * from test_sample tablesample (100M)",
                    UnsupportedOperationException.class,
                    expectedMessage);
            assertSqlException(
                    "select * from test_sample tablesample (BUCKET 3 OUT OF 64 ON a)",
                    UnsupportedOperationException.class,
                    expectedMessage);
        } finally {
            tableEnv.executeSql("drop table test_sample");
        }
    }

    private void assertSqlException(
            String sql, Class<?> expectedExceptionClz, String expectedMessage) {
        assertThatThrownBy(() -> tableEnv.executeSql(sql))
                .rootCause()
                .isInstanceOf(expectedExceptionClz)
                .hasMessage(expectedMessage);
    }

    @Test
    public void testInsertDirectory() throws Exception {
        String warehouse = hiveCatalog.getHiveConf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);

        // test insert overwrite directory with row format parameters
        tableEnv.executeSql("create table map_table (foo STRING , bar MAP<STRING, INT>)");
        tableEnv.executeSql(
                        "insert into map_table select 'A', map('math',100,'english',90,'history',85)")
                .await();

        String dataDir = warehouse + "/map_table_dir";
        tableEnv.executeSql(
                        String.format(
                                "INSERT OVERWRITE LOCAL DIRECTORY '%s'"
                                        + "ROW FORMAT DELIMITED \n"
                                        + "FIELDS TERMINATED BY ':'\n"
                                        + "COLLECTION ITEMS TERMINATED BY '#' \n"
                                        + "MAP KEYS TERMINATED BY '=' select * from map_table",
                                dataDir))
                .await();
        java.nio.file.Path[] files =
                FileUtils.listFilesInDirectory(Paths.get(dataDir), this::isDataFile)
                        .toArray(new Path[0]);
        assertThat(files.length).isEqualTo(1);
        String actualString = FileUtils.readFileUtf8(files[0].toFile());
        assertThat(actualString.trim()).isEqualTo("A:english=90#math=100#history=85");

        // test insert overwrite directory store as other format
        tableEnv.executeSql("create table d_table(x int) PARTITIONED BY (ds STRING, hr STRING)");
        tableEnv.executeSql("INSERT OVERWRITE TABLE d_table PARTITION (ds='1', hr='1') select 1")
                .await();
        tableEnv.executeSql("INSERT OVERWRITE TABLE d_table PARTITION (ds='1', hr='2') select 2")
                .await();

        String tableAggDir = warehouse + "/d_table_agg";
        // create an external referring to the directory to be inserted
        tableEnv.executeSql(
                String.format(
                        "create external table d_table_agg(x int, ds STRING) STORED AS RCFILE location '%s' ",
                        tableAggDir));
        // insert into directory stored as RCFILE
        tableEnv.executeSql(
                        String.format(
                                "INSERT OVERWRITE DIRECTORY '%s' STORED AS RCFILE select count(x), ds from d_table group by ds ",
                                tableAggDir))
                .await();
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select * from d_table_agg").collect());
        // verify the data read from the external table
        assertThat(result.toString()).isEqualTo("[+I[2, 1]]");
    }

    /**
     * Checks whether the give file is a data file which must not be a hidden file or a success
     * file.
     */
    private boolean isDataFile(Path path) {
        String successFileName =
                tableEnv.getConfig()
                        .get(FileSystemConnectorOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME);
        return !path.toFile().isHidden() && !path.toFile().getName().equals(successFileName);
    }

    @Test
    public void testScriptTransform() throws Exception {
        tableEnv.executeSql("CREATE TABLE dest1(key INT, ten INT, one INT, value STRING)");
        tableEnv.executeSql("CREATE TABLE destp1 (key string) partitioned by (p1 int,p2 string)");
        try {
            // test explain transform
            String actualPlan =
                    explainSql(
                            "select transform(key, value)"
                                    + " ROW FORMAT SERDE 'MySerDe'"
                                    + " WITH SERDEPROPERTIES ('p1'='v1','p2'='v2')"
                                    + " RECORDWRITER 'MyRecordWriter' "
                                    + " using 'cat' as (cola int, value string)"
                                    + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
                                    + " RECORDREADER 'MyRecordReader' from src");
            assertThat(actualPlan).isEqualTo(readFromResource("/explain/testScriptTransform.out"));

            // transform using + specified schema
            List<Row> result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql(
                                            "select * from (\n"
                                                    + " select transform(key, value) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\003'"
                                                    + " using 'cat'"
                                                    + " as (cola int, value string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\003'"
                                                    + " from src\n"
                                                    + " union all\n"
                                                    + " select transform(key, value) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\003'"
                                                    + " using 'cat' as (cola int, value string) from src) s")
                                    .collect());
            assertThat(result.toString())
                    .isEqualTo(
                            "[+I[1, val1], +I[2, val2], +I[3, val3], +I[1, val1], +I[2, val2], +I[3, val3]]");

            // transform using + distributed by
            tableEnv.executeSql(
                            "from src insert overwrite table dest1 map src.key,"
                                    + " CAST(src.key / 10 AS INT), CAST(src.key % 10 AS INT),"
                                    + " src.value using 'cat' as (tkey, ten, one, tvalue)"
                                    + " distribute by tvalue, tkey")
                    .await();
            result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from dest1").collect());
            assertThat(result.toString())
                    .isEqualTo("[+I[1, 0, 1, val1], +I[2, 0, 2, val2], +I[3, 0, 3, val3]]");

            // transform using with default output schema(key string, value string) + insert into
            // partitioned table
            // `value` after this script transform will be null, so that will fall into Hive's
            // default partition
            tableEnv.executeSql(
                            "insert into destp1 partition (p1=0,p2) (SELECT TRANSFORM(key, upper(value))"
                                    + " USING 'tr \t _' FROM ((select key, value from src) tmp))")
                    .await();
            result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from destp1").collect());
            String defaultPartitionName =
                    hiveCatalog.getHiveConf().getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME);
            assertThat(result.toString())
                    .isEqualTo(
                            String.format(
                                    "[+I[1_VAL1, 0, %s], +I[2_VAL2, 0, %s], +I[3_VAL3, 0, %s]]",
                                    defaultPartitionName,
                                    defaultPartitionName,
                                    defaultPartitionName));

            // test use binary record reader
            result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql(
                                            "select transform(key) using 'cat' as (tkey)"
                                                    + " RECORDREADER 'org.apache.hadoop.hive.ql.exec.BinaryRecordReader' from src")
                                    .collect());
            assertThat(result.toString()).isEqualTo("[+I[1\n2\n3\n]]");
        } finally {
            tableEnv.executeSql("drop table dest1");
            tableEnv.executeSql("drop table destp1");
        }
    }

    @Test
    public void testMultiInsert() throws Exception {
        tableEnv.executeSql("create table t1 (id bigint, name string)");
        tableEnv.executeSql("create table t2 (id bigint, name string)");
        tableEnv.executeSql("create table t3 (id bigint, name string, age int)");
        try {
            String multiInsertSql =
                    "from (select id, name, age from t3) t"
                            + " insert overwrite table t1 select id, name where age < 20"
                            + "  insert overwrite table t2 select id, name where age > 20";
            // test explain
            String actualPlan = explainSql(multiInsertSql);
            assertThat(actualPlan).isEqualTo(readFromResource("/explain/testMultiInsert.out"));
            // test execution
            tableEnv.executeSql("insert into table t3 values (1, 'test1', 18 ), (2, 'test2', 28 )")
                    .await();
            tableEnv.executeSql(multiInsertSql).await();
            List<Row> result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from t1").collect());
            assertThat(result.toString()).isEqualTo("[+I[1, test1]]");
            result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from t2").collect());
            assertThat(result.toString()).isEqualTo("[+I[2, test2]]");
        } finally {
            tableEnv.executeSql("drop table t1");
            tableEnv.executeSql("drop table t2");
            tableEnv.executeSql("drop table t3");
        }
    }

    @Test
    public void testNestType() throws Exception {
        tableEnv.executeSql("CREATE TABLE dummy (i int)");
        tableEnv.executeSql("INSERT INTO TABLE dummy VALUES (42)").await();
        tableEnv.executeSql(
                        "INSERT INTO TABLE nested SELECT\n"
                                + "  1, named_struct('f1', false, 'f2', 'foo', 'f3', named_struct('f4', 4, 'f5', cast(5.0 as double)), 'f6', 4),\n"
                                + "  named_struct('f7', 'f7', 'f8', named_struct('f9', true, 'f10', array(10, 11), 'f11', map('key1', true, 'key2', false))),\n"
                                + "  named_struct('f12', array(named_struct('f13', 'foo', 'f14', 14), named_struct('f13', 'bar', 'f14', 28))),\n"
                                + "  map('key1', named_struct('f15', 1), 'key2', named_struct('f15', 2)),\n"
                                + "  named_struct('f16', array(named_struct('f17', 'foo', 'f18', named_struct('f19', 14)), named_struct('f17', 'bar', 'f18', named_struct('f19', 28)))),\n"
                                + "  map('key1', named_struct('f20', array(named_struct('f21', named_struct('f22', 1)))),\n"
                                + "      'key2', named_struct('f20', array(named_struct('f21', named_struct('f22', 2)))))\n"
                                + "FROM dummy")
                .await();

        List<Row> result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select s3.f12[0].f14 FROM nested").collect());
        assertThat(result.toString()).isEqualTo("[+I[14]]");

        result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("SELECT s6['key1'].f20.f21.f22 FROM nested").collect());
        assertThat(result.toString()).isEqualTo("[+I[[1]]]");

        result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("SELECT s5.f16.f18.f19 FROM nested").collect());
        assertThat(result.toString()).isEqualTo("[+I[[14, 28]]]");
    }

    @Test
    public void testWithOverWindow() throws Exception {
        tableEnv.executeSql("create table over_test(a int, b int, c int, d int)");
        try {
            tableEnv.executeSql(
                            "insert into over_test values(3, 2, 1, 4), (1, 2, 3, 4), (2, 1, 4, 4)")
                    .await();
            List<Row> result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql(
                                            "select a, count(b) over(order by a rows between 1 preceding and 1 following) from over_test")
                                    .collect());
            assertThat(result.toString()).isEqualTo("[+I[1, 2], +I[2, 3], +I[3, 2]]");

            result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql(
                                            "select a, count(b) over(order by a rows between 1 preceding and 1 following),"
                                                    + " count(c) over(distribute by a sort by b range between 5 preceding and current row)"
                                                    + " from over_test")
                                    .collect());
            assertThat(result.toString()).isEqualTo("[+I[1, 2, 1], +I[2, 3, 1], +I[3, 2, 1]]");
        } finally {
            tableEnv.executeSql("drop table over_test");
        }
    }

    @Test
    public void testLoadData() throws Exception {
        tableEnv.executeSql("create table tab1 (col1 int, col2 int) stored as orc");
        tableEnv.executeSql("create table tab2 (col1 int, col2 int) STORED AS ORC");
        tableEnv.executeSql(
                "create table p_table(col1 int, col2 int) partitioned by (dateint int) row format delimited fields terminated by ','");
        try {
            String testLoadCsvFilePath =
                    Objects.requireNonNull(getClass().getResource("/csv/test.csv")).toString();
            // test explain
            String actualPlan =
                    explainSql(
                            String.format(
                                    "load data local inpath '%s' overwrite into table p_table partition (dateint=2022) ",
                                    testLoadCsvFilePath));
            assertThat(actualPlan)
                    .isEqualTo(
                            readFromResource("/explain/testLoadData.out")
                                    .replace("$filepath", testLoadCsvFilePath));

            // test load data into table
            tableEnv.executeSql("insert into tab1 values (1, 1), (1, 2)").await();
            tableEnv.executeSql(
                    String.format(
                            "load data local inpath '%s' INTO TABLE tab2", warehouse + "/tab1"));
            List<Row> result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from tab2").collect());
            assertThat(result.toString()).isEqualTo("[+I[1, 1], +I[1, 2]]");
            // there should still exist data in tab1
            result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from tab1").collect());
            assertThat(result.toString()).isEqualTo("[+I[1, 1], +I[1, 2]]");

            // test load data overwrite
            tableEnv.executeSql("insert overwrite table tab1 values (2, 1), (2, 2)").await();
            tableEnv.executeSql(
                            String.format(
                                    "load data inpath '%s' overwrite into table tab2",
                                    warehouse + "/tab1"))
                    .await();
            result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from tab2").collect());
            assertThat(result.toString()).isEqualTo("[+I[2, 1], +I[2, 2]]");

            // test load data into partition
            tableEnv.executeSql(
                            String.format(
                                    "load data local inpath '%s' into table p_table partition (dateint=2022) ",
                                    testLoadCsvFilePath))
                    .await();
            // the file should be removed
            assertThat(new File(testLoadCsvFilePath).exists()).isFalse();
            result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from p_table where dateint=2022")
                                    .collect());
            assertThat(result.toString())
                    .isEqualTo("[+I[1, 1, 2022], +I[2, 2, 2022], +I[3, 3, 2022]]");
        } finally {
            tableEnv.executeSql("drop table tab1");
            tableEnv.executeSql("drop table tab2");
            tableEnv.executeSql("drop table p_table");
        }
    }

    @Test
    public void testBoolComparison() throws Exception {
        tableEnv.executeSql("CREATE TABLE tbool (id int, a int, b string, c boolean)");
        try {
            tableEnv.executeSql("insert into tbool values (1, 1, '12', true), (2, 1, '0.4', false)")
                    .await();
            // test compare boolean with numeric/string type
            List<Row> results =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql(
                                            "select id from tbool where a = true and b != false and c = '1'")
                                    .collect());
            assertThat(results.toString()).isEqualTo("[+I[1]]");
        } finally {
            tableEnv.executeSql("drop table tbool");
        }
    }

    @Test
    public void testCastTimeStampToDecimal() throws Exception {
        try {
            String timestamp = "2012-12-19 11:12:19.1234567";
            // timestamp's behavior is different between hive2 and hive3, so
            // use HiveShim in this test to hide such difference
            HiveShim hiveShim = HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion());
            Object hiveTimestamp = hiveShim.toHiveTimestamp(Timestamp.valueOf(timestamp));
            TimestampObjectInspector timestampObjectInspector =
                    (TimestampObjectInspector)
                            HiveInspectors.getObjectInspector(TypeInfoFactory.timestampTypeInfo);

            HiveDecimal expectTimeStampDecimal =
                    timestampObjectInspector
                            .getPrimitiveWritableObject(hiveTimestamp)
                            .getHiveDecimal();

            // test cast timestamp to decimal explicitly
            List<Row> results =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql(
                                            String.format(
                                                    "select cast(cast('%s' as timestamp) as decimal(30,8))",
                                                    timestamp))
                                    .collect());
            assertThat(results.toString())
                    .isEqualTo(String.format("[+I[%s]]", expectTimeStampDecimal));

            // test insert timestamp type to decimal type directly
            tableEnv.executeSql("create table t1 (c1 DECIMAL(38,6))");
            tableEnv.executeSql("create table t2 (c2 TIMESTAMP)");
            tableEnv.executeSql(String.format("insert into t2 values('%s')", timestamp)).await();
            tableEnv.executeSql("insert into t1 select * from t2").await();
            results =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from t1").collect());
            assertThat(results.toString())
                    .isEqualTo(String.format("[+I[%s]]", expectTimeStampDecimal.toFormatString(6)));
        } finally {
            tableEnv.executeSql("drop table t1");
            tableEnv.executeSql("drop table t2");
        }
    }

    @Test
    public void testCount() throws Exception {
        tableEnv.executeSql("create table abcd (a int, b int, c int, d int)");
        tableEnv.executeSql(
                        "insert into abcd values (null,35,23,6), (10, 100, 23, 5), (10, 35, 23, 5)")
                .await();
        try {
            List<Row> results =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql(
                                            "select count(1), count(*), count(a),"
                                                    + " count(distinct a,b), count(distinct b,d), count(distinct b, c) from abcd")
                                    .collect());
            assertThat(results.toString()).isEqualTo("[+I[3, 3, 2, 2, 3, 2]]");
            assertThatThrownBy(() -> tableEnv.executeSql(" select count(a,b) from abcd"))
                    .hasRootCauseInstanceOf(UDFArgumentException.class)
                    .hasRootCauseMessage("DISTINCT keyword must be specified");
        } finally {
            tableEnv.executeSql("drop table abcd");
        }
    }

    @Test
    public void testLiteral() throws Exception {
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("SELECT asin(2), binary('1'), struct(2, 9, 7)")
                                .collect());
        if (HiveVersionTestUtil.HIVE_310_OR_LATER) {
            assertThat(result.toString()).isEqualTo("[+I[null, [49], +I[2, 9, 7]]]");
        } else {
            assertThat(result.toString()).isEqualTo("[+I[NaN, [49], +I[2, 9, 7]]]");
        }
        tableEnv.executeSql("create table test_decimal_literal(d decimal(10, 2))");
        try {
            tableEnv.executeSql("insert into test_decimal_literal values (1.2)").await();
            result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql(
                                            "select d / 3, d / 3L, 6 / d, 6L / d from test_decimal_literal")
                                    .collect());
            assertThat(result.toString())
                    .isEqualTo("[+I[0.400000, 0.400000, 5.00000000000, 5.00000000000]]");

        } finally {
            tableEnv.executeSql("drop table test_decimal_literal");
        }
    }

    @Test
    public void testCrossCatalogQueryNoHiveTable() throws Exception {
        // register a new in-memory catalog
        Catalog inMemoryCatalog = new GenericInMemoryCatalog("m_catalog", "db");
        tableEnv.registerCatalog("m_catalog", inMemoryCatalog);
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        // create a non-hive table
        tableEnv.executeSql(
                String.format(
                        "create table m_catalog.db.t1(x int, y string) "
                                + "with ('connector' = 'filesystem', 'path' = '%s', 'format'='csv')",
                        tempFolder.newFolder().toURI()));
        // create a non-hive partitioned table
        tableEnv.executeSql(
                String.format(
                        "create table m_catalog.db.t2(x int, p1 int,p2 string) partitioned by (p1, p2) "
                                + "with ('connector' = 'filesystem', 'path' = '%s', 'format'='csv')",
                        tempFolder.newFolder().toURI()));

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        // create a hive table
        tableEnv.executeSql("create table t1(x int, y string)");

        try {
            // insert data into the non-hive table and hive table
            tableEnv.executeSql("insert into m_catalog.db.t1 values (1, 'v1'), (2, 'v2')").await();
            tableEnv.executeSql(
                            "insert into m_catalog.db.t2 partition (p1=0,p2='static') values (1), (2), (1)")
                    .await();
            tableEnv.executeSql("insert into t1 values (1, 'h1'), (4, 'h2')").await();
            // query a non-hive table
            List<Row> result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from m_catalog.db.t1 sort by x desc")
                                    .collect());
            assertThat(result.toString()).isEqualTo("[+I[2, v2], +I[1, v1]]");
            // query a non-hive partitioned table
            result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from m_catalog.db.t2 cluster by x")
                                    .collect());
            assertThat(result.toString())
                    .isEqualTo("[+I[1, 0, static], +I[1, 0, static], +I[2, 0, static]]");
            // join a table using a hive table and a non-hive table
            result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql(
                                            "select ht1.x, ht1.y from m_catalog.db.t1 as mt1 join t1 as ht1 using (x)")
                                    .collect());
            assertThat(result.toString()).isEqualTo("[+I[1, h1]]");
        } finally {
            tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
            tableEnv.executeSql("drop table m_catalog.db.t1");
            tableEnv.executeSql("drop table m_catalog.db.t2");
            tableEnv.executeSql("drop table t1");
            tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        }
    }

    @Test
    public void testNullLiteralAsArgument() throws Exception {
        tableEnv.executeSql("create table test_ts(ts timestamp)");
        tableEnv.executeSql("create table t_bigint(ts bigint)");
        tableEnv.executeSql("create table t_array(a_t array<bigint>)");
        Long testTimestamp = 1671058803926L;
        // timestamp's behavior is different between hive2 and hive3, so
        // use HiveShim in this test to hide such difference
        HiveShim hiveShim = HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion());
        LocalDateTime expectDateTime =
                hiveShim.toFlinkTimestamp(
                        PrimitiveObjectInspectorUtils.getTimestamp(
                                testTimestamp, new JavaConstantLongObjectInspector(testTimestamp)));
        try {
            tableEnv.executeSql(
                            String.format(
                                    "insert into table t_bigint values (%s), (null)",
                                    testTimestamp))
                    .await();
            // the return data type for expression if(ts = 0, null ,ts) should be bigint instead of
            // string. otherwise, the all values in table t_bigint wll be null since
            // cast("1671058803926" as timestamp) will return null
            tableEnv.executeSql(
                            "insert into table test_ts select if(ts = 0, null ,ts) from t_bigint")
                    .await();
            List<Row> result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from test_ts").collect());
            // verify it can cast to timestamp value correctly
            assertThat(result.toString())
                    .isEqualTo(String.format("[+I[%s], +I[null]]", expectDateTime));

            // test cast null as bigint
            tableEnv.executeSql("insert into t_array select array(cast(null as bigint))").await();
            result =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from t_array").collect());
            assertThat(result.toString()).isEqualTo("[+I[null]]");
        } finally {
            tableEnv.executeSql("drop table test_ts");
            tableEnv.executeSql("drop table t_bigint");
            tableEnv.executeSql("drop table t_array");
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

    private String explainSql(String sql) {
        return (String)
                CollectionUtil.iteratorToList(tableEnv.executeSql("explain " + sql).collect())
                        .get(0)
                        .getField(0);
    }

    private static TableEnvironment getTableEnvWithHiveCatalog() {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
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
            assertThat(results).hasSize(statements.size());
        }
    }
}
