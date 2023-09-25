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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.module.CoreModuleFactory;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.runtime.utils.BatchAbstractTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FileUtils;

import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.catalog.hive.HiveTestUtils.createTableEnvWithHiveCatalog;
import static org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/** Tests {@link HiveTableSource}. */
public class HiveTableSourceITCase extends BatchAbstractTestBase {

    private static HiveCatalog hiveCatalog;
    private static TableEnvironment batchTableEnv;

    @BeforeClass
    public static void createCatalog() {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog.open();
        batchTableEnv = createTableEnv();
    }

    @AfterClass
    public static void closeCatalog() {
        if (null != hiveCatalog) {
            hiveCatalog.close();
        }
    }

    @Before
    public void setupSourceDatabaseAndData() {
        batchTableEnv.executeSql("CREATE DATABASE IF NOT EXISTS source_db");
    }

    @Test
    public void testReadNonPartitionedTable() throws Exception {
        final String dbName = "source_db";
        final String tblName = "test";
        batchTableEnv.executeSql(
                "CREATE TABLE source_db.test ( a INT, b INT, c STRING, d BIGINT, e DOUBLE)");
        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {1, 1, "a", 1000L, 1.11})
                .addRow(new Object[] {2, 2, "b", 2000L, 2.22})
                .addRow(new Object[] {3, 3, "c", 3000L, 3.33})
                .addRow(new Object[] {4, 4, "d", 4000L, 4.44})
                .commit();

        Table src = batchTableEnv.sqlQuery("select * from hive.source_db.test");
        List<Row> rows = CollectionUtil.iteratorToList(src.execute().collect());

        assertThat(rows).hasSize(4);
        assertThat(rows.get(0).toString()).isEqualTo("+I[1, 1, a, 1000, 1.11]");
        assertThat(rows.get(1).toString()).isEqualTo("+I[2, 2, b, 2000, 2.22]");
        assertThat(rows.get(2).toString()).isEqualTo("+I[3, 3, c, 3000, 3.33]");
        assertThat(rows.get(3).toString()).isEqualTo("+I[4, 4, d, 4000, 4.44]");
    }

    @Test
    public void testReadComplexDataType() throws Exception {
        final String dbName = "source_db";
        final String tblName = "complex_test";
        batchTableEnv.executeSql(
                "create table source_db.complex_test("
                        + "a array<int>, m map<int,string>, s struct<f1:int,f2:bigint>)");
        Integer[] array = new Integer[] {1, 2, 3};
        Map<Integer, String> map = new LinkedHashMap<>();
        map.put(1, "a");
        map.put(2, "b");
        Object[] struct = new Object[] {3, 3L};
        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {array, map, struct})
                .commit();
        Table src = batchTableEnv.sqlQuery("select * from hive.source_db.complex_test");
        List<Row> rows = CollectionUtil.iteratorToList(src.execute().collect());
        assertThat(rows).hasSize(1);
        assertThat((Integer[]) rows.get(0).getField(0)).isEqualTo(array);
        assertThat(rows.get(0).getField(1)).isEqualTo(map);
        assertThat(rows.get(0).getField(2)).isEqualTo(Row.of(struct[0], struct[1]));
    }

    @Test
    public void testReadParquetComplexDataType() throws Exception {
        batchTableEnv.executeSql(
                "create table parquet_complex_type_test("
                        + "a array<int>, m map<int,string>, s struct<f1:int,f2:bigint>) stored as parquet");
        String[] modules = batchTableEnv.listModules();
        // load hive module so that we can use array,map, named_struct function
        // for convenient writing complex data
        batchTableEnv.loadModule("hive", new HiveModule());
        batchTableEnv.useModules("hive", CoreModuleFactory.IDENTIFIER);

        batchTableEnv
                .executeSql(
                        "insert into parquet_complex_type_test"
                                + " select array(1, 2), map(1, 'val1', 2, 'val2'),"
                                + " named_struct('f1', 1,  'f2', 2)")
                .await();

        Table src = batchTableEnv.sqlQuery("select * from parquet_complex_type_test");
        List<Row> rows = CollectionUtil.iteratorToList(src.execute().collect());
        assertThat(rows.toString()).isEqualTo("[+I[[1, 2], {1=val1, 2=val2}, +I[1, 2]]]");
        batchTableEnv.unloadModule("hive");
    }

    /**
     * Test to read from partition table.
     *
     * @throws Exception
     */
    @Test
    public void testReadPartitionTable() throws Exception {
        final String dbName = "source_db";
        final String tblName = "test_table_pt";
        batchTableEnv.executeSql(
                "CREATE TABLE source_db.test_table_pt "
                        + "(`year` STRING, `value` INT) partitioned by (pt int)");
        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {"2014", 3})
                .addRow(new Object[] {"2014", 4})
                .commit("pt=0");
        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {"2015", 2})
                .addRow(new Object[] {"2015", 5})
                .commit("pt=1");
        Table src = batchTableEnv.sqlQuery("select * from hive.source_db.test_table_pt");
        List<Row> rows = CollectionUtil.iteratorToList(src.execute().collect());

        assertThat(rows).hasSize(4);
        Object[] rowStrings = rows.stream().map(Row::toString).sorted().toArray();
        assertThat(rowStrings)
                .isEqualTo(
                        new String[] {
                            "+I[2014, 3, 0]", "+I[2014, 4, 0]", "+I[2015, 2, 1]", "+I[2015, 5, 1]"
                        });
    }

    @Test
    public void testPartitionPrunning() throws Exception {
        final String dbName = "source_db";
        final String tblName = "test_table_pt_1";
        batchTableEnv.executeSql(
                "CREATE TABLE source_db.test_table_pt_1 "
                        + "(`year` STRING, `value` INT) partitioned by (pt int)");
        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {"2014", 3})
                .addRow(new Object[] {"2014", 4})
                .commit("pt=0");
        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {"2015", 2})
                .addRow(new Object[] {"2015", 5})
                .commit("pt=1");
        Table src =
                batchTableEnv.sqlQuery("select * from hive.source_db.test_table_pt_1 where pt = 0");
        // first check execution plan to ensure partition prunning works
        String[] explain = src.explain().split("==.*==\n");
        assertThat(explain).hasSize(4);
        String optimizedLogicalPlan = explain[2];
        assertThat(optimizedLogicalPlan)
                .as(optimizedLogicalPlan)
                .contains(
                        "table=[[hive, source_db, test_table_pt_1, partitions=[{pt=0}], project=[year, value]]]");
        // second check execute results
        List<Row> rows = CollectionUtil.iteratorToList(src.execute().collect());
        assertThat(rows).hasSize(2);
        Object[] rowStrings = rows.stream().map(Row::toString).sorted().toArray();
        assertThat(rowStrings).isEqualTo(new String[] {"+I[2014, 3, 0]", "+I[2014, 4, 0]"});

        // test the case that prune partition with reading partition from catalog without filter and
        // there exists default partition
        // insert null value for the partition column which will fall into the default partition
        batchTableEnv
                .executeSql(
                        "insert into source_db.test_table_pt_1 values ('2014', 1, null), ('2015', 2, null)")
                .await();
        // currently, the expression "is null" is not supported HiveCatalog#listPartitionsByFilter,
        // then the planer will list all partitions and then prue the partitions.
        // the test is to cover such case
        src =
                batchTableEnv.sqlQuery(
                        "select * from hive.source_db.test_table_pt_1 where pt is null");
        rows = CollectionUtil.iteratorToList(src.execute().collect());
        assertThat(rows.toString()).isEqualTo("[+I[2014, 1, null], +I[2015, 2, null]]");
    }

    @Test
    public void testPartitionFilter() throws Exception {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        TestPartitionFilterCatalog catalog =
                new TestPartitionFilterCatalog(
                        hiveCatalog.getName(),
                        hiveCatalog.getDefaultDatabase(),
                        hiveCatalog.getHiveConf(),
                        hiveCatalog.getHiveVersion());
        tableEnv.registerCatalog(catalog.getName(), catalog);
        tableEnv.useCatalog(catalog.getName());
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.part(x int) partitioned by (p1 int,p2 string)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "part")
                    .addRow(new Object[] {1})
                    .commit("p1=1,p2='a'");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "part")
                    .addRow(new Object[] {2})
                    .commit("p1=2,p2='b'");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "part")
                    .addRow(new Object[] {3})
                    .commit("p1=3,p2='c'");
            // test string partition columns with special characters
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "part")
                    .addRow(new Object[] {4})
                    .commit("p1=4,p2='c:2'");
            Table query =
                    tableEnv.sqlQuery("select x from db1.part where p1>1 or p2<>'a' order by x");
            String[] explain = query.explain().split("==.*==\n");
            assertThat(catalog.fallback).isFalse();
            String optimizedPlan = explain[2];
            assertThat(optimizedPlan)
                    .as(optimizedPlan)
                    .contains(
                            "table=[[test-catalog, db1, part, partitions=[{p1=2, p2=b}, {p1=3, p2=c}, {p1=4, p2=c:2}]");
            List<Row> results = CollectionUtil.iteratorToList(query.execute().collect());
            assertThat(results.toString()).isEqualTo("[+I[2], +I[3], +I[4]]");

            query = tableEnv.sqlQuery("select x from db1.part where p1>2 and p2<='a' order by x");
            explain = query.explain().split("==.*==\n");
            assertThat(catalog.fallback).isFalse();
            optimizedPlan = explain[2];
            assertThat(optimizedPlan).as(optimizedPlan).contains("Values(tuples=[[]], values=[x])");
            results = CollectionUtil.iteratorToList(query.execute().collect());
            assertThat(results.toString()).isEqualTo("[]");

            query = tableEnv.sqlQuery("select x from db1.part where p1 in (1,3,5) order by x");
            explain = query.explain().split("==.*==\n");
            assertThat(catalog.fallback).isFalse();
            optimizedPlan = explain[2];
            assertThat(optimizedPlan)
                    .as(optimizedPlan)
                    .contains(
                            "table=[[test-catalog, db1, part, partitions=[{p1=1, p2=a}, {p1=3, p2=c}], project=[x]]]");
            results = CollectionUtil.iteratorToList(query.execute().collect());
            assertThat(results.toString()).isEqualTo("[+I[1], +I[3]]");

            query =
                    tableEnv.sqlQuery(
                            "select x from db1.part where (p1=1 and p2='a') or ((p1=2 and p2='b') or p2='d') order by x");
            explain = query.explain().split("==.*==\n");
            assertThat(catalog.fallback).isFalse();
            optimizedPlan = explain[2];
            assertThat(optimizedPlan)
                    .as(optimizedPlan)
                    .contains(
                            "table=[[test-catalog, db1, part, partitions=[{p1=1, p2=a}, {p1=2, p2=b}], project=[x]]]");
            results = CollectionUtil.iteratorToList(query.execute().collect());
            assertThat(results.toString()).isEqualTo("[+I[1], +I[2]]");

            query = tableEnv.sqlQuery("select x from db1.part where p2 = 'c:2' order by x");
            explain = query.explain().split("==.*==\n");
            assertThat(catalog.fallback).isFalse();
            optimizedPlan = explain[2];
            assertThat(optimizedPlan)
                    .as(optimizedPlan)
                    .contains(
                            "table=[[test-catalog, db1, part, partitions=[{p1=4, p2=c:2}], project=[x]]]");
            results = CollectionUtil.iteratorToList(query.execute().collect());
            assertThat(results.toString()).isEqualTo("[+I[4]]");

            query = tableEnv.sqlQuery("select x from db1.part where '' = p2");
            explain = query.explain().split("==.*==\n");
            assertThat(catalog.fallback).isFalse();
            optimizedPlan = explain[2];
            assertThat(optimizedPlan).as(optimizedPlan).contains("Values(tuples=[[]], values=[x])");
            results = CollectionUtil.iteratorToList(query.execute().collect());
            assertThat(results.toString()).isEqualTo("[]");
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testPartitionFilterDateTimestamp() throws Exception {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        TestPartitionFilterCatalog catalog =
                new TestPartitionFilterCatalog(
                        hiveCatalog.getName(),
                        hiveCatalog.getDefaultDatabase(),
                        hiveCatalog.getHiveConf(),
                        hiveCatalog.getHiveVersion());
        tableEnv.registerCatalog(catalog.getName(), catalog);
        tableEnv.useCatalog(catalog.getName());
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql(
                    "create table db1.part(x int) partitioned by (p1 date,p2 timestamp)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "part")
                    .addRow(new Object[] {1})
                    .commit("p1='2018-08-08',p2='2018-08-08 08:08:08.1'");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "part")
                    .addRow(new Object[] {2})
                    .commit("p1='2018-08-09',p2='2018-08-08 08:08:09.1'");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "part")
                    .addRow(new Object[] {3})
                    .commit("p1='2018-08-10',p2='2018-08-08 08:08:10.1'");

            Table query =
                    tableEnv.sqlQuery(
                            "select x from db1.part where p1>cast('2018-08-09' as date) and p2<>cast('2018-08-08 08:08:09.1' as timestamp)");
            String[] explain = query.explain().split("==.*==\n");
            assertThat(catalog.fallback).isTrue();
            String optimizedPlan = explain[2];
            assertThat(optimizedPlan)
                    .as(optimizedPlan)
                    .contains(
                            "table=[[test-catalog, db1, part, partitions=[{p1=2018-08-10, p2=2018-08-08 08:08:10.1}]");
            List<Row> results = CollectionUtil.iteratorToList(query.execute().collect());
            assertThat(results.toString()).isEqualTo("[+I[3]]");

            // filter by timestamp partition
            query =
                    tableEnv.sqlQuery(
                            "select x from db1.part where timestamp '2018-08-08 08:08:09.1' = p2");
            results = CollectionUtil.iteratorToList(query.execute().collect());
            assertThat(results.toString()).isEqualTo("[+I[2]]");
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testProjectionPushDown() throws Exception {
        batchTableEnv.executeSql(
                "create table src(x int,y string) partitioned by (p1 bigint, p2 string)");
        try {
            HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "src")
                    .addRow(new Object[] {1, "a"})
                    .addRow(new Object[] {2, "b"})
                    .commit("p1=2013, p2='2013'");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "src")
                    .addRow(new Object[] {3, "c"})
                    .commit("p1=2014, p2='2014'");
            Table table =
                    batchTableEnv.sqlQuery(
                            "select p1, count(y) from hive.`default`.src group by p1");
            String[] explain = table.explain().split("==.*==\n");
            assertThat(explain).hasSize(4);
            String logicalPlan = explain[2];
            String expectedExplain = "table=[[hive, default, src, project=[p1, y]]]";
            assertThat(logicalPlan).as(logicalPlan).contains(expectedExplain);

            List<Row> rows = CollectionUtil.iteratorToList(table.execute().collect());
            assertThat(rows).hasSize(2);
            Object[] rowStrings = rows.stream().map(Row::toString).sorted().toArray();
            assertThat(rowStrings).isEqualTo(new String[] {"+I[2013, 2]", "+I[2014, 1]"});
        } finally {
            batchTableEnv.executeSql("drop table src");
        }
    }

    @Test
    public void testLimitPushDown() throws Exception {
        batchTableEnv.executeSql("create table src (a string)");
        try {
            HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "src")
                    .addRow(new Object[] {"a"})
                    .addRow(new Object[] {"b"})
                    .addRow(new Object[] {"c"})
                    .addRow(new Object[] {"d"})
                    .commit();
            Table table = batchTableEnv.sqlQuery("select * from hive.`default`.src limit 1");
            String[] explain = table.explain().split("==.*==\n");
            assertThat(explain).hasSize(4);
            String logicalPlan = explain[2];
            assertThat(logicalPlan)
                    .as(logicalPlan)
                    .contains("table=[[hive, default, src, limit=[1]]]");

            List<Row> rows = CollectionUtil.iteratorToList(table.execute().collect());
            assertThat(rows).hasSize(1);
            Object[] rowStrings = rows.stream().map(Row::toString).sorted().toArray();
            assertThat(rowStrings).isEqualTo(new String[] {"+I[a]"});
        } finally {
            batchTableEnv.executeSql("drop table src");
        }
    }

    @Test
    public void testParallelismSetting() throws Exception {
        final String dbName = "source_db";
        final String tblName = "test_parallelism";
        batchTableEnv.executeSql(
                "CREATE TABLE source_db.test_parallelism "
                        + "(`year` STRING, `value` INT) partitioned by (pt int)");

        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {"2014", 3})
                .addRow(new Object[] {"2014", 4})
                .commit("pt=0");
        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {"2015", 2})
                .addRow(new Object[] {"2015", 5})
                .commit("pt=1");

        Table table = batchTableEnv.sqlQuery("select * from hive.source_db.test_parallelism");
        testParallelismSettingTranslateAndAssert(2, table, batchTableEnv);
    }

    @Test
    public void testParallelismSettingWithFileNum() throws IOException {
        // create test files
        File dir = Files.createTempDirectory("testParallelismSettingWithFileNum").toFile();
        dir.deleteOnExit();
        for (int i = 0; i < 3; i++) {
            File csv = new File(dir, "data" + i + ".csv");
            csv.createNewFile();
            FileUtils.writeFileUtf8(csv, "1|100\n2|200\n");
        }

        TableEnvironment tEnv = createTableEnv();
        tEnv.executeSql(
                "CREATE EXTERNAL TABLE source_db.test_parallelism_setting_with_file_num "
                        + "(a INT, b INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '"
                        + dir.toString()
                        + "'");

        Table table =
                tEnv.sqlQuery(
                        "select * from hive.source_db.test_parallelism_setting_with_file_num");
        testParallelismSettingTranslateAndAssert(3, table, tEnv);

        tEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX, 2);
        testParallelismSettingTranslateAndAssert(2, table, tEnv);
    }

    private void testParallelismSettingTranslateAndAssert(
            int expected, Table table, TableEnvironment tEnv) {
        PlannerBase planner = (PlannerBase) ((TableEnvironmentImpl) tEnv).getPlanner();
        RelNode relNode = planner.optimize(TableTestUtil.toRelNode(table));
        ExecNode<?> execNode =
                planner.translateToExecNodeGraph(toScala(Collections.singletonList(relNode)), false)
                        .getRootNodes()
                        .get(0);
        Transformation<?> transformation = execNode.translateToPlan(planner);
        assertThat(transformation.getParallelism()).isEqualTo(expected);
    }

    @Test
    public void testParallelismOnLimitPushDown() throws Exception {
        final String dbName = "source_db";
        final String tblName = "test_parallelism_limit_pushdown";
        TableEnvironment tEnv = createTableEnv();
        tEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM, false);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        tEnv.executeSql(
                "CREATE TABLE source_db.test_parallelism_limit_pushdown "
                        + "(`year` STRING, `value` INT) partitioned by (pt int)");
        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {"2014", 3})
                .addRow(new Object[] {"2014", 4})
                .commit("pt=0");
        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {"2015", 2})
                .addRow(new Object[] {"2015", 5})
                .commit("pt=1");
        Table table =
                tEnv.sqlQuery(
                        "select * from hive.source_db.test_parallelism_limit_pushdown limit 1");
        PlannerBase planner = (PlannerBase) ((TableEnvironmentImpl) tEnv).getPlanner();
        RelNode relNode = planner.optimize(TableTestUtil.toRelNode(table));
        ExecNode<?> execNode =
                planner.translateToExecNodeGraph(toScala(Collections.singletonList(relNode)), false)
                        .getRootNodes()
                        .get(0);
        Transformation<?> transformation =
                (execNode.translateToPlan(planner).getInputs().get(0)).getInputs().get(0);
        // when there's no infer, should use the default parallelism configured
        assertThat(transformation.getParallelism()).isEqualTo(2);
    }

    @Test
    public void testParallelismWithoutParallelismInfer() throws Exception {
        final String dbName = "source_db";
        final String tblName = "test_parallelism_no_infer";
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.registerCatalog("hive", hiveCatalog);
        tEnv.useCatalog("hive");
        tEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM, false);
        tEnv.executeSql(
                "CREATE TABLE source_db.test_parallelism_no_infer "
                        + "(`year` STRING, `value` INT) partitioned by (pt int)");
        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {"2014", 3})
                .addRow(new Object[] {"2014", 4})
                .commit("pt=0");
        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {"2015", 2})
                .addRow(new Object[] {"2015", 5})
                .commit("pt=1");
        Table table =
                tEnv.sqlQuery("select * from hive.source_db.test_parallelism_no_infer limit 1");
        PlannerBase planner = (PlannerBase) ((TableEnvironmentImpl) tEnv).getPlanner();
        RelNode relNode = planner.optimize(TableTestUtil.toRelNode(table));
        ExecNode<?> execNode =
                planner.translateToExecNodeGraph(toScala(Collections.singletonList(relNode)), false)
                        .getRootNodes()
                        .get(0);
        Transformation<?> transformation =
                (execNode.translateToPlan(planner).getInputs().get(0)).getInputs().get(0);
        // when there's no infer, should use the default parallelism
        assertThat(transformation.getParallelism())
                .isEqualTo(
                        ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM
                                .defaultValue()
                                .intValue());
    }

    @Test
    public void testSourceConfig() throws Exception {
        // vector reader not available for 1.x and we're not testing orc for 2.0.x
        Assume.assumeTrue(HiveVersionTestUtil.HIVE_230_OR_LATER);
        Map<String, String> env = System.getenv();
        batchTableEnv.executeSql("create database db1");
        try {
            batchTableEnv.executeSql("create table db1.src (x int,y string) stored as orc");
            batchTableEnv.executeSql("insert into db1.src values (1,'a'),(2,'b')").await();
            testSourceConfig(true, true);
            testSourceConfig(false, false);
        } finally {
            CommonTestUtils.setEnv(env);
            batchTableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test(timeout = 120000)
    public void testStreamPartitionReadByPartitionName() throws Exception {
        final String catalogName = "hive";
        final String dbName = "source_db";
        final String tblName = "stream_partition_name_test";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        StreamTableEnvironment tEnv =
                HiveTestUtils.createTableEnvInStreamingMode(env, SqlDialect.HIVE);
        tEnv.registerCatalog(catalogName, hiveCatalog);
        tEnv.useCatalog(catalogName);
        tEnv.executeSql(
                "CREATE TABLE source_db.stream_partition_name_test (x int, y string, z int)"
                        + " PARTITIONED BY ("
                        + " pt_year int, pt_mon string, pt_day string) TBLPROPERTIES("
                        + "'streaming-source.enable'='true',"
                        + "'streaming-source.monitor-interval'='1s',"
                        + "'streaming-source.consume-start-offset'='pt_year=2019/pt_month=09/pt_day=02'"
                        + ")");

        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {0, "a", 11})
                .commit("pt_year='2019',pt_mon='09',pt_day='01'");
        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {1, "b", 12})
                .commit("pt_year='2020',pt_mon='09',pt_day='03'");

        TableResult result =
                tEnv.executeSql("select * from hive.source_db.stream_partition_name_test");
        CloseableIterator<Row> iter = result.collect();

        assertThat(fetchRows(iter, 1).get(0))
                .isEqualTo(Row.of(1, "b", "12", "2020", "09", "03").toString());

        for (int i = 2; i < 6; i++) {
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                    .addRow(new Object[] {i, "new_add", 11 + i})
                    .addRow(new Object[] {i, "new_add_1", 11 + i})
                    .commit("pt_year='2020',pt_mon='10',pt_day='0" + i + "'");

            assertThat(fetchRows(iter, 2))
                    .isEqualTo(
                            Arrays.asList(
                                    Row.of(i, "new_add", 11 + i, "2020", "10", "0" + i).toString(),
                                    Row.of(i, "new_add_1", 11 + i, "2020", "10", "0" + i)
                                            .toString()));
        }

        result.getJobClient().get().cancel();
    }

    @Test(timeout = 120000)
    public void testStreamPartitionReadByCreateTime() throws Exception {
        final String catalogName = "hive";
        final String dbName = "source_db";
        final String tblName = "stream_create_time_test";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        StreamTableEnvironment tEnv =
                HiveTestUtils.createTableEnvInStreamingMode(env, SqlDialect.HIVE);
        tEnv.registerCatalog(catalogName, hiveCatalog);
        tEnv.useCatalog(catalogName);
        tEnv.executeSql(
                "CREATE TABLE source_db.stream_create_time_test (x int, y string, z int)"
                        + " PARTITIONED BY ("
                        + " p1 string, p2 string, p3 string) TBLPROPERTIES("
                        + "'streaming-source.enable'='true',"
                        + "'streaming-source.partition-include'='all',"
                        + "'streaming-source.consume-order'='create-time',"
                        + "'streaming-source.monitor-interval'='1s',"
                        + "'streaming-source.consume-start-offset'='2020-10-02 00:00:00'"
                        + ")");

        // the create-time is near current timestamp and bigger than '2020-10-02 00:00:00' since the
        // code wrote
        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {0, "a", 11})
                .commit("p1='A1',p2='B1',p3='C1'");

        TableResult result =
                tEnv.executeSql("select * from hive.source_db.stream_create_time_test");
        CloseableIterator<Row> iter = result.collect();

        assertThat(fetchRows(iter, 1).get(0))
                .isEqualTo(Row.of(0, "a", "11", "A1", "B1", "C1").toString());

        for (int i = 1; i < 6; i++) {
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                    .addRow(new Object[] {i, "new_add", 11 + i})
                    .addRow(new Object[] {i, "new_add_1", 11 + i})
                    .commit("p1='A',p2='B',p3='" + i + "'");

            assertThat(fetchRows(iter, 2))
                    .isEqualTo(
                            Arrays.asList(
                                    Row.of(i, "new_add", 11 + i, "A", "B", i).toString(),
                                    Row.of(i, "new_add_1", 11 + i, "A", "B", i).toString()));
        }
        result.getJobClient().get().cancel();
    }

    @Test(timeout = 120000)
    public void testStreamPartitionReadByPartitionTime() throws Exception {
        final String catalogName = "hive";
        final String dbName = "source_db";
        final String tblName = "stream_test";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        StreamTableEnvironment tEnv =
                HiveTestUtils.createTableEnvInStreamingMode(env, SqlDialect.HIVE);
        tEnv.registerCatalog(catalogName, hiveCatalog);
        tEnv.useCatalog(catalogName);
        tEnv.executeSql(
                "CREATE TABLE source_db.stream_test ("
                        + " a INT,"
                        + " b STRING"
                        + ") PARTITIONED BY (ts STRING) TBLPROPERTIES ("
                        + "'streaming-source.enable'='true',"
                        + "'streaming-source.monitor-interval'='1s',"
                        + "'streaming-source.consume-order'='partition-time'"
                        + ")");

        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {0, "0"})
                .commit("ts='2020-05-06 00:00:00'");

        TableResult result = tEnv.executeSql("select * from hive.source_db.stream_test");
        CloseableIterator<Row> iter = result.collect();

        assertThat(fetchRows(iter, 1).get(0))
                .isEqualTo(Row.of(0, "0", "2020-05-06 00:00:00").toString());

        for (int i = 1; i < 6; i++) {
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                    .addRow(new Object[] {i, String.valueOf(i)})
                    .addRow(new Object[] {i, i + "_copy"})
                    .commit("ts='2020-05-06 00:" + i + "0:00'");

            assertThat(fetchRows(iter, 2))
                    .isEqualTo(
                            Arrays.asList(
                                    Row.of(i, String.valueOf(i), "2020-05-06 00:" + i + "0:00")
                                            .toString(),
                                    Row.of(i, i + "_copy", "2020-05-06 00:" + i + "0:00")
                                            .toString()));
        }

        result.getJobClient().get().cancel();
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> strings = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            assertThat(iter.hasNext()).isTrue();
            strings.add(iter.next().toString());
        }
        strings.sort(String::compareTo);
        return strings;
    }

    @Test(timeout = 30000)
    public void testNonPartitionStreamingSourceWithMapredReader() throws Exception {
        testNonPartitionStreamingSource(true, "test_mapred_reader");
    }

    @Test(timeout = 30000)
    public void testNonPartitionStreamingSourceWithVectorizedReader() throws Exception {
        testNonPartitionStreamingSource(false, "test_vectorized_reader");
    }

    private void testNonPartitionStreamingSource(Boolean useMapredReader, String tblName)
            throws Exception {
        final String catalogName = "hive";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv =
                HiveTestUtils.createTableEnvInStreamingMode(env, SqlDialect.HIVE);
        tEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER, useMapredReader);
        tEnv.registerCatalog(catalogName, hiveCatalog);
        tEnv.useCatalog(catalogName);
        tEnv.executeSql(
                "CREATE TABLE source_db."
                        + tblName
                        + " ("
                        + "  a INT,"
                        + "  b CHAR(1) "
                        + ") stored as parquet TBLPROPERTIES ("
                        + "  'streaming-source.enable'='true',"
                        + "  'streaming-source.partition-order'='create-time',"
                        + "  'streaming-source.monitor-interval'='100ms'"
                        + ")");

        TableResult result = tEnv.executeSql("select * from hive.source_db." + tblName);
        CloseableIterator<Row> iter = result.collect();

        for (int i = 1; i < 3; i++) {
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            batchTableEnv
                    .executeSql(
                            "insert into table source_db." + tblName + " values (1,'a'), (2,'b')")
                    .await();
            assertThat(fetchRows(iter, 2))
                    .isEqualTo(Arrays.asList(Row.of(1, "a").toString(), Row.of(2, "b").toString()));
        }

        result.getJobClient().get().cancel();
    }

    private void testSourceConfig(boolean fallbackMR, boolean inferParallelism) throws Exception {
        HiveDynamicTableFactory tableFactorySpy =
                spy((HiveDynamicTableFactory) hiveCatalog.getFactory().get());

        doAnswer(
                        invocation -> {
                            DynamicTableFactory.Context context = invocation.getArgument(0);
                            assertThat(
                                            context.getConfiguration()
                                                    .get(
                                                            HiveOptions
                                                                    .TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER))
                                    .isEqualTo(fallbackMR);
                            return new TestConfigSource(
                                    new JobConf(hiveCatalog.getHiveConf()),
                                    context.getConfiguration(),
                                    context.getObjectIdentifier().toObjectPath(),
                                    context.getCatalogTable(),
                                    inferParallelism);
                        })
                .when(tableFactorySpy)
                .createDynamicTableSource(any(DynamicTableFactory.Context.class));

        HiveCatalog catalogSpy = spy(hiveCatalog);
        doReturn(Optional.of(tableFactorySpy)).when(catalogSpy).getTableFactory();

        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode();
        tableEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER, fallbackMR);
        tableEnv.getConfig()
                .set(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM, inferParallelism);
        tableEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        tableEnv.registerCatalog(catalogSpy.getName(), catalogSpy);
        tableEnv.useCatalog(catalogSpy.getName());

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select * from db1.src order by x").execute().collect());
        assertThat(results.toString()).isEqualTo("[+I[1, a], +I[2, b]]");
    }

    @Test
    public void testParquetCaseInsensitive() throws Exception {
        testCaseInsensitive("parquet");
    }

    private void testCaseInsensitive(String format) throws Exception {
        TableEnvironment tEnv = createTableEnvWithHiveCatalog(hiveCatalog);
        String folderURI = createTempFolder().toURI().toString();

        // Flink to write sensitive fields to parquet file
        tEnv.executeSql(
                String.format(
                        "create table parquet_t (I int, J int) with ("
                                + "'connector'='filesystem','format'='%s','path'='%s')",
                        format, folderURI));
        tEnv.executeSql("insert into parquet_t select 1, 2").await();
        tEnv.executeSql("drop table parquet_t");

        // Hive to read parquet file
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql(
                String.format(
                        "create external table parquet_t (i int, j int) stored as %s location '%s'",
                        format, folderURI));
        assertThat(tEnv.executeSql("select * from parquet_t").collect().next())
                .isEqualTo(Row.of(1, 2));
    }

    @Test(timeout = 120000)
    public void testStreamReadWithProjectPushDown() throws Exception {
        final String catalogName = "hive";
        final String dbName = "source_db";
        final String tblName = "stream_project_pushdown_test";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        StreamTableEnvironment tEnv =
                HiveTestUtils.createTableEnvInStreamingMode(env, SqlDialect.HIVE);
        tEnv.registerCatalog(catalogName, hiveCatalog);
        tEnv.useCatalog(catalogName);
        tEnv.executeSql(
                "CREATE TABLE source_db.stream_project_pushdown_test (x int, y string, z int)"
                        + " PARTITIONED BY ("
                        + " pt_year int, pt_mon string, pt_day string) TBLPROPERTIES("
                        + "'streaming-source.enable'='true',"
                        + "'streaming-source.monitor-interval'='1s',"
                        + "'streaming-source.consume-start-offset'='pt_year=2019/pt_month=09/pt_day=02'"
                        + ")");

        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {0, "a", 11})
                .commit("pt_year='2019',pt_mon='09',pt_day='01'");
        HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                .addRow(new Object[] {1, "b", 12})
                .commit("pt_year='2020',pt_mon='09',pt_day='03'");

        TableResult result =
                tEnv.executeSql(
                        "select x, y from hive.source_db.stream_project_pushdown_test where pt_year = '2020'");
        CloseableIterator<Row> iter = result.collect();

        assertThat(fetchRows(iter, 1).get(0)).isEqualTo(Row.of(1, "b").toString());

        for (int i = 2; i < 6; i++) {
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            HiveTestUtils.createTextTableInserter(hiveCatalog, dbName, tblName)
                    .addRow(new Object[] {i, "new_add", 11 + i})
                    .addRow(new Object[] {i, "new_add_1", 11 + i})
                    .commit("pt_year='2020',pt_mon='10',pt_day='0" + i + "'");

            assertThat(fetchRows(iter, 2))
                    .isEqualTo(
                            Arrays.asList(
                                    Row.of(i, "new_add").toString(),
                                    Row.of(i, "new_add_1").toString()));
        }

        result.getJobClient().get().cancel();
    }

    @Test(timeout = 120000)
    public void testReadParquetWithNullableComplexType() throws Exception {
        final String catalogName = "hive";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(100);
        StreamTableEnvironment tEnv =
                HiveTestUtils.createTableEnvInStreamingMode(env, SqlDialect.HIVE);
        tEnv.registerCatalog(catalogName, hiveCatalog);
        tEnv.useCatalog(catalogName);

        List<Row> rows = generateRows();
        List<Row> expectedRows = generateExpectedRows(rows);
        DataStream<Row> stream =
                env.addSource(
                                new FiniteTestSource<>(rows),
                                new RowTypeInfo(
                                        new TypeInformation[] {
                                            Types.INT,
                                            Types.STRING,
                                            new RowTypeInfo(
                                                    new TypeInformation[] {
                                                        Types.STRING, Types.INT, Types.INT
                                                    },
                                                    new String[] {"c1", "c2", "c3"}),
                                            new MapTypeInfo<>(Types.STRING, Types.STRING),
                                            Types.OBJECT_ARRAY(Types.STRING),
                                            Types.STRING
                                        },
                                        new String[] {"a", "b", "c", "d", "e", "f"}))
                        .filter((FilterFunction<Row>) value -> true)
                        .setParallelism(3); // to parallel tasks

        tEnv.createTemporaryView("my_table", stream);
        assertResults(executeAndGetResult(tEnv), expectedRows);
    }

    private static List<Row> generateRows() {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            Map<String, String> e = new HashMap<>();
            e.put(i + "", i % 2 == 0 ? null : i + "");
            String[] f = new String[2];
            f[0] = i % 3 == 0 ? null : i + "";
            f[1] = i % 3 == 2 ? null : i + "";
            rows.add(
                    Row.of(
                            i,
                            String.valueOf(i % 10),
                            Row.of(
                                    i % 2 == 0 ? null : String.valueOf(i % 10),
                                    i % 3 == 0 ? null : i % 10,
                                    i % 5 == 0 ? null : i % 10),
                            e,
                            f,
                            String.valueOf(i % 10)));
        }
        return rows;
    }

    private static List<Row> generateExpectedRows(List<Row> rows) {
        List<Row> sortedRows = new ArrayList<>();
        sortedRows.addAll(rows);
        sortedRows.addAll(rows);
        sortedRows.sort(Comparator.comparingInt(o -> (Integer) o.getField(0)));

        List<Row> expectedRows = new ArrayList<>();
        for (int i = 0; i < sortedRows.size(); i++) {
            Row rowExpect = Row.copy(sortedRows.get(i));
            Row nestedRow = (Row) rowExpect.getField(2);
            if (nestedRow.getField(0) == null
                    && nestedRow.getField(1) == null
                    && nestedRow.getField(2) == null) {
                rowExpect.setField(2, null);
            }
            expectedRows.add(rowExpect);
        }
        return expectedRows;
    }

    private static CloseableIterator<Row> executeAndGetResult(StreamTableEnvironment tEnv)
            throws Exception {
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.executeSql(
                "CREATE TABLE sink_table (a int, b string,"
                        + "c struct<c1:string, c2:int, c3:int>,"
                        + "d map<string, string>, e array<string>, f string "
                        + ") "
                        + " stored as parquet"
                        + " TBLPROPERTIES ("
                        + "'sink.partition-commit.policy.kind'='metastore,success-file',"
                        + "'auto-compaction'='true',"
                        + "'compaction.file-size' = '128MB',"
                        + "'sink.rolling-policy.file-size' = '1b'"
                        + ")");
        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        String sql =
                "insert into sink_table /*+ OPTIONS('sink.parallelism' = '3') */"
                        + " select * from my_table";
        tEnv.executeSql(sql).await();
        return tEnv.executeSql("select * from sink_table").collect();
    }

    private static void assertResults(CloseableIterator<Row> iterator, List<Row> expectedRows)
            throws Exception {
        List<Row> result = CollectionUtil.iteratorToList(iterator);
        iterator.close();
        result.sort(Comparator.comparingInt(o -> (Integer) o.getField(0)));
        assertThat(result).isEqualTo(expectedRows);
    }

    private static TableEnvironment createTableEnv() {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        tableEnv.registerCatalog("hive", hiveCatalog);
        tableEnv.useCatalog("hive");
        return tableEnv;
    }

    /** A sub-class of HiveTableSource to test vector reader switch. */
    private static class TestConfigSource extends HiveTableSource {
        private final boolean inferParallelism;

        TestConfigSource(
                JobConf jobConf,
                ReadableConfig flinkConf,
                ObjectPath tablePath,
                ResolvedCatalogTable catalogTable,
                boolean inferParallelism) {
            super(jobConf, flinkConf, tablePath, catalogTable);
            this.inferParallelism = inferParallelism;
        }

        @Override
        public DataStream<RowData> getDataStream(
                ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
            DataStreamSource<RowData> dataStream =
                    (DataStreamSource<RowData>) super.getDataStream(providerContext, execEnv);
            int parallelism = dataStream.getTransformation().getParallelism();
            assertThat(parallelism).isEqualTo(inferParallelism ? 1 : 2);
            return dataStream;
        }
    }

    // A sub-class of HiveCatalog to test list partitions by filter.
    private static class TestPartitionFilterCatalog extends HiveCatalog {

        private boolean fallback = false;

        TestPartitionFilterCatalog(
                String catalogName,
                String defaultDatabase,
                @Nullable HiveConf hiveConf,
                String hiveVersion) {
            super(catalogName, defaultDatabase, hiveConf, hiveVersion, true);
        }

        @Override
        public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
                throws TableNotExistException, TableNotPartitionedException, CatalogException {
            fallback = true;
            return super.listPartitions(tablePath);
        }
    }
}
