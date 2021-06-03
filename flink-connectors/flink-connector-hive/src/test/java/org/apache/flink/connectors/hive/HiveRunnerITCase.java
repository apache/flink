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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.ArrayUtils;
import org.apache.flink.util.CollectionUtil;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_IN_TEST;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_TXN_MANAGER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests that need to run with hive runner. Since hive runner is heavy, make sure to add test cases
 * here only if a test requires hive-side functionalities, e.g. preparing or validating data in
 * hive.
 */
@RunWith(FlinkEmbeddedHiveRunner.class)
public class HiveRunnerITCase {

    @HiveSQL(files = {})
    private static HiveShell hiveShell;

    @HiveRunnerSetup
    private static final HiveRunnerConfig CONFIG =
            new HiveRunnerConfig() {
                {
                    if (HiveShimLoader.getHiveVersion().startsWith("3.")) {
                        // hive-3.x requires a proper txn manager to create ACID table
                        getHiveConfSystemOverride()
                                .put(HIVE_TXN_MANAGER.varname, DbTxnManager.class.getName());
                        getHiveConfSystemOverride().put(HIVE_SUPPORT_CONCURRENCY.varname, "true");
                        // tell TxnHandler to prepare txn DB
                        getHiveConfSystemOverride().put(HIVE_IN_TEST.varname, "true");
                    }
                }
            };

    private static HiveCatalog hiveCatalog;

    @BeforeClass
    public static void createCatalog() throws IOException {
        HiveConf hiveConf = hiveShell.getHiveConf();
        hiveCatalog = HiveTestUtils.createHiveCatalog(hiveConf);
        hiveCatalog.open();
    }

    @AfterClass
    public static void closeCatalog() {
        if (hiveCatalog != null) {
            hiveCatalog.close();
        }
    }

    @Test
    public void testInsertIntoNonPartitionTable() throws Exception {
        List<Row> toWrite = generateRecords(5);
        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(toWrite);
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithHiveCatalog(hiveCatalog);
        tableEnv.executeSql(
                "create table default_catalog.default_database.src (i int,l bigint,d double,s string) "
                        + "with ('connector'='COLLECTION','is-bounded' = 'true')");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("create table dest (i int,l bigint,d double,s string)");
        try {
            tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
            tableEnv.executeSql(
                            "insert into dest select * from default_catalog.default_database.src")
                    .await();
            verifyWrittenData(toWrite, hiveShell.executeQuery("select * from dest"));
        } finally {
            tableEnv.executeSql("drop table dest");
        }
    }

    @Test
    public void testWriteComplexType() throws Exception {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithHiveCatalog(hiveCatalog);

        Row row = new Row(3);
        Object[] array = new Object[] {1, 2, 3};
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "a");
        map.put(2, "b");
        Row struct = new Row(2);
        struct.setField(0, 3);
        struct.setField(1, "c");
        row.setField(0, array);
        row.setField(1, map);
        row.setField(2, struct);
        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(Collections.singletonList(row));
        tableEnv.executeSql(
                "create table default_catalog.default_database.complexSrc (a array<int>,m map<int, string>,s row<f1 int,f2 string>) "
                        + "with ('connector'='COLLECTION','is-bounded' = 'true')");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql(
                "create table dest (a array<int>,m map<int, string>,s struct<f1:int,f2:string>)");

        try {
            tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
            tableEnv.executeSql(
                            "insert into dest select * from default_catalog.default_database.complexSrc")
                    .await();
            List<String> result = hiveShell.executeQuery("select * from dest");
            assertEquals(1, result.size());
            assertEquals("[1,2,3]\t{1:\"a\",2:\"b\"}\t{\"f1\":3,\"f2\":\"c\"}", result.get(0));
        } finally {
            tableEnv.executeSql("drop table dest");
        }
    }

    @Test
    public void testWriteNestedComplexType() throws Exception {

        TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithHiveCatalog(hiveCatalog);

        Row row = new Row(1);
        Object[] array = new Object[3];
        row.setField(0, array);
        for (int i = 0; i < array.length; i++) {
            Row struct = new Row(2);
            struct.setField(0, 1 + i);
            struct.setField(1, String.valueOf((char) ('a' + i)));
            array[i] = struct;
        }
        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(Collections.singletonList(row));
        tableEnv.executeSql(
                "create table default_catalog.default_database.nestedSrc (a array<row<f1 int,f2 string>>) "
                        + "with ('connector'='COLLECTION','is-bounded' = 'true')");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("create table dest (a array<struct<f1:int,f2:string>>)");

        try {
            tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
            tableEnv.executeSql(
                            "insert into dest select * from default_catalog.default_database.nestedSrc")
                    .await();
            List<String> result = hiveShell.executeQuery("select * from dest");
            assertEquals(1, result.size());
            assertEquals(
                    "[{\"f1\":1,\"f2\":\"a\"},{\"f1\":2,\"f2\":\"b\"},{\"f1\":3,\"f2\":\"c\"}]",
                    result.get(0));
        } finally {
            tableEnv.executeSql("drop table dest");
        }
    }

    @Test
    public void testWriteNullValues() throws Exception {
        TableEnvironment tableEnv =
                HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(SqlDialect.HIVE);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
        tableEnv.executeSql("create database db1");
        try {
            // 17 data types
            tableEnv.executeSql(
                    "create table db1.src"
                            + "(t tinyint,s smallint,i int,b bigint,f float,d double,de decimal(10,5),ts timestamp,dt date,"
                            + "str string,ch char(5),vch varchar(8),bl boolean,bin binary,arr array<int>,mp map<int,string>,strt struct<f1:int,f2:string>)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "src")
                    .addRow(
                            new Object[] {
                                null, null, null, null, null, null, null, null, null, null, null,
                                null, null, null, null, null, null
                            })
                    .commit();
            hiveShell.execute("create table db1.dest like db1.src");

            tableEnv.executeSql("insert into db1.dest select * from db1.src").await();
            List<String> results = hiveShell.executeQuery("select * from db1.dest");
            assertEquals(1, results.size());
            String[] cols = results.get(0).split("\t");
            assertEquals(17, cols.length);
            assertEquals("NULL", cols[0]);
            assertEquals(1, new HashSet<>(Arrays.asList(cols)).size());
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testDifferentFormats() throws Exception {
        String[] formats = new String[] {"orc", "parquet", "sequencefile", "csv", "avro"};
        for (String format : formats) {
            if (format.equals("avro") && !HiveVersionTestUtil.HIVE_110_OR_LATER) {
                // timestamp is not supported for avro tables before 1.1.0
                continue;
            }
            readWriteFormat(format);
        }
    }

    @Test
    public void testDecimal() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.src1 (x decimal(10,2))");
            tableEnv.executeSql("create table db1.src2 (x decimal(10,2))");
            tableEnv.executeSql("create table db1.dest (x decimal(10,2))");
            // populate src1 from Hive
            // TABLE keyword in INSERT INTO is mandatory prior to 1.1.0
            hiveShell.execute(
                    "insert into table db1.src1 values (1.0),(2.12),(5.123),(5.456),(123456789.12)");

            // populate src2 with same data from Flink
            tableEnv.executeSql(
                            "insert into db1.src2 values (1.0),(2.12),(5.123),(5.456),(123456789.12)")
                    .await();
            // verify src1 and src2 contain same data
            verifyHiveQueryResult(
                    "select * from db1.src2", hiveShell.executeQuery("select * from db1.src1"));

            // populate dest with src1 from Flink -- to test reading decimal type from Hive
            tableEnv.executeSql("insert into db1.dest select * from db1.src1").await();
            verifyHiveQueryResult(
                    "select * from db1.dest", hiveShell.executeQuery("select * from db1.src1"));
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testInsertOverwrite() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            // non-partitioned
            tableEnv.executeSql("create table db1.dest (x int, y string)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "dest")
                    .addRow(new Object[] {1, "a"})
                    .addRow(new Object[] {2, "b"})
                    .commit();
            verifyHiveQueryResult("select * from db1.dest", Arrays.asList("1\ta", "2\tb"));

            tableEnv.executeSql("insert overwrite db1.dest values (3, 'c')").await();
            verifyHiveQueryResult("select * from db1.dest", Collections.singletonList("3\tc"));

            // static partition
            tableEnv.executeSql("create table db1.part(x int) partitioned by (y int)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "part")
                    .addRow(new Object[] {1})
                    .commit("y=1");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "part")
                    .addRow(new Object[] {2})
                    .commit("y=2");
            tableEnv = getTableEnvWithHiveCatalog();
            tableEnv.executeSql("insert overwrite db1.part partition (y=1) select 100").await();
            verifyHiveQueryResult("select * from db1.part", Arrays.asList("100\t1", "2\t2"));

            // dynamic partition
            tableEnv = getTableEnvWithHiveCatalog();
            tableEnv.executeSql("insert overwrite db1.part values (200,2),(3,3)").await();
            // only overwrite dynamically matched partitions, other existing partitions remain
            // intact
            verifyHiveQueryResult(
                    "select * from db1.part", Arrays.asList("100\t1", "200\t2", "3\t3"));
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testStaticPartition() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.src (x int)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "src")
                    .addRow(new Object[] {1})
                    .addRow(new Object[] {2})
                    .commit();
            tableEnv.executeSql(
                    "create table db1.dest (x int) partitioned by (p1 string, p2 double)");
            tableEnv.executeSql(
                            "insert into db1.dest partition (p1='1\\'1', p2=1.1) select x from db1.src")
                    .await();
            assertEquals(1, hiveCatalog.listPartitions(new ObjectPath("db1", "dest")).size());
            verifyHiveQueryResult(
                    "select * from db1.dest", Arrays.asList("1\t1'1\t1.1", "2\t1'1\t1.1"));
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testDynamicPartition() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.src (x int, y string, z double)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "src")
                    .addRow(new Object[] {1, "a", 1.1})
                    .addRow(new Object[] {2, "a", 2.2})
                    .addRow(new Object[] {3, "b", 3.3})
                    .commit();
            tableEnv.executeSql(
                    "create table db1.dest (x int) partitioned by (p1 string, p2 double)");
            tableEnv.executeSql("insert into db1.dest select * from db1.src").await();
            assertEquals(3, hiveCatalog.listPartitions(new ObjectPath("db1", "dest")).size());
            verifyHiveQueryResult(
                    "select * from db1.dest", Arrays.asList("1\ta\t1.1", "2\ta\t2.2", "3\tb\t3.3"));
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testPartialDynamicPartition() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.src (x int, y string)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "src")
                    .addRow(new Object[] {1, "a"})
                    .addRow(new Object[] {2, "b"})
                    .commit();
            tableEnv.executeSql(
                    "create table db1.dest (x int) partitioned by (p1 double, p2 string)");
            tableEnv.executeSql(
                            "insert into db1.dest partition (p1=1.1,p2) select x,y from db1.src")
                    .await();
            assertEquals(2, hiveCatalog.listPartitions(new ObjectPath("db1", "dest")).size());
            verifyHiveQueryResult(
                    "select * from db1.dest", Arrays.asList("1\t1.1\ta", "2\t1.1\tb"));
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testBatchCompressTextTable() throws Exception {
        testCompressTextTable(true);
    }

    @Test
    public void testStreamCompressTextTable() throws Exception {
        testCompressTextTable(false);
    }

    @Test
    public void testTimestamp() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.src (ts timestamp)");
            tableEnv.executeSql("create table db1.dest (ts timestamp)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "src")
                    .addRow(new Object[] {Timestamp.valueOf("2019-11-11 00:00:00")})
                    .addRow(new Object[] {Timestamp.valueOf("2019-12-03 15:43:32.123456789")})
                    .commit();
            // test read timestamp from hive
            List<Row> results =
                    CollectionUtil.iteratorToList(
                            tableEnv.sqlQuery("select * from db1.src").execute().collect());
            assertEquals(2, results.size());
            assertEquals(LocalDateTime.of(2019, 11, 11, 0, 0), results.get(0).getField(0));
            assertEquals(
                    LocalDateTime.of(2019, 12, 3, 15, 43, 32, 123456789),
                    results.get(1).getField(0));
            // test write timestamp to hive
            tableEnv.executeSql("insert into db1.dest select max(ts) from db1.src").await();
            verifyHiveQueryResult(
                    "select * from db1.dest",
                    Collections.singletonList("2019-12-03 15:43:32.123456789"));
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testDate() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.src (dt date)");
            tableEnv.executeSql("create table db1.dest (dt date)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "src")
                    .addRow(new Object[] {Date.valueOf("2019-12-09")})
                    .addRow(new Object[] {Date.valueOf("2019-12-12")})
                    .commit();
            // test read date from hive
            List<Row> results =
                    CollectionUtil.iteratorToList(
                            tableEnv.sqlQuery("select * from db1.src").execute().collect());
            assertEquals(2, results.size());
            assertEquals(LocalDate.of(2019, 12, 9), results.get(0).getField(0));
            assertEquals(LocalDate.of(2019, 12, 12), results.get(1).getField(0));
            // test write date to hive
            tableEnv.executeSql("insert into db1.dest select max(dt) from db1.src").await();
            verifyHiveQueryResult(
                    "select * from db1.dest", Collections.singletonList("2019-12-12"));
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testViews() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.src (key int,val string)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "src")
                    .addRow(new Object[] {1, "a"})
                    .addRow(new Object[] {1, "aa"})
                    .addRow(new Object[] {1, "aaa"})
                    .addRow(new Object[] {2, "b"})
                    .addRow(new Object[] {3, "c"})
                    .addRow(new Object[] {3, "ccc"})
                    .commit();
            tableEnv.executeSql("create table db1.keys (key int,name string)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "keys")
                    .addRow(new Object[] {1, "key1"})
                    .addRow(new Object[] {2, "key2"})
                    .addRow(new Object[] {3, "key3"})
                    .addRow(new Object[] {4, "key4"})
                    .commit();
            hiveShell.execute(
                    "create view db1.v1 as select key as k,val as v from db1.src limit 2");
            hiveShell.execute(
                    "create view db1.v2 as select key,count(*) from db1.src group by key having count(*)>1 order by key");
            hiveShell.execute(
                    "create view db1.v3 as select k.key,k.name,count(*) from db1.src s join db1.keys k on s.key=k.key group by k.key,k.name order by k.key");
            List<Row> results =
                    CollectionUtil.iteratorToList(
                            tableEnv.sqlQuery("select count(v) from db1.v1").execute().collect());
            assertEquals("[+I[2]]", results.toString());
            results =
                    CollectionUtil.iteratorToList(
                            tableEnv.sqlQuery("select * from db1.v2").execute().collect());
            assertEquals("[+I[1, 3], +I[3, 2]]", results.toString());
            results =
                    CollectionUtil.iteratorToList(
                            tableEnv.sqlQuery("select * from db1.v3").execute().collect());
            assertEquals("[+I[1, key1, 3], +I[2, key2, 1], +I[3, key3, 2]]", results.toString());
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testWhitespacePartValue() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.dest (x int) partitioned by (p string)");
            StatementSet stmtSet = tableEnv.createStatementSet();
            stmtSet.addInsertSql("insert into db1.dest select 1,'  '");
            stmtSet.addInsertSql("insert into db1.dest select 2,'a \t'");
            stmtSet.execute().await();
            assertEquals(
                    "[p=  , p=a %09]",
                    hiveShell.executeQuery("show partitions db1.dest").toString());
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testBatchTransactionalTable() {
        testTransactionalTable(true);
    }

    @Test
    public void testStreamTransactionalTable() {
        testTransactionalTable(false);
    }

    @Test
    public void testOrcSchemaEvol() throws Exception {
        // not supported until 2.1.0 -- https://issues.apache.org/jira/browse/HIVE-11981,
        // https://issues.apache.org/jira/browse/HIVE-13178
        Assume.assumeTrue(HiveVersionTestUtil.HIVE_210_OR_LATER);
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.src (x smallint,y int) stored as orc");
            hiveShell.execute("insert into table db1.src values (1,100),(2,200)");

            tableEnv.getConfig()
                    .getConfiguration()
                    .setBoolean(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER, true);

            tableEnv.executeSql("alter table db1.src change x x int");
            assertEquals(
                    "[+I[1, 100], +I[2, 200]]",
                    CollectionUtil.iteratorToList(
                                    tableEnv.sqlQuery("select * from db1.src").execute().collect())
                            .toString());

            tableEnv.executeSql("alter table db1.src change y y string");
            assertEquals(
                    "[+I[1, 100], +I[2, 200]]",
                    CollectionUtil.iteratorToList(
                                    tableEnv.sqlQuery("select * from db1.src").execute().collect())
                            .toString());
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    private void testTransactionalTable(boolean batch) {
        TableEnvironment tableEnv =
                batch ? getTableEnvWithHiveCatalog() : getStreamTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.src (x string,y string)");
            hiveShell.execute(
                    "create table db1.dest (x string,y string) clustered by (x) into 3 buckets stored as orc tblproperties ('transactional'='true')");
            List<Exception> exceptions = new ArrayList<>();
            try {
                tableEnv.executeSql("insert into db1.src select * from db1.dest").await();
            } catch (Exception e) {
                exceptions.add(e);
            }
            try {
                tableEnv.executeSql("insert into db1.dest select * from db1.src").await();
            } catch (Exception e) {
                exceptions.add(e);
            }
            assertEquals(2, exceptions.size());
            exceptions.forEach(
                    e -> {
                        assertTrue(e instanceof FlinkHiveException);
                        assertEquals(
                                "Reading or writing ACID table db1.dest is not supported.",
                                e.getMessage());
                    });
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    private void testCompressTextTable(boolean batch) throws Exception {
        TableEnvironment tableEnv =
                batch ? getTableEnvWithHiveCatalog() : getStreamTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.src (x string,y string)");
            hiveShell.execute("create table db1.dest like db1.src");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "src")
                    .addRow(new Object[] {"a", "b"})
                    .addRow(new Object[] {"c", "d"})
                    .commit();
            hiveCatalog.getHiveConf().setBoolVar(HiveConf.ConfVars.COMPRESSRESULT, true);
            tableEnv.executeSql("insert into db1.dest select * from db1.src").await();
            List<String> expected = Arrays.asList("a\tb", "c\td");
            verifyHiveQueryResult("select * from db1.dest", expected);
            verifyFlinkQueryResult(tableEnv.sqlQuery("select * from db1.dest"), expected);
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    private static TableEnvironment getTableEnvWithHiveCatalog() {
        TableEnvironment tableEnv =
                HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(SqlDialect.HIVE);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
        return tableEnv;
    }

    private TableEnvironment getStreamTableEnvWithHiveCatalog() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv =
                HiveTestUtils.createTableEnvWithBlinkPlannerStreamMode(env, SqlDialect.HIVE);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
        return tableEnv;
    }

    private void readWriteFormat(String format) throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();

        tableEnv.executeSql("create database db1");

        // create source and dest tables
        String suffix;
        if (format.equals("csv")) {
            suffix = "row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'";
        } else {
            suffix = "stored as " + format;
        }
        String tableSchema;
        // use 2018-08-20 00:00:00.1 to avoid multi-version print difference.
        List<Object> row1 = new ArrayList<>(Arrays.asList(1, "a", "2018-08-20 00:00:00.1"));
        List<Object> row2 = new ArrayList<>(Arrays.asList(2, "b", "2019-08-26 00:00:00.1"));
        // some data types are not supported for parquet tables in early versions --
        // https://issues.apache.org/jira/browse/HIVE-6384
        if (HiveVersionTestUtil.HIVE_120_OR_LATER || !format.equals("parquet")) {
            tableSchema = "(i int,s string,ts timestamp,dt date)";
            row1.add("2018-08-20");
            row2.add("2019-08-26");
        } else {
            tableSchema = "(i int,s string,ts timestamp)";
        }

        tableEnv.executeSql(
                String.format(
                        "create table db1.src %s partitioned by (p1 string, p2 timestamp) %s",
                        tableSchema, suffix));
        tableEnv.executeSql(
                String.format(
                        "create table db1.dest %s partitioned by (p1 string, p2 timestamp) %s",
                        tableSchema, suffix));

        // prepare source data with Hive
        // TABLE keyword in INSERT INTO is mandatory prior to 1.1.0
        hiveShell.execute(
                String.format(
                        "insert into table db1.src partition(p1='first',p2='2018-08-20 00:00:00.1') values (%s)",
                        toRowValue(row1)));
        hiveShell.execute(
                String.format(
                        "insert into table db1.src partition(p1='second',p2='2018-08-26 00:00:00.1') values (%s)",
                        toRowValue(row2)));

        List<String> expected =
                Arrays.asList(
                        String.join(
                                "\t",
                                ArrayUtils.concat(
                                        row1.stream().map(Object::toString).toArray(String[]::new),
                                        new String[] {"first", "2018-08-20 00:00:00.1"})),
                        String.join(
                                "\t",
                                ArrayUtils.concat(
                                        row2.stream().map(Object::toString).toArray(String[]::new),
                                        new String[] {"second", "2018-08-26 00:00:00.1"})));

        verifyFlinkQueryResult(tableEnv.sqlQuery("select * from db1.src"), expected);

        // Ignore orc write test for Hive version 2.0.x for now due to FLINK-13998
        if (!format.equals("orc") || !HiveShimLoader.getHiveVersion().startsWith("2.0")) {
            // populate dest table with source table
            tableEnv.executeSql("insert into db1.dest select * from db1.src").await();

            // verify data on hive side
            verifyHiveQueryResult("select * from db1.dest", expected);
        }

        tableEnv.executeSql("drop database db1 cascade");
    }

    private static void verifyWrittenData(List<Row> expected, List<String> results)
            throws Exception {
        assertEquals(expected.size(), results.size());
        Set<String> expectedSet = new HashSet<>();
        for (int i = 0; i < results.size(); i++) {
            final String rowString = expected.get(i).toString();
            expectedSet.add(rowString.substring(3, rowString.length() - 1).replaceAll(", ", "\t"));
        }
        assertEquals(expectedSet, new HashSet<>(results));
    }

    private static List<Row> generateRecords(int numRecords) {
        int arity = 4;
        List<Row> res = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            Row row = new Row(arity);
            row.setField(0, i);
            row.setField(1, (long) i);
            row.setField(2, Double.valueOf(String.valueOf(String.format("%d.%d", i, i))));
            row.setField(3, String.valueOf((char) ('a' + i)));
            res.add(row);
        }
        return res;
    }

    private static void verifyHiveQueryResult(String query, List<String> expected) {
        List<String> results = hiveShell.executeQuery(query);
        assertEquals(expected.size(), results.size());
        assertEquals(new HashSet<>(expected), new HashSet<>(results));
    }

    private static void verifyFlinkQueryResult(
            org.apache.flink.table.api.Table table, List<String> expected) throws Exception {
        List<Row> rows = CollectionUtil.iteratorToList(table.execute().collect());
        List<String> results =
                rows.stream()
                        .map(
                                row ->
                                        IntStream.range(0, row.getArity())
                                                .mapToObj(row::getField)
                                                .map(
                                                        o ->
                                                                o instanceof LocalDateTime
                                                                        ? Timestamp.valueOf(
                                                                                (LocalDateTime) o)
                                                                        : o)
                                                .map(Object::toString)
                                                .collect(Collectors.joining("\t")))
                        .collect(Collectors.toList());
        assertEquals(expected.size(), results.size());
        assertEquals(new HashSet<>(expected), new HashSet<>(results));
    }

    private static String toRowValue(List<Object> row) {
        return row.stream()
                .map(
                        o -> {
                            String res = o.toString();
                            if (o instanceof String) {
                                res = "'" + res + "'";
                            }
                            return res;
                        })
                .collect(Collectors.joining(","));
    }
}
