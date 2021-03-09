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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test hive connector with table API. */
public class TableEnvHiveConnectorITCase {

    private static HiveCatalog hiveCatalog;
    private static HiveMetastoreClientWrapper hmsClient;

    @BeforeClass
    public static void setup() {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog.open();
        hmsClient =
                HiveMetastoreClientFactory.create(
                        hiveCatalog.getHiveConf(), HiveShimLoader.getHiveVersion());
    }

    @Test
    public void testDefaultPartitionName() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        tableEnv.executeSql("create table db1.src (x int, y int)");
        tableEnv.executeSql("create table db1.part (x int) partitioned by (y int)");
        HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "src")
                .addRow(new Object[] {1, 1})
                .addRow(new Object[] {2, null})
                .commit();

        // test generating partitions with default name
        tableEnv.executeSql("insert into db1.part select * from db1.src").await();
        HiveConf hiveConf = hiveCatalog.getHiveConf();
        String defaultPartName = hiveConf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME);
        Table hiveTable = hmsClient.getTable("db1", "part");
        Path defaultPartPath = new Path(hiveTable.getSd().getLocation(), "y=" + defaultPartName);
        FileSystem fs = defaultPartPath.getFileSystem(hiveConf);
        assertTrue(fs.exists(defaultPartPath));

        TableImpl flinkTable =
                (TableImpl) tableEnv.sqlQuery("select y, x from db1.part order by x");
        List<Row> rows = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        assertEquals("[+I[1, 1], +I[null, 2]]", rows.toString());

        tableEnv.executeSql("drop database db1 cascade");
    }

    @Test
    public void testGetNonExistingFunction() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        tableEnv.executeSql("create table db1.src (d double, s string)");
        tableEnv.executeSql("create table db1.dest (x bigint)");

        // just make sure the query runs through, no need to verify result
        tableEnv.executeSql("insert into db1.dest select count(d) from db1.src").await();

        tableEnv.executeSql("drop database db1 cascade");
    }

    @Test
    public void testDateTimestampPartitionColumns() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql(
                    "create table db1.part(x int) partitioned by (dt date,ts timestamp)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "part")
                    .addRow(new Object[] {1})
                    .addRow(new Object[] {2})
                    .commit("dt='2019-12-23',ts='2019-12-23 00:00:00'");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "part")
                    .addRow(new Object[] {3})
                    .commit("dt='2019-12-25',ts='2019-12-25 16:23:43.012'");
            List<Row> results =
                    CollectionUtil.iteratorToList(
                            tableEnv.sqlQuery("select * from db1.part order by x")
                                    .execute()
                                    .collect());
            assertEquals(
                    "[+I[1, 2019-12-23, 2019-12-23T00:00], +I[2, 2019-12-23, 2019-12-23T00:00], +I[3, 2019-12-25, 2019-12-25T16:23:43.012]]",
                    results.toString());

            results =
                    CollectionUtil.iteratorToList(
                            tableEnv.sqlQuery(
                                            "select x from db1.part where dt=cast('2019-12-25' as date)")
                                    .execute()
                                    .collect());
            assertEquals("[+I[3]]", results.toString());

            tableEnv.executeSql(
                            "insert into db1.part select 4,cast('2019-12-31' as date),cast('2019-12-31 12:00:00.0' as timestamp)")
                    .await();
            results =
                    CollectionUtil.iteratorToList(
                            tableEnv.sqlQuery("select max(dt) from db1.part").execute().collect());
            assertEquals("[+I[2019-12-31]]", results.toString());
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testUDTF() throws Exception {
        // W/o https://issues.apache.org/jira/browse/HIVE-11878 Hive registers the App classloader
        // as the classloader
        // for the UDTF and closes the App classloader when we tear down the session. This causes
        // problems for JUnit code
        // and shutdown hooks that have to run after the test finishes, because App classloader can
        // no longer load new
        // classes. And will crash the forked JVM, thus failing the test phase.
        // Therefore disable such tests for older Hive versions.
        String hiveVersion = HiveShimLoader.getHiveVersion();
        Assume.assumeTrue(
                hiveVersion.compareTo("2.0.0") >= 0 || hiveVersion.compareTo("1.3.0") >= 0);
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.simple (i int,a array<int>)");
            tableEnv.executeSql("create table db1.nested (a array<map<int, string>>)");
            tableEnv.executeSql(
                    "create function hiveudtf as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode'");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "simple")
                    .addRow(new Object[] {3, Arrays.asList(1, 2, 3)})
                    .commit();
            Map<Integer, String> map1 = new HashMap<>();
            map1.put(1, "a");
            map1.put(2, "b");
            Map<Integer, String> map2 = new HashMap<>();
            map2.put(3, "c");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "nested")
                    .addRow(new Object[] {Arrays.asList(map1, map2)})
                    .commit();

            List<Row> results =
                    CollectionUtil.iteratorToList(
                            tableEnv.sqlQuery(
                                            "select x from db1.simple, lateral table(hiveudtf(a)) as T(x)")
                                    .execute()
                                    .collect());
            assertEquals("[+I[1], +I[2], +I[3]]", results.toString());
            results =
                    CollectionUtil.iteratorToList(
                            tableEnv.sqlQuery(
                                            "select x from db1.nested, lateral table(hiveudtf(a)) as T(x)")
                                    .execute()
                                    .collect());
            assertEquals("[+I[{1=a, 2=b}], +I[{3=c}]]", results.toString());

            tableEnv.executeSql("create table db1.ts (a array<timestamp>)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "ts")
                    .addRow(
                            new Object[] {
                                new Object[] {
                                    Timestamp.valueOf("2015-04-28 15:23:00"),
                                    Timestamp.valueOf("2016-06-03 17:05:52")
                                }
                            })
                    .commit();
            results =
                    CollectionUtil.iteratorToList(
                            tableEnv.sqlQuery(
                                            "select x from db1.ts, lateral table(hiveudtf(a)) as T(x)")
                                    .execute()
                                    .collect());
            assertEquals("[+I[2015-04-28T15:23], +I[2016-06-03T17:05:52]]", results.toString());
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
            tableEnv.executeSql("drop function hiveudtf");
        }
    }

    @Test
    public void testNotNullConstraints() throws Exception {
        Assume.assumeTrue(HiveVersionTestUtil.HIVE_310_OR_LATER);
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql(
                    "create table db1.tbl (x int,y bigint not null enable rely,z string not null enable norely)");
            CatalogBaseTable catalogTable = hiveCatalog.getTable(new ObjectPath("db1", "tbl"));
            TableSchema tableSchema = catalogTable.getSchema();
            assertTrue(
                    "By default columns should be nullable",
                    tableSchema.getFieldDataTypes()[0].getLogicalType().isNullable());
            assertFalse(
                    "NOT NULL columns should be reflected in table schema",
                    tableSchema.getFieldDataTypes()[1].getLogicalType().isNullable());
            assertTrue(
                    "NOT NULL NORELY columns should be considered nullable",
                    tableSchema.getFieldDataTypes()[2].getLogicalType().isNullable());
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testPKConstraint() throws Exception {
        // While PK constraints are supported since Hive 2.1.0, the constraints cannot be RELY in
        // 2.x versions.
        // So let's only test for 3.x.
        Assume.assumeTrue(HiveVersionTestUtil.HIVE_310_OR_LATER);
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            // test rely PK constraints
            tableEnv.executeSql(
                    "create table db1.tbl1 (x tinyint,y smallint,z int, primary key (x,z) disable novalidate rely)");
            CatalogBaseTable catalogTable = hiveCatalog.getTable(new ObjectPath("db1", "tbl1"));
            TableSchema tableSchema = catalogTable.getSchema();
            assertTrue(tableSchema.getPrimaryKey().isPresent());
            UniqueConstraint pk = tableSchema.getPrimaryKey().get();
            assertEquals(2, pk.getColumns().size());
            assertTrue(pk.getColumns().containsAll(Arrays.asList("x", "z")));

            // test norely PK constraints
            tableEnv.executeSql(
                    "create table db1.tbl2 (x tinyint,y smallint, primary key (x) disable norely)");
            catalogTable = hiveCatalog.getTable(new ObjectPath("db1", "tbl2"));
            tableSchema = catalogTable.getSchema();
            assertFalse(tableSchema.getPrimaryKey().isPresent());

            // test table w/o PK
            tableEnv.executeSql("create table db1.tbl3 (x tinyint)");
            catalogTable = hiveCatalog.getTable(new ObjectPath("db1", "tbl3"));
            tableSchema = catalogTable.getSchema();
            assertFalse(tableSchema.getPrimaryKey().isPresent());
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testRegexSerDe() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql(
                    "create table db1.src (x int,y string) "
                            + "row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe' "
                            + "with serdeproperties ('input.regex'='([\\\\d]+)\\u0001([\\\\S]+)')");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "src")
                    .addRow(new Object[] {1, "a"})
                    .addRow(new Object[] {2, "ab"})
                    .commit();
            assertEquals(
                    "[+I[1, a], +I[2, ab]]",
                    CollectionUtil.iteratorToList(
                                    tableEnv.sqlQuery("select * from db1.src order by x")
                                            .execute()
                                            .collect())
                            .toString());
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testUpdatePartitionSD() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql(
                    "create table db1.dest (x int) partitioned by (p string) stored as rcfile");
            tableEnv.executeSql("insert overwrite db1.dest partition (p='1') select 1").await();
            tableEnv.executeSql("alter table db1.dest set fileformat sequencefile");
            tableEnv.executeSql("insert overwrite db1.dest partition (p='1') select 1").await();
            assertEquals(
                    "[+I[1, 1]]",
                    CollectionUtil.iteratorToList(
                                    tableEnv.sqlQuery("select * from db1.dest").execute().collect())
                            .toString());
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testParquetNameMapping() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.t1 (x int,y int) stored as parquet");
            tableEnv.executeSql("insert into table db1.t1 values (1,10),(2,20)").await();
            Table hiveTable = hiveCatalog.getHiveTable(new ObjectPath("db1", "t1"));
            String location = hiveTable.getSd().getLocation();
            tableEnv.executeSql(
                    String.format(
                            "create table db1.t2 (y int,x int) stored as parquet location '%s'",
                            location));
            tableEnv.getConfig()
                    .getConfiguration()
                    .setBoolean(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER, true);
            assertEquals(
                    "[+I[1], +I[2]]",
                    CollectionUtil.iteratorToList(
                                    tableEnv.sqlQuery("select x from db1.t1").execute().collect())
                            .toString());
            assertEquals(
                    "[+I[1], +I[2]]",
                    CollectionUtil.iteratorToList(
                                    tableEnv.sqlQuery("select x from db1.t2").execute().collect())
                            .toString());
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testNonExistingPartitionFolder() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create database db1");
        try {
            tableEnv.executeSql("create table db1.part (x int) partitioned by (p int)");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "part")
                    .addRow(new Object[] {1})
                    .commit("p=1");
            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "part")
                    .addRow(new Object[] {2})
                    .commit("p=2");
            tableEnv.executeSql("alter table db1.part add partition (p=3)");
            // remove one partition
            Path toRemove =
                    new Path(
                            hiveCatalog
                                    .getHiveTable(new ObjectPath("db1", "part"))
                                    .getSd()
                                    .getLocation(),
                            "p=2");
            FileSystem fs = toRemove.getFileSystem(hiveCatalog.getHiveConf());
            fs.delete(toRemove, true);

            List<Row> results =
                    CollectionUtil.iteratorToList(
                            tableEnv.sqlQuery("select * from db1.part").execute().collect());
            assertEquals("[+I[1, 1]]", results.toString());
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testInsertPartitionWithStarSource() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create table src (x int,y string)");
        HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "src")
                .addRow(new Object[] {1, "a"})
                .commit();
        tableEnv.executeSql("create table dest (x int) partitioned by (p1 int,p2 string)");
        tableEnv.executeSql("insert into dest partition (p1=1) select * from src").await();
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select * from dest").execute().collect());
        assertEquals("[+I[1, 1, a]]", results.toString());
        tableEnv.executeSql("drop table if exists src");
        tableEnv.executeSql("drop table if exists dest");
    }

    @Test
    public void testInsertPartitionWithValuesSource() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create table dest (x int) partitioned by (p1 int,p2 string)");
        tableEnv.executeSql("insert into dest partition (p1=1) values(1, 'a')").await();
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select * from dest").execute().collect());
        assertEquals("[+I[1, 1, a]]", results.toString());
        tableEnv.executeSql("drop table if exists dest");
    }

    @Test
    public void testDynamicPartWithOrderBy() throws Exception {
        TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.executeSql("create table src(x int,y int)");
        tableEnv.executeSql("create table dest(x int) partitioned by (p int)");
        try {
            HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "src")
                    .addRow(new Object[] {2, 0})
                    .addRow(new Object[] {1, 0})
                    .commit();
            // some hive feature relies on the results being sorted, e.g. bucket table
            tableEnv.executeSql("insert into dest partition(p) select * from src order by x")
                    .await();
            List<Row> results =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from dest").collect());
            assertEquals("[+I[1, 0], +I[2, 0]]", results.toString());
        } finally {
            tableEnv.executeSql("drop table src");
            tableEnv.executeSql("drop table dest");
        }
    }

    private TableEnvironment getTableEnvWithHiveCatalog() {
        TableEnvironment tableEnv =
                HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(SqlDialect.HIVE);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
        return tableEnv;
    }
}
