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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableBuilder;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FileUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * IT case for HiveCatalog. TODO: move to flink-connector-hive-test end-to-end test module once it's
 * setup
 */
public class HiveCatalogITCase {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    private static HiveCatalog hiveCatalog;

    private String sourceTableName = "csv_source";
    private String sinkTableName = "csv_sink";

    @BeforeClass
    public static void createCatalog() {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog.open();
    }

    @AfterClass
    public static void closeCatalog() {
        if (hiveCatalog != null) {
            hiveCatalog.close();
        }
    }

    @Test
    public void testCsvTableViaSQL() {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.registerCatalog("myhive", hiveCatalog);
        tableEnv.useCatalog("myhive");

        String path = this.getClass().getResource("/csv/test.csv").getPath();

        tableEnv.executeSql(
                "create table test2 (name String, age Int) with (\n"
                        + "   'connector.type' = 'filesystem',\n"
                        + "   'connector.path' = 'file://"
                        + path
                        + "',\n"
                        + "   'format.type' = 'csv'\n"
                        + ")");

        Table t = tableEnv.sqlQuery("SELECT * FROM myhive.`default`.test2");

        List<Row> result = CollectionUtil.iteratorToList(t.execute().collect());

        // assert query result
        assertEquals(
                new HashSet<>(Arrays.asList(Row.of("1", 1), Row.of("2", 2), Row.of("3", 3))),
                new HashSet<>(result));

        tableEnv.executeSql("ALTER TABLE test2 RENAME TO newtable");

        t = tableEnv.sqlQuery("SELECT * FROM myhive.`default`.newtable");

        result = CollectionUtil.iteratorToList(t.execute().collect());

        // assert query result
        assertEquals(
                new HashSet<>(Arrays.asList(Row.of("1", 1), Row.of("2", 2), Row.of("3", 3))),
                new HashSet<>(result));

        tableEnv.executeSql("DROP TABLE newtable");
    }

    @Test
    public void testCsvTableViaAPI() throws Exception {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.getConfig()
                .addConfiguration(new Configuration().set(CoreOptions.DEFAULT_PARALLELISM, 1));

        tableEnv.registerCatalog("myhive", hiveCatalog);
        tableEnv.useCatalog("myhive");

        TableSchema schema =
                TableSchema.builder()
                        .field("name", DataTypes.STRING())
                        .field("age", DataTypes.INT())
                        .build();

        FormatDescriptor format =
                new OldCsv().field("name", Types.STRING()).field("age", Types.INT());

        CatalogTable source =
                new CatalogTableBuilder(
                                new FileSystem()
                                        .path(
                                                this.getClass()
                                                        .getResource("/csv/test.csv")
                                                        .getPath()),
                                schema)
                        .withFormat(format)
                        .inAppendMode()
                        .withComment("Comment.")
                        .build();

        Path p = Paths.get(tempFolder.newFolder().getAbsolutePath(), "test.csv");

        CatalogTable sink =
                new CatalogTableBuilder(
                                new FileSystem().path(p.toAbsolutePath().toString()), schema)
                        .withFormat(format)
                        .inAppendMode()
                        .withComment("Comment.")
                        .build();

        hiveCatalog.createTable(
                new ObjectPath(HiveCatalog.DEFAULT_DB, sourceTableName), source, false);

        hiveCatalog.createTable(new ObjectPath(HiveCatalog.DEFAULT_DB, sinkTableName), sink, false);

        Table t =
                tableEnv.sqlQuery(
                        String.format("select * from myhive.`default`.%s", sourceTableName));

        List<Row> result = CollectionUtil.iteratorToList(t.execute().collect());
        result.sort(Comparator.comparing(String::valueOf));

        // assert query result
        assertEquals(Arrays.asList(Row.of("1", 1), Row.of("2", 2), Row.of("3", 3)), result);

        tableEnv.executeSql(
                        String.format(
                                "insert into myhive.`default`.%s select * from myhive.`default`.%s",
                                sinkTableName, sourceTableName))
                .await();

        // assert written result
        File resultFile = new File(p.toAbsolutePath().toString());
        BufferedReader reader = new BufferedReader(new FileReader(resultFile));
        String readLine;
        for (int i = 0; i < 3; i++) {
            readLine = reader.readLine();
            assertEquals(String.format("%d,%d", i + 1, i + 1), readLine);
        }

        // No more line
        assertNull(reader.readLine());

        tableEnv.executeSql(String.format("DROP TABLE %s", sourceTableName));
        tableEnv.executeSql(String.format("DROP TABLE %s", sinkTableName));
    }

    @Test
    public void testReadWriteCsv() throws Exception {
        // similar to CatalogTableITCase::testReadWriteCsvUsingDDL but uses HiveCatalog
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.getConfig()
                .getConfiguration()
                .setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        tableEnv.registerCatalog("myhive", hiveCatalog);
        tableEnv.useCatalog("myhive");

        String srcPath = this.getClass().getResource("/csv/test3.csv").getPath();

        tableEnv.executeSql(
                "CREATE TABLE src ("
                        + "price DECIMAL(10, 2),currency STRING,ts6 TIMESTAMP(6),ts AS CAST(ts6 AS TIMESTAMP(3)),WATERMARK FOR ts AS ts) "
                        + String.format(
                                "WITH ('connector.type' = 'filesystem','connector.path' = 'file://%s','format.type' = 'csv')",
                                srcPath));

        String sinkPath = new File(tempFolder.newFolder(), "csv-order-sink").toURI().toString();

        tableEnv.executeSql(
                "CREATE TABLE sink ("
                        + "window_end TIMESTAMP(3),max_ts TIMESTAMP(6),counter BIGINT,total_price DECIMAL(10, 2)) "
                        + String.format(
                                "WITH ('connector.type' = 'filesystem','connector.path' = '%s','format.type' = 'csv')",
                                sinkPath));

        tableEnv.executeSql(
                        "INSERT INTO sink "
                                + "SELECT TUMBLE_END(ts, INTERVAL '5' SECOND),MAX(ts6),COUNT(*),MAX(price) FROM src "
                                + "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)")
                .await();

        String expected =
                "2019-12-12 00:00:05.0,2019-12-12 00:00:04.004001,3,50.00\n"
                        + "2019-12-12 00:00:10.0,2019-12-12 00:00:06.006001,2,5.33\n";
        assertEquals(expected, FileUtils.readFileUtf8(new File(new URI(sinkPath))));
    }

    @Test
    public void testBatchReadWriteCsvWithProctime() {
        testReadWriteCsvWithProctime(false);
    }

    @Test
    public void testStreamReadWriteCsvWithProctime() {
        testReadWriteCsvWithProctime(true);
    }

    private void testReadWriteCsvWithProctime(boolean isStreaming) {
        TableEnvironment tableEnv = prepareTable(isStreaming);
        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("SELECT * FROM proctime_src").collect());
        Assert.assertEquals(5, rows.size());
        tableEnv.executeSql("DROP TABLE proctime_src");
    }

    @Test
    public void testTableApiWithProctimeForBatch() {
        testTableApiWithProctime(false);
    }

    @Test
    public void testTableApiWithProctimeForStreaming() {
        testTableApiWithProctime(true);
    }

    private void testTableApiWithProctime(boolean isStreaming) {
        TableEnvironment tableEnv = prepareTable(isStreaming);
        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tableEnv.from("proctime_src")
                                .select($("price"), $("ts"), $("l_proctime"))
                                .execute()
                                .collect());
        Assert.assertEquals(5, rows.size());
        tableEnv.executeSql("DROP TABLE proctime_src");
    }

    private TableEnvironment prepareTable(boolean isStreaming) {
        EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance().useBlinkPlanner();
        if (isStreaming) {
            builder = builder.inStreamingMode();
        } else {
            builder = builder.inBatchMode();
        }
        EnvironmentSettings settings = builder.build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.getConfig()
                .getConfiguration()
                .setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        tableEnv.registerCatalog("myhive", hiveCatalog);
        tableEnv.useCatalog("myhive");

        String srcPath = this.getClass().getResource("/csv/test3.csv").getPath();

        tableEnv.executeSql(
                "CREATE TABLE proctime_src ("
                        + "price DECIMAL(10, 2),"
                        + "currency STRING,"
                        + "ts6 TIMESTAMP(6),"
                        + "ts AS CAST(ts6 AS TIMESTAMP(3)),"
                        + "WATERMARK FOR ts AS ts,"
                        + "l_proctime AS PROCTIME( )) "
                        + // test " " in proctime()
                        String.format(
                                "WITH ("
                                        + "'connector.type' = 'filesystem',"
                                        + "'connector.path' = 'file://%s',"
                                        + "'format.type' = 'csv')",
                                srcPath));

        return tableEnv;
    }

    @Test
    public void testTableWithPrimaryKey() {
        EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance().useBlinkPlanner();
        EnvironmentSettings settings = builder.build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.getConfig()
                .getConfiguration()
                .setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        tableEnv.registerCatalog("catalog1", hiveCatalog);
        tableEnv.useCatalog("catalog1");

        final String createTable =
                "CREATE TABLE pk_src (\n"
                        + "  uuid varchar(40) not null,\n"
                        + "  price DECIMAL(10, 2),\n"
                        + "  currency STRING,\n"
                        + "  ts6 TIMESTAMP(6),\n"
                        + "  ts AS CAST(ts6 AS TIMESTAMP(3)),\n"
                        + "  WATERMARK FOR ts AS ts,\n"
                        + "  constraint ct1 PRIMARY KEY(uuid) NOT ENFORCED)\n"
                        + "  WITH (\n"
                        + "    'connector.type' = 'filesystem',"
                        + "    'connector.path' = 'file://fakePath',"
                        + "    'format.type' = 'csv')";

        tableEnv.executeSql(createTable);

        TableSchema tableSchema =
                tableEnv.getCatalog(tableEnv.getCurrentCatalog())
                        .map(
                                catalog -> {
                                    try {
                                        final ObjectPath tablePath =
                                                ObjectPath.fromString(
                                                        catalog.getDefaultDatabase()
                                                                + '.'
                                                                + "pk_src");
                                        return catalog.getTable(tablePath).getSchema();
                                    } catch (TableNotExistException e) {
                                        return null;
                                    }
                                })
                        .orElse(null);
        assertNotNull(tableSchema);
        assertEquals(
                tableSchema.getPrimaryKey(),
                Optional.of(UniqueConstraint.primaryKey("ct1", Collections.singletonList("uuid"))));
        tableEnv.executeSql("DROP TABLE pk_src");
    }

    @Test
    public void testNewTableFactory() throws Exception {
        TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.registerCatalog("myhive", hiveCatalog);
        tEnv.useCatalog("myhive");
        tEnv.getConfig().getConfiguration().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        String path = this.getClass().getResource("/csv/test.csv").getPath();

        PrintStream originalSystemOut = System.out;
        try {
            ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(arrayOutputStream));

            tEnv.executeSql(
                    "create table csv_table (name String, age Int) with ("
                            + "'connector.type' = 'filesystem',"
                            + "'connector.path' = 'file://"
                            + path
                            + "',"
                            + "'format.type' = 'csv')");
            tEnv.executeSql(
                    "create table print_table (name String, age Int) with ('connector' = 'print')");

            tEnv.executeSql("insert into print_table select * from csv_table").await();

            // assert query result
            assertEquals("+I[1, 1]\n+I[2, 2]\n+I[3, 3]\n", arrayOutputStream.toString());
        } finally {
            if (System.out != originalSystemOut) {
                System.out.close();
            }
            System.setOut(originalSystemOut);
            tEnv.executeSql("DROP TABLE csv_table");
            tEnv.executeSql("DROP TABLE print_table");
        }
    }

    @Test
    public void testConcurrentAccessHiveCatalog() throws Exception {
        int numThreads = 5;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        Callable<List<String>> listDBCallable = () -> hiveCatalog.listDatabases();
        List<Future<List<String>>> listDBFutures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            listDBFutures.add(executorService.submit(listDBCallable));
        }
        executorService.shutdown();
        for (Future<List<String>> future : listDBFutures) {
            future.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testTemporaryGenericTable() throws Exception {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(Arrays.asList(Row.of(1), Row.of(2)));
        tableEnv.executeSql(
                "create temporary table src(x int) with ('connector'='COLLECTION','is-bounded' = 'false')");
        File tempDir = Files.createTempDirectory("dest-").toFile();
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(() -> org.apache.commons.io.FileUtils.deleteQuietly(tempDir)));
        tableEnv.executeSql(
                "create temporary table dest(x int) with ("
                        + "'connector' = 'filesystem',"
                        + String.format("'path' = 'file://%s/1.csv',", tempDir.getAbsolutePath())
                        + "'format' = 'csv')");
        tableEnv.executeSql("insert into dest select * from src").await();

        tableEnv.executeSql(
                "create temporary table datagen(i int) with ("
                        + "'connector'='datagen',"
                        + "'rows-per-second'='5',"
                        + "'fields.i.kind'='sequence',"
                        + "'fields.i.start'='1',"
                        + "'fields.i.end'='10')");
        tableEnv.executeSql(
                "create temporary table blackhole(i int) with ('connector'='blackhole')");
        tableEnv.executeSql("insert into blackhole select * from datagen").await();
    }

    @Test
    public void testCreateTableLike() throws Exception {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
        tableEnv.executeSql("create table generic_table (x int) with ('connector'='COLLECTION')");
        tableEnv.useCatalog(EnvironmentSettings.DEFAULT_BUILTIN_CATALOG);
        tableEnv.executeSql(
                String.format(
                        "create table copy like `%s`.`default`.generic_table",
                        hiveCatalog.getName()));
        Catalog builtInCat = tableEnv.getCatalog(EnvironmentSettings.DEFAULT_BUILTIN_CATALOG).get();
        CatalogBaseTable catalogTable =
                builtInCat.getTable(
                        new ObjectPath(EnvironmentSettings.DEFAULT_BUILTIN_DATABASE, "copy"));
        assertEquals(1, catalogTable.getOptions().size());
        assertEquals("COLLECTION", catalogTable.getOptions().get(FactoryUtil.CONNECTOR.key()));
        assertEquals(1, catalogTable.getSchema().getFieldCount());
        assertEquals("x", catalogTable.getSchema().getFieldNames()[0]);
        assertEquals(DataTypes.INT(), catalogTable.getSchema().getFieldDataTypes()[0]);
    }

    @Test
    public void testViewSchema() throws Exception {
        TableEnvironment tableEnv =
                HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(SqlDialect.DEFAULT);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());

        tableEnv.executeSql("create database db1");
        try {
            tableEnv.useDatabase("db1");
            tableEnv.executeSql(
                    "create table src(x int,ts timestamp(3)) with ('connector'='datagen','number-of-rows'='10')");
            tableEnv.executeSql("create view v1 as select x,ts from src order by x limit 3");

            CatalogView catalogView =
                    (CatalogView) hiveCatalog.getTable(new ObjectPath("db1", "v1"));
            Schema viewSchema = catalogView.getUnresolvedSchema();
            assertEquals(
                    Schema.newBuilder()
                            .fromFields(
                                    new String[] {"x", "ts"},
                                    new AbstractDataType[] {
                                        DataTypes.INT(), DataTypes.TIMESTAMP(3)
                                    })
                            .build(),
                    viewSchema);

            List<Row> results =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select x from v1").collect());
            assertEquals(3, results.size());

            tableEnv.executeSql(
                    "create view v2 (v2_x,v2_ts) comment 'v2 comment' as select x,cast(ts as timestamp_ltz(3)) from v1");
            catalogView = (CatalogView) hiveCatalog.getTable(new ObjectPath("db1", "v2"));
            assertEquals(
                    Schema.newBuilder()
                            .fromFields(
                                    new String[] {"v2_x", "v2_ts"},
                                    new AbstractDataType[] {
                                        DataTypes.INT(), DataTypes.TIMESTAMP_LTZ(3)
                                    })
                            .build(),
                    catalogView.getUnresolvedSchema());
            assertEquals("v2 comment", catalogView.getComment());
            results =
                    CollectionUtil.iteratorToList(
                            tableEnv.executeSql("select * from v2").collect());
            assertEquals(3, results.size());
        } finally {
            tableEnv.executeSql("drop database db1 cascade");
        }
    }
}
