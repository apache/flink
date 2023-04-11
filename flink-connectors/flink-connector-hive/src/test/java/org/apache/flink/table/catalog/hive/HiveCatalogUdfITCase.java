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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.functions.hive.util.TestHiveGenericUDF;
import org.apache.flink.table.functions.hive.util.TestHiveSimpleUDF;
import org.apache.flink.table.functions.hive.util.TestHiveUDTF;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.planner.runtime.utils.TestingRetractSink;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FileUtils;

import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT case for HiveCatalog. TODO: move to flink-connector-hive-test end-to-end test module once it's
 * setup
 */
public class HiveCatalogUdfITCase extends AbstractTestBase {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    private static HiveCatalog hiveCatalog;

    private final String sourceTableName = "csv_source";
    private final String sinkTableName = "csv_sink";

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
    public void testFlinkUdf() throws Exception {
        final ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("age", DataTypes.INT()));

        final Map<String, String> sourceOptions = new HashMap<>();
        sourceOptions.put("connector.type", "filesystem");
        sourceOptions.put("connector.path", getClass().getResource("/csv/test.csv").getPath());
        sourceOptions.put("format.type", "csv");

        CatalogTable unresolved =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(schema).build(),
                        "Comment.",
                        new ArrayList<>(),
                        sourceOptions);
        ResolvedCatalogTable source = new ResolvedCatalogTable(unresolved, schema);

        hiveCatalog.createTable(
                new ObjectPath(HiveCatalog.DEFAULT_DB, sourceTableName), source, false);

        hiveCatalog.createFunction(
                new ObjectPath(HiveCatalog.DEFAULT_DB, "myudf"),
                new CatalogFunctionImpl(TestHiveSimpleUDF.class.getCanonicalName()),
                false);
        hiveCatalog.createFunction(
                new ObjectPath(HiveCatalog.DEFAULT_DB, "mygenericudf"),
                new CatalogFunctionImpl(TestHiveGenericUDF.class.getCanonicalName()),
                false);
        hiveCatalog.createFunction(
                new ObjectPath(HiveCatalog.DEFAULT_DB, "myudtf"),
                new CatalogFunctionImpl(TestHiveUDTF.class.getCanonicalName()),
                false);
        hiveCatalog.createFunction(
                new ObjectPath(HiveCatalog.DEFAULT_DB, "myudaf"),
                new CatalogFunctionImpl(GenericUDAFSum.class.getCanonicalName()),
                false);

        testUdf(true);
        testUdf(false);
    }

    private void testUdf(boolean batch) throws Exception {
        StreamExecutionEnvironment env = null;
        TableEnvironment tEnv;
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings.newInstance();
        if (batch) {
            settingsBuilder.inBatchMode();
        } else {
            settingsBuilder.inStreamingMode();
        }
        if (batch) {
            tEnv = TableEnvironment.create(settingsBuilder.build());
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
        }

        BatchTestBase.configForMiniCluster(tEnv.getConfig());

        tEnv.registerCatalog("myhive", hiveCatalog);
        tEnv.useCatalog("myhive");

        String innerSql =
                format(
                        "select mygenericudf(myudf(name), 1) as a, mygenericudf(myudf(age), 1) as b,"
                                + " s from %s, lateral table(myudtf(name, 1)) as T(s)",
                        sourceTableName);

        String selectSql =
                format("select a, s, sum(b), myudaf(b) from (%s) group by a, s", innerSql);

        List<String> results;
        if (batch) {
            Path p = Paths.get(tempFolder.newFolder().getAbsolutePath(), "test.csv");

            final ResolvedSchema sinkSchema =
                    ResolvedSchema.of(
                            Column.physical("name1", DataTypes.STRING()),
                            Column.physical("name2", DataTypes.STRING()),
                            Column.physical("sum1", DataTypes.INT()),
                            Column.physical("sum2", DataTypes.BIGINT()));

            final Map<String, String> sinkOptions = new HashMap<>();
            sinkOptions.put("connector.type", "filesystem");
            sinkOptions.put("connector.path", p.toAbsolutePath().toString());
            sinkOptions.put("format.type", "csv");

            CatalogTable unresolved =
                    CatalogTable.of(
                            Schema.newBuilder().fromResolvedSchema(sinkSchema).build(),
                            "Comment.",
                            new ArrayList<>(),
                            sinkOptions);
            final ResolvedCatalogTable sink = new ResolvedCatalogTable(unresolved, sinkSchema);

            hiveCatalog.createTable(
                    new ObjectPath(HiveCatalog.DEFAULT_DB, sinkTableName), sink, false);

            tEnv.executeSql(format("insert into %s " + selectSql, sinkTableName)).await();

            // assert written result
            StringBuilder builder = new StringBuilder();
            try (Stream<Path> paths = Files.walk(Paths.get(p.toAbsolutePath().toString()))) {
                paths.filter(Files::isRegularFile)
                        .forEach(
                                path -> {
                                    try {
                                        String content = FileUtils.readFileUtf8(path.toFile());
                                        if (content.isEmpty()) {
                                            return;
                                        }
                                        builder.append(content);
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
            }
            results =
                    Arrays.stream(builder.toString().split("\n"))
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toList());
        } else {
            StreamTableEnvironment streamTEnv = (StreamTableEnvironment) tEnv;
            TestingRetractSink sink = new TestingRetractSink();
            streamTEnv
                    .toRetractStream(tEnv.sqlQuery(selectSql), Row.class)
                    .map(new JavaToScala())
                    .addSink((SinkFunction) sink);
            env.execute("");
            results = JavaScalaConversionUtil.toJava(sink.getRetractResults());
        }

        results = new ArrayList<>(results);
        results.sort(String::compareTo);
        assertThat(results).isEqualTo(Arrays.asList("1,1,2,2", "2,2,4,4", "3,3,6,6"));
    }

    @Test
    public void testTimestampUDF() throws Exception {

        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
        tableEnv.executeSql(
                String.format("create function myyear as '%s'", UDFYear.class.getName()));
        tableEnv.executeSql("create table src(ts timestamp)");
        try {
            HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "src")
                    .addRow(new Object[] {Timestamp.valueOf("2013-07-15 10:00:00")})
                    .addRow(new Object[] {Timestamp.valueOf("2019-05-23 17:32:55")})
                    .commit();

            List<Row> results =
                    CollectionUtil.iteratorToList(
                            tableEnv.sqlQuery("select myyear(ts) as y from src")
                                    .execute()
                                    .collect());
            assertThat(results).hasSize(2);
            assertThat(results.toString()).isEqualTo("[+I[2013], +I[2019]]");
        } finally {
            tableEnv.executeSql("drop table src");
        }
    }

    @Test
    public void testDateUDF() throws Exception {

        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
        tableEnv.executeSql(
                String.format("create function mymonth as '%s'", UDFMonth.class.getName()));
        tableEnv.executeSql("create table src(dt date)");
        try {
            HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "src")
                    .addRow(new Object[] {Date.valueOf("2019-01-19")})
                    .addRow(new Object[] {Date.valueOf("2019-03-02")})
                    .commit();

            List<Row> results =
                    CollectionUtil.iteratorToList(
                            tableEnv.sqlQuery("select mymonth(dt) as m from src order by m")
                                    .execute()
                                    .collect());
            assertThat(results).hasSize(2);
            assertThat(results.toString()).isEqualTo("[+I[1], +I[3]]");
        } finally {
            tableEnv.executeSql("drop table src");
        }
    }

    private static class JavaToScala
            implements MapFunction<Tuple2<Boolean, Row>, scala.Tuple2<Boolean, Row>> {

        @Override
        public scala.Tuple2<Boolean, Row> map(Tuple2<Boolean, Row> value) throws Exception {
            return new scala.Tuple2<>(value.f0, value.f1);
        }
    }
}
