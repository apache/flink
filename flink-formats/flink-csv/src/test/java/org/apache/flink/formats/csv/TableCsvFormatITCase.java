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

package org.apache.flink.formats.csv;

import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.connector.file.table.FileSystemTableFactory;
import org.apache.flink.formats.common.TimeFormats;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.utils.DateTimeUtils.toLocalDateTime;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the CSV file format. */
@ExtendWith(MiniClusterExtension.class)
class TableCsvFormatITCase {

    private TableEnvironment tableEnv;

    @BeforeEach
    void setup() {
        tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    @AfterEach
    void after() {
        TestValuesTableFactory.clearAllData();
    }

    @Test
    void testProjectPushDown(@TempDir Path sourcePath, @TempDir Path sinkPath) throws Exception {
        List<String> data = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");

        Schema sourceSchema =
                Schema.newBuilder()
                        .column("a", BIGINT())
                        .column("b", INT())
                        .column("c", STRING())
                        .build();

        createSourceTable(sourcePath.resolve("table.csv"), "MyTable", data, sourceSchema);

        Schema sinkSchema = Schema.newBuilder().column("a", BIGINT()).column("c", STRING()).build();

        createSinkTable(sinkPath, "MySink", sinkSchema);

        tableEnv.executeSql("insert into MySink select a, c from MyTable").await();

        assertResult(Arrays.asList("1,hi", "2,hello", "3,hello world"), sinkPath);
    }

    @Test
    void testReadingMetadata(@TempDir Path sinkPath) throws Exception {

        Schema sourceSchema =
                Schema.newBuilder()
                        .column("a", INT())
                        .column("b", BIGINT())
                        .columnByMetadata("m", STRING())
                        .build();

        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.smallData3()),
                sourceSchema,
                new HashMap<String, String>() {
                    {
                        put("readable-metadata", "m:STRING");
                    }
                });

        Schema sinkSchema = Schema.newBuilder().column("a", BIGINT()).column("m", STRING()).build();

        createSinkTable(sinkPath, "MySink", sinkSchema);

        tableEnv.executeSql("insert into MySink select a, m from MyTable").await();

        assertResult(Arrays.asList("1,Hi", "2,Hello", "3,Hello world"), sinkPath);
    }

    @Test
    void testFilterPushDown(@TempDir Path sourcePath, @TempDir Path sinkPath) throws Exception {
        List<String> data = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");

        Schema sourceSchema =
                Schema.newBuilder()
                        .column("a", BIGINT())
                        .column("b", INT())
                        .column("c", STRING())
                        .build();

        createSourceTable(sourcePath.resolve("table.csv"), "MyTable", data, sourceSchema);

        Schema sinkSchema =
                Schema.newBuilder()
                        .column("a", BIGINT())
                        .column("b", INT())
                        .column("c", STRING())
                        .build();

        createSinkTable(sinkPath, "MySink", sinkSchema);

        tableEnv.executeSql("insert into MySink select * from MyTable where a > 1").await();

        assertResult(Arrays.asList("2,1,hello", "3,2,hello world"), sinkPath);
    }

    @Test
    void testPartitionPushDown(@TempDir Path sinkPath) throws Exception {
        Schema sourceSchema =
                Schema.newBuilder()
                        .column("a", INT())
                        .column("p", BIGINT())
                        .column("c", STRING())
                        .build();

        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.smallData3()),
                sourceSchema,
                new HashMap<String, String>() {
                    {
                        put("partition-list", "p:1;p:2");
                    }
                },
                "p");

        Schema sinkSchema =
                Schema.newBuilder()
                        .column("a", INT())
                        .column("p", BIGINT())
                        .column("c", STRING())
                        .build();

        createSinkTable(sinkPath, "MySink", sinkSchema);

        tableEnv.executeSql("insert into MySink select * from MyTable where p = 2").await();

        assertResult(Arrays.asList("2,2,Hello", "3,2,Hello world"), sinkPath);
    }

    @Test
    void testWatermarkPushDown(@TempDir Path sinkPath) throws Exception {

        Schema sourceSchema =
                Schema.newBuilder()
                        .column("a", INT())
                        .column("b", BIGINT())
                        .column("c", STRING())
                        .column("ts", TIMESTAMP(3))
                        .watermark("ts", "ts - INTERVAL '5' SECOND")
                        .build();

        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.data3WithTimestamp()),
                sourceSchema,
                new HashMap<String, String>() {
                    {
                        put("enable-watermark-push-down", "true");
                    }
                });

        Schema sinkSchema =
                Schema.newBuilder()
                        .column("a", INT())
                        .column("b", BIGINT())
                        .column("ts", TIMESTAMP(3))
                        .build();

        createSinkTable(sinkPath, "MySink", sinkSchema);

        tableEnv.executeSql("insert into MySink select a, b, ts from MyTable where b = 3").await();

        assertResult(
                Arrays.asList(
                        "4,3," + formatSqlTimestamp(4000L),
                        "5,3," + formatSqlTimestamp(5000L),
                        "6,3," + formatSqlTimestamp(6000L)),
                sinkPath);
    }

    @Test
    void testPushDowns(@TempDir Path sinkPath) throws Exception {
        Schema sourceSchema =
                Schema.newBuilder()
                        .column("a", INT())
                        .column("b", BIGINT())
                        .column("c", STRING())
                        .column("ts", TIMESTAMP(3))
                        .watermark("ts", "ts - INTERVAL '5' SECOND")
                        .build();

        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.data3WithTimestamp()),
                sourceSchema,
                new HashMap<String, String>() {
                    {
                        put("readable-metadata", "a:INT");
                        put("filterable-fields", "a");
                        put("enable-watermark-push-down", "true");
                        put("partition-list", "b:1;b:2;b:3;b:4;b:5;b:6");
                    }
                },
                "b");

        Schema sinkSchema =
                Schema.newBuilder().column("a", INT()).column("ts", TIMESTAMP(3)).build();

        createSinkTable(sinkPath, "MySink", sinkSchema);

        tableEnv.executeSql("insert into MySink select a, ts from MyTable where b = 3 and a > 4")
                .await();

        assertResult(
                Arrays.asList("5," + formatSqlTimestamp(5000L), "6," + formatSqlTimestamp(6000L)),
                sinkPath);
    }

    private static String formatSqlTimestamp(long timestamp) {
        return TimeFormats.SQL_TIMESTAMP_FORMAT.format(toLocalDateTime(timestamp));
    }

    private void createSourceTable(
            Path sourceFile, String tableName, List<String> data, Schema schema)
            throws IOException {

        Collections.shuffle(data);
        Files.write(sourceFile, String.join("\n", data).getBytes());

        tableEnv.createTemporaryTable(
                tableName,
                TableDescriptor.forConnector(FileSystemTableFactory.IDENTIFIER)
                        .option(
                                FileSystemConnectorOptions.PATH,
                                sourceFile.toAbsolutePath().toString())
                        .format(CsvCommons.IDENTIFIER)
                        .schema(schema)
                        .build());
    }

    private void createSinkTable(Path sinkPath, String tableName, Schema schema) {

        tableEnv.createTemporaryTable(
                tableName,
                TableDescriptor.forConnector(FileSystemTableFactory.IDENTIFIER)
                        .option(
                                FileSystemConnectorOptions.PATH,
                                sinkPath.toAbsolutePath().toString())
                        .option("csv.disable-quote-character", "true")
                        .format(CsvCommons.IDENTIFIER)
                        .schema(schema)
                        .build());
    }

    private void createTestValuesSourceTable(
            String tableName,
            List<Row> data,
            Schema schema,
            Map<String, String> extraProperties,
            @Nullable String... partitionFields) {

        String dataId = TestValuesTableFactory.registerData(data);
        Map<String, String> properties = new HashMap<>();
        properties.put("data-id", dataId);
        properties.put("bounded", "true");
        properties.put("disable-lookup", "true");
        properties.putAll(extraProperties);

        TableDescriptor.Builder descriptor =
                TableDescriptor.forConnector("values")
                        .schema(schema)
                        .partitionedBy(partitionFields);

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            descriptor.option(entry.getKey(), entry.getValue());
        }

        tableEnv.createTemporaryTable(tableName, descriptor.build());
    }

    private void assertResult(List<String> expected, Path resultFile) throws IOException {
        List<String> actual = readLines(resultFile.toFile());
        assertThat(actual).hasSameElementsAs(expected);
    }

    private List<String> readLines(File path) throws IOException {
        List<String> result = new ArrayList<>();
        for (File file : checkNotNull(path.listFiles())) {
            if (file.isHidden()) {
                continue;
            }
            if (file.isFile()) {
                String value = new String(Files.readAllBytes(file.toPath()));
                result.addAll(Arrays.asList(value.split("\n")));
            } else {
                result.addAll(readLines(file));
            }
        }
        return result;
    }
}
