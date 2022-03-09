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

import org.apache.flink.formats.common.TimeFormats;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.utils.DateTimeUtils.toLocalDateTime;

/** Tests for the CSV file format. */
public class TableCsvFormatITCase extends JsonPlanTestBase {

    @Test
    public void testProjectPushDown() throws Exception {
        List<String> data = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");
        createSourceTable("MyTable", data, "a bigint", "b int not null", "c varchar");
        File sinkPath = createSinkTable("MySink", "a bigint", "c varchar");

        tableEnv.executeSql("insert into MySink select a, c from MyTable").await();

        assertResult(Arrays.asList("1,hi", "2,hello", "3,hello world"), sinkPath);
    }

    @Test
    public void testReadingMetadata() throws Exception {
        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.smallData3()),
                new String[] {"a int", "b bigint", "m varchar metadata"},
                new HashMap<String, String>() {
                    {
                        put("readable-metadata", "m:STRING");
                    }
                });

        File sinkPath = createSinkTable("MySink", "a bigint", "m varchar");

        tableEnv.executeSql("insert into MySink select a, m from MyTable").await();

        assertResult(Arrays.asList("1,Hi", "2,Hello", "3,Hello world"), sinkPath);
    }

    @Test
    public void testFilterPushDown() throws Exception {
        List<String> data = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");
        createSourceTable("MyTable", data, "a bigint", "b int not null", "c varchar");
        File sinkPath = createSinkTable("MySink", "a bigint", "b int", "c varchar");

        tableEnv.executeSql("insert into MySink select * from MyTable where a > 1").await();

        assertResult(Arrays.asList("2,1,hello", "3,2,hello world"), sinkPath);
    }

    @Test
    public void testPartitionPushDown() throws Exception {
        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.smallData3()),
                new String[] {"a int", "p bigint", "c varchar"},
                "p",
                new HashMap<String, String>() {
                    {
                        put("partition-list", "p:1;p:2");
                    }
                });
        File sinkPath = createSinkTable("MySink", "a int", "p bigint", "c varchar");

        tableEnv.executeSql("insert into MySink select * from MyTable where p = 2").await();

        assertResult(Arrays.asList("2,2,Hello", "3,2,Hello world"), sinkPath);
    }

    @Test
    public void testWatermarkPushDown() throws Exception {
        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.data3WithTimestamp()),
                new String[] {
                    "a int",
                    "b bigint",
                    "c varchar",
                    "ts timestamp(3)",
                    "watermark for ts as ts - interval '5' second"
                },
                new HashMap<String, String>() {
                    {
                        put("enable-watermark-push-down", "true");
                    }
                });

        File sinkPath = createSinkTable("MySink", "a int", "b bigint", "ts timestamp(3)");

        tableEnv.executeSql("insert into MySink select a, b, ts from MyTable where b = 3").await();

        assertResult(
                Arrays.asList(
                        "4,3," + formatSqlTimestamp(4000L),
                        "5,3," + formatSqlTimestamp(5000L),
                        "6,3," + formatSqlTimestamp(6000L)),
                sinkPath);
    }

    @Test
    public void testPushDowns() throws Exception {
        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.data3WithTimestamp()),
                new String[] {
                    "a int",
                    "b bigint",
                    "c varchar",
                    "ts timestamp(3)",
                    "watermark for ts as ts - interval '5' second"
                },
                "b",
                new HashMap<String, String>() {
                    {
                        put("readable-metadata", "a:INT");
                        put("filterable-fields", "a");
                        put("enable-watermark-push-down", "true");
                        put("partition-list", "b:1;b:2;b:3;b:4;b:5;b:6");
                    }
                });

        File sinkPath = createSinkTable("MySink", "a int", "ts timestamp(3)");

        tableEnv.executeSql("insert into MySink select a, ts from MyTable where b = 3 and a > 4")
                .await();

        assertResult(
                Arrays.asList("5," + formatSqlTimestamp(5000L), "6," + formatSqlTimestamp(6000L)),
                sinkPath);
    }

    private static String formatSqlTimestamp(long timestamp) {
        return TimeFormats.SQL_TIMESTAMP_FORMAT.format(toLocalDateTime(timestamp));
    }

    private void createSourceTable(String tableName, List<String> data, String... fieldNameAndTypes)
            throws IOException {
        File sourceFile = TEMPORARY_FOLDER.newFile();
        Collections.shuffle(data);
        Files.write(sourceFile.toPath(), String.join("\n", data).getBytes());

        Map<String, String> properties = new HashMap<>();
        properties.put("connector", "filesystem");
        properties.put("path", sourceFile.getAbsolutePath());
        properties.put("format", "csv");

        createTestSourceTable(tableName, fieldNameAndTypes, null, properties);
    }

    private File createSinkTable(String tableName, String... fieldNameAndTypes) throws IOException {
        File sinkPath = TEMPORARY_FOLDER.newFolder();

        Map<String, String> properties = new HashMap<>();
        properties.put("connector", "filesystem");
        properties.put("path", sinkPath.getAbsolutePath());
        properties.put("format", "csv");
        properties.put("csv.disable-quote-character", "true");

        createTestSinkTable(tableName, fieldNameAndTypes, null, properties);
        return sinkPath;
    }
}
