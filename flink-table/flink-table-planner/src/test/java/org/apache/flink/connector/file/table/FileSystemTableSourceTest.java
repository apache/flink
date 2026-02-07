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

package org.apache.flink.connector.file.table;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for {@link FileSystemTableSource}. */
class FileSystemTableSourceTest extends TableTestBase {

    private StreamTableTestUtil util;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        TableEnvironment tEnv = util.getTableEnv();

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + " 'connector' = 'filesystem',"
                        + " 'format' = 'testcsv',"
                        + " 'path' = '/tmp')";
        tEnv.executeSql(srcTableDdl);

        String srcTableWithMetaDdl =
                "CREATE TABLE MyTableWithMeta (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar,\n"
                        + "  filemeta STRING METADATA FROM 'file.path'\n"
                        + ") with (\n"
                        + " 'connector' = 'filesystem',"
                        + " 'format' = 'testcsv',"
                        + " 'path' = '/tmp')";
        tEnv.executeSql(srcTableWithMetaDdl);

        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
    }

    @Test
    void testFilterPushDown() {
        util.verifyRelPlanInsert("insert into MySink select * from MyTable where a > 10");
    }

    @Test
    void testMetadataReading() {
        util.verifyRelPlanInsert(
                "insert into MySink(a, b, c) select a, b, filemeta from MyTableWithMeta");
    }

    @ParameterizedTest(name = "extractFileName({0}) -> {1}")
    @MethodSource("fileNameCases")
    void testFileNameExtraction(String rawPath, String expected) {
        String extractedFileName = FileSystemTableSource.extractFileName(new Path(rawPath));
        assertThat(extractedFileName).isEqualTo(expected);
    }

    @ParameterizedTest(name = "file.name accessor for {0}")
    @MethodSource("fileNameCases")
    void testFileNameMetadataAccessor(String rawPath, String expected) {
        FileSourceSplit split = mock(FileSourceSplit.class);
        when(split.path()).thenReturn(new Path(rawPath));

        Object actual =
                FileSystemTableSource.ReadableFileInfo.FILENAME.getAccessor().getValue(split);

        assertThat(actual).isEqualTo(StringData.fromString(expected));
    }

    static Stream<Arguments> fileNameCases() {
        return Stream.of(
                Arguments.of("file:/D:/AI-Book/FlinkApplication/data/input/user.csv", "user.csv"),
                Arguments.of("file:/D:/tmp/input/test.csv", "test.csv"),
                Arguments.of("file:/C:/Users/me/Desktop/thing.txt", "thing.txt"),
                Arguments.of("file:///tmp/input/user.csv", "user.csv"),
                Arguments.of("file:/tmp/input/dir/", "dir"),
                Arguments.of("file://localhost/tmp/input/user.csv", "user.csv"),
                Arguments.of("s3://bucket/a/b/c.parquet", "c.parquet"),
                Arguments.of("/tmp/input/dir/file.txt", "file.txt"));
    }
}
