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

import static org.assertj.core.api.Assertions.assertThat;

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

    @ParameterizedTest(name = "{0} accessor for {1}")
    @MethodSource("metadataAccessorCases")
    void testMetadataAccessor(
            FileSystemTableSource.ReadableFileInfo fileInfo, String rawPath, String expected) {
        FileSourceSplit split =
                new FileSourceSplit("test-split", new Path(rawPath), 0L, 1L, 0L, 1L);

        Object actual = fileInfo.getAccessor().getValue(split);

        assertThat(actual).isEqualTo(StringData.fromString(expected));
    }

    static Stream<Arguments> fileNameCases() {
        return Stream.of(
                Arguments.of("file.txt", "file.txt"),
                Arguments.of("/tmp/input/dir/file.txt", "file.txt"),
                Arguments.of("tmp/input/dir/file.txt", "file.txt"),
                Arguments.of("./local/file.txt", "file.txt"),
                Arguments.of("../input/file.txt", "file.txt"),
                Arguments.of("../../data/report.csv", "report.csv"),
                Arguments.of("file:///tmp/input/user.csv", "user.csv"),
                Arguments.of("file://localhost/tmp/input/user.csv", "user.csv"),
                Arguments.of("file:/tmp/input/dir/", "dir"),
                Arguments.of("file:/C:/Users/me/Desktop/thing.txt", "thing.txt"),
                Arguments.of("file:/D:/tmp/input/test.csv", "test.csv"),
                Arguments.of("file:/D:/AI-Book/FlinkApplication/data/input/user.csv", "user.csv"),
                Arguments.of("s3://bucket/a/b/c.parquet", "c.parquet"),
                Arguments.of("hdfs://node:8020/data/output/result.parquet", "result.parquet"),
                Arguments.of("gs://my-bucket/path/to/data.json", "data.json"),
                Arguments.of("/tmp/input/archive.tar.gz", "archive.tar.gz"),
                Arguments.of("s3://bucket/data/backup.2026.01.01.csv", "backup.2026.01.01.csv"),
                Arguments.of("/tmp/input/spacing test.txt", "spacing test.txt"),
                Arguments.of("/tmp/input/report (final).csv", "report (final).csv"),
                Arguments.of("/tmp/input/file_name-v2.txt", "file_name-v2.txt"),
                Arguments.of("/tmp/input/it's a file.txt", "it's a file.txt"),
                Arguments.of("/tmp/input/it's a \"quoted\" name.txt", "it's a \"quoted\" name.txt"),
                Arguments.of("file:///tmp/input/my%20file.txt", "my%20file.txt"));
    }

    static Stream<Arguments> filePathCases() {
        return Stream.of(
                Arguments.of("file.txt", "file.txt"),
                Arguments.of("/tmp/input/dir/file.txt", "/tmp/input/dir/file.txt"),
                Arguments.of("tmp/input/dir/file.txt", "tmp/input/dir/file.txt"),
                Arguments.of("./local/file.txt", "local/file.txt"),
                Arguments.of("../input/file.txt", "../input/file.txt"),
                Arguments.of("../../data/report.csv", "../../data/report.csv"),
                Arguments.of("file:///tmp/input/user.csv", "/tmp/input/user.csv"),
                Arguments.of("file://localhost/tmp/input/user.csv", "/tmp/input/user.csv"),
                Arguments.of(
                        "file://localhost/tmp/weird \"quoted\" directory/user.csv",
                        "/tmp/weird \"quoted\" directory/user.csv"),
                Arguments.of("file:/tmp/input/dir/", "/tmp/input/dir"),
                Arguments.of(
                        "file:/C:/Users/me/Desktop/thing.txt", "/C:/Users/me/Desktop/thing.txt"),
                Arguments.of("file:/D:/tmp/input/test.csv", "/D:/tmp/input/test.csv"),
                Arguments.of(
                        "file:/D:/AI-Book/FlinkApplication/data/input/user.csv",
                        "/D:/AI-Book/FlinkApplication/data/input/user.csv"),
                Arguments.of("s3://bucket/a/b/c.parquet", "/a/b/c.parquet"),
                Arguments.of(
                        "hdfs://node:8020/data/output/result.parquet",
                        "/data/output/result.parquet"),
                Arguments.of("gs://my-bucket/path/to/data.json", "/path/to/data.json"),
                Arguments.of("/tmp/input/archive.tar.gz", "/tmp/input/archive.tar.gz"),
                Arguments.of(
                        "s3://bucket/data/backup.2026.01.01.csv", "/data/backup.2026.01.01.csv"),
                Arguments.of("/tmp/input/file_name-v2.txt", "/tmp/input/file_name-v2.txt"));
    }

    static Stream<Arguments> metadataAccessorCases() {
        return Stream.concat(
                withInfo(FileSystemTableSource.ReadableFileInfo.FILENAME, fileNameCases()),
                withInfo(FileSystemTableSource.ReadableFileInfo.FILEPATH, filePathCases()));
    }

    private static Stream<Arguments> withInfo(
            FileSystemTableSource.ReadableFileInfo info, Stream<Arguments> cases) {
        return cases.map(args -> Arguments.of(info, args.get()[0], args.get()[1]));
    }
}
