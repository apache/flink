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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link FileSystemTableSource} with partitioned field(s). */
class FileSystemTableSourceWithPartitionsITCase extends BatchTestBase {

    private static final String TABLE_NAME = "test_table";
    private static final List<Row> TEST_DATA =
            Arrays.asList(Row.of(1, 4, 7, 10), Row.of(2, 5, 8, 11), Row.of(3, 6, 9, 12));

    private File tempFolder;

    @BeforeEach
    void setup() throws IOException {
        tempFolder = createTempFolder();
    }

    @ParameterizedTest(name = "Partition count: {0}")
    @ValueSource(ints = {1, 2, 3, 4})
    void testPartitions(Integer partitionCount) throws IOException {
        writePartitionedTestFiles(partitionCount);
        createTestTable(partitionCount, Collections.emptyList());

        List<Row> actual = executeAndCollectResults("SELECT * FROM " + TABLE_NAME + " WHERE f0=1;");
        assertThat(actual).containsExactly(Row.of(1, 4, 7, 10));
        actual = executeAndCollectResults("SELECT * FROM " + TABLE_NAME + " WHERE f0=2 AND f1=5;");
        assertThat(actual).containsExactly(Row.of(2, 5, 8, 11));
        actual = executeAndCollectResults("SELECT * FROM " + TABLE_NAME + " WHERE f0=2 OR f1=4;");
        assertThat(actual).containsExactlyInAnyOrder(Row.of(1, 4, 7, 10), Row.of(2, 5, 8, 11));
        actual = executeAndCollectResults("SELECT * FROM " + TABLE_NAME + " WHERE f0>1;");
        assertThat(actual).containsExactlyInAnyOrder(Row.of(2, 5, 8, 11), Row.of(3, 6, 9, 12));
        actual = executeAndCollectResults("SELECT * FROM " + TABLE_NAME + " WHERE f0>0 AND f2<9;");
        assertThat(actual).containsExactlyInAnyOrder(Row.of(1, 4, 7, 10), Row.of(2, 5, 8, 11));
    }

    @ParameterizedTest(name = "Partition count: {0}")
    @ValueSource(ints = {1, 2, 3, 4})
    void testPartitionsWithMetadataFields(Integer partitionCount) throws IOException {
        List<Schema.UnresolvedColumn> additionalColumns =
                Arrays.asList(
                        new Schema.UnresolvedMetadataColumn(
                                "file.name", DataTypes.STRING(), "file.name", false),
                        new Schema.UnresolvedComputedColumn(
                                "v0", new SqlCallExpression("f0 * f1 + f2 - f3")));
        writePartitionedTestFiles(partitionCount);
        createTestTable(partitionCount, additionalColumns);

        List<Row> actual = executeAndCollectResults("SELECT * FROM " + TABLE_NAME + " WHERE f0=1;");
        assertThat(actual).containsExactly(Row.of(1, 4, 7, 10, "part0.csv", 1));
    }

    private void writePartitionedTestFiles(Integer partitionCount) throws IOException {
        for (Row row : TEST_DATA) {
            LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
            List<String> fileContent = new ArrayList<>();
            for (int i = 0; i <= TEST_DATA.size(); i++) {
                if (i < partitionCount) {
                    partitionSpec.put(String.format("f%d", i), String.valueOf(row.getField(i)));
                } else {
                    fileContent.add(String.valueOf(row.getField(i)));
                }
            }

            String partitionPath = PartitionPathUtils.generatePartitionPath(partitionSpec);
            File partitionSubDir = new File(tempFolder, partitionPath);
            partitionSubDir.mkdirs();

            Files.write(
                    Paths.get(partitionSubDir.getPath(), "part0.csv"),
                    Collections.singletonList(String.join(",", fileContent)),
                    StandardOpenOption.CREATE);
        }
    }

    private void createTestTable(
            Integer partitionCount, List<Schema.UnresolvedColumn> additionalColumns) {
        String[] partitionKeys =
                IntStream.range(0, partitionCount)
                        .mapToObj(i -> String.format("f%d", i))
                        .toArray(String[]::new);

        List<Schema.UnresolvedColumn> columns = new ArrayList<>();
        columns.add(new Schema.UnresolvedPhysicalColumn("f0", DataTypes.INT()));
        columns.add(new Schema.UnresolvedPhysicalColumn("f1", DataTypes.INT()));
        columns.add(new Schema.UnresolvedPhysicalColumn("f2", DataTypes.INT()));
        columns.add(new Schema.UnresolvedPhysicalColumn("f3", DataTypes.INT()));
        columns.addAll(additionalColumns);

        tEnv().createTable(
                        TABLE_NAME,
                        TableDescriptor.forConnector("filesystem")
                                .schema(Schema.newBuilder().fromColumns(columns).build())
                                .format("testcsv")
                                .option(FileSystemConnectorOptions.PATH, tempFolder.getPath())
                                .partitionedBy(partitionKeys)
                                .build());
    }

    private List<Row> executeAndCollectResults(String sql) {
        return CollectionUtil.iteratorToList(tEnv().sqlQuery(sql).execute().collect());
    }
}
