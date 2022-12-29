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

package org.apache.flink.connector.file.table.batch.compact;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.connector.file.table.FileSystemFactory;
import org.apache.flink.connector.file.table.PartitionTempFileManager;
import org.apache.flink.connector.file.table.RowPartitionComputer;
import org.apache.flink.connector.file.table.stream.compact.AbstractCompactTestBase;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorInput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.InputFile;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BatchFileWriter}. */
public class BatchFileWriterTest extends AbstractCompactTestBase {
    private final String[] columnNames = new String[] {"a", "b", "c"};
    private FileSystemFactory fsFactory = FileSystem::get;

    @Test
    public void testWriteWithoutPartition() throws Exception {
        String[] partitionColumns = new String[0];
        BatchFileWriter<Row> fileWriter =
                createBatchFileWriter(columnNames, partitionColumns, new LinkedHashMap<>(), false);
        PartitionTempFileManager tempFileManager =
                new PartitionTempFileManager(
                        fsFactory, folder, 0, 0, OutputFileConfig.builder().build());

        try (OneInputStreamOperatorTestHarness<Row, CoordinatorInput> testHarness =
                new OneInputStreamOperatorTestHarness<>(fileWriter)) {
            testHarness.setup();
            testHarness.open();

            // write data
            testHarness.processElement(new StreamRecord<>(Row.of("a1", 1, 2)));
            testHarness.processElement(new StreamRecord<>(Row.of("a1", 1, 2)));
            List<CoordinatorInput> coordinatorInputs = testHarness.extractOutputValues();

            // should only write and emit one file
            assertInputFile(
                    coordinatorInputs,
                    1,
                    Collections.singletonList(""),
                    Collections.singletonList(tempFileManager.createPartitionDir()));
        }
    }

    @Test
    public void testWriteWithStaticPartition() throws Exception {
        String[] partitionColumns = new String[] {"b", "c"};
        LinkedHashMap<String, String> staticParts = new LinkedHashMap<>();
        staticParts.put("b", "p1");
        staticParts.put("c", "p2");

        BatchFileWriter<Row> fileWriter =
                createBatchFileWriter(partitionColumns, partitionColumns, staticParts, false);
        PartitionTempFileManager tempFileManager =
                new PartitionTempFileManager(
                        fsFactory, folder, 0, 0, OutputFileConfig.builder().build());

        try (OneInputStreamOperatorTestHarness<Row, CoordinatorInput> testHarness =
                new OneInputStreamOperatorTestHarness<>(fileWriter)) {
            testHarness.setup();
            testHarness.open();

            testHarness.processElement(new StreamRecord<>(Row.of("a1", 1, 2)));
            testHarness.processElement(new StreamRecord<>(Row.of("a1", 1, 2)));
            List<CoordinatorInput> coordinatorInputs = testHarness.extractOutputValues();

            // should only write and emit one file
            assertInputFile(
                    coordinatorInputs,
                    1,
                    Collections.singletonList("b=p1/c=p2/"),
                    Collections.singletonList(tempFileManager.createPartitionDir("b=p1/c=p2/")));
        }
    }

    @Test
    public void testWriteWithoutDynamicPartitionGrouped() throws Exception {
        // test without dynamicGrouped
        testWriteWithDynamicPartition(false);
    }

    @Test
    public void testWriteWithDynamicPartitionGrouped() throws Exception {
        // test with dynamicGrouped
        testWriteWithDynamicPartition(true);
    }

    private void testWriteWithDynamicPartition(boolean dynamicGrouped) throws Exception {
        String[] partitionColumns = new String[] {"b", "c"};

        BatchFileWriter<Row> fileWriter =
                createBatchFileWriter(
                        columnNames, partitionColumns, new LinkedHashMap<>(), dynamicGrouped);
        PartitionTempFileManager tempFileManager =
                new PartitionTempFileManager(
                        fsFactory, folder, 0, 0, OutputFileConfig.builder().build());

        try (OneInputStreamOperatorTestHarness<Row, CoordinatorInput> testHarness =
                new OneInputStreamOperatorTestHarness<>(fileWriter)) {
            testHarness.setup();
            testHarness.open();

            testHarness.processElement(new StreamRecord<>(Row.of("a1", 1, 2)));
            testHarness.processElement(new StreamRecord<>(Row.of("a1", 2, 1)));
            List<CoordinatorInput> coordinatorInputs = testHarness.extractOutputValues();
            // should write two files, one if for partition
            assertInputFile(
                    coordinatorInputs,
                    2,
                    Arrays.asList("b=1/c=2/", "b=2/c=1/"),
                    Arrays.asList(
                            tempFileManager.createPartitionDir("b=1/c=2/"),
                            tempFileManager.createPartitionDir("b=2/c=1/")));
        }
    }

    private void assertInputFile(
            List<CoordinatorInput> coordinatorInputs,
            int expectSize,
            List<String> expectedPartitions,
            List<Path> expectedFilePaths) {
        assertThat(coordinatorInputs).hasSize(expectSize);
        for (int i = 0; i < expectSize; i++) {
            CoordinatorInput input = coordinatorInputs.get(i);
            assertThat(input).isInstanceOf(InputFile.class);
            InputFile inputFile = (InputFile) input;
            assertThat(inputFile.getPartition()).isEqualTo(expectedPartitions.get(i));
            assertThat(inputFile.getFile()).isEqualTo(expectedFilePaths.get(i));
        }
    }

    private BatchFileWriter<Row> createBatchFileWriter(
            String[] columnNames,
            String[] partitionColumns,
            LinkedHashMap<String, String> staticPartitions,
            boolean dynamicGrouped) {
        return new BatchFileWriter<>(
                fsFactory,
                folder,
                partitionColumns,
                dynamicGrouped,
                staticPartitions,
                TextOutputFormat::new,
                new RowPartitionComputer("default", columnNames, partitionColumns),
                OutputFileConfig.builder().build());
    }
}
