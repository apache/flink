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

import org.apache.flink.api.common.io.FinalizeOnMaster.FinalizationContext;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowUtils;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FileSystemOutputFormat}. */
class FileSystemOutputFormatTest {

    @TempDir private java.nio.file.Path outputPath;

    private final TestingFinalizationContext finalizationContext = new TestingFinalizationContext();

    private static final Supplier<List<StreamRecord<Row>>> DEFAULT_INPUT_SUPPLIER =
            () ->
                    Arrays.asList(
                            new StreamRecord<>(Row.of("a1", 1, "p1"), 1L),
                            new StreamRecord<>(Row.of("a2", 2, "p1"), 1L),
                            new StreamRecord<>(Row.of("a2", 2, "p2"), 1L),
                            new StreamRecord<>(Row.of("a3", 3, "p1"), 1L));

    private static final Supplier<List<String>> DEFAULT_OUTPUT_SUPPLIER =
            () -> Collections.singletonList("a1,1,p1\n" + "a2,2,p1\n" + "a2,2,p2\n" + "a3,3,p1\n");

    private static Map<File, String> getFileContentByPath(java.nio.file.Path directory)
            throws IOException {
        Map<File, String> contents = new HashMap<>(4);

        if (Files.notExists(directory) || !Files.isDirectory(directory)) {
            return contents;
        }

        final Collection<File> filesInBucket = FileUtils.listFiles(directory.toFile(), null, true);
        for (File file : filesInBucket) {
            contents.put(file, FileUtils.readFileToString(file));
        }
        return contents;
    }

    private static Map<File, String> getStagingFileContent(
            AtomicReference<FileSystemOutputFormat<Row>> ref) throws IOException {
        return getFileContentByPath(Paths.get(ref.get().getStagingPath().getPath()));
    }

    @BeforeEach
    void before() {
        RowUtils.USE_LEGACY_TO_STRING = true;
    }

    @AfterEach
    void after() {
        RowUtils.USE_LEGACY_TO_STRING = false;
    }

    @Test
    void testClosingWithoutInput() throws Exception {
        try (OneInputStreamOperatorTestHarness<Row, Object> testHarness =
                createTestHarness(
                        false, false, false, new LinkedHashMap<>(), new AtomicReference<>())) {
            testHarness.setup();
            testHarness.open();
        }
    }

    @Test
    void testNonPartition() throws Exception {
        checkWriteAndCommit(
                false,
                false,
                false,
                new LinkedHashMap<>(),
                DEFAULT_INPUT_SUPPLIER,
                DEFAULT_OUTPUT_SUPPLIER);
    }

    @Test
    void testOverrideNonPartition() throws Exception {
        testNonPartition();
        checkWriteAndCommit(
                true,
                false,
                false,
                new LinkedHashMap<>(),
                DEFAULT_INPUT_SUPPLIER,
                DEFAULT_OUTPUT_SUPPLIER);
    }

    @Test
    void testStaticPartition() throws Exception {
        LinkedHashMap<String, String> staticParts = new LinkedHashMap<>();
        staticParts.put("c", "p1");

        checkWriteAndCommit(
                false,
                true,
                false,
                staticParts,
                () ->
                        Arrays.asList(
                                new StreamRecord<>(Row.of("a1", 1), 1L),
                                new StreamRecord<>(Row.of("a2", 2), 1L),
                                new StreamRecord<>(Row.of("a2", 2), 1L),
                                new StreamRecord<>(Row.of("a3", 3), 1L)),
                () ->
                        Collections.singletonList(
                                "c=p1:" + "a1,1\n" + "a2,2\n" + "a2,2\n" + "a3,3\n"));
    }

    @Test
    void testDynamicPartition() throws Exception {
        checkWriteAndCommit(
                false,
                true,
                false,
                new LinkedHashMap<>(),
                DEFAULT_INPUT_SUPPLIER,
                () -> Arrays.asList("c=p1:" + "a1,1\n" + "a2,2\n" + "a3,3\n", "c=p2:" + "a2,2\n"));
    }

    @Test
    void testGroupedDynamicPartition() throws Exception {
        checkWriteAndCommit(
                false,
                true,
                true,
                new LinkedHashMap<>(),
                () ->
                        Arrays.asList(
                                new StreamRecord<>(Row.of("a1", 1, "p1"), 1L),
                                new StreamRecord<>(Row.of("a2", 2, "p1"), 1L),
                                new StreamRecord<>(Row.of("a3", 3, "p1"), 1L),
                                new StreamRecord<>(Row.of("a2", 2, "p2"), 1L)),
                () -> Arrays.asList("c=p1:" + "a1,1\n" + "a2,2\n" + "a3,3\n", "c=p2:" + "a2,2\n"));
    }

    @Test
    void testGetUniqueStagingDirectory() throws IOException {
        final Path alreadyExistingStagingDir = new Path(outputPath.toFile().getAbsolutePath());
        assertThat(alreadyExistingStagingDir.getFileSystem().exists(alreadyExistingStagingDir))
                .as("The staging folder should already exist.")
                .isTrue();

        final FileSystemOutputFormat.Builder<Row> builder =
                new FileSystemOutputFormat.Builder<Row>()
                        .setPartitionColumns(new String[0])
                        .setFormatFactory(TextOutputFormat::new)
                        .setMetaStoreFactory(
                                new FileSystemCommitterTest.TestMetaStoreFactory(
                                        new Path(outputPath.toFile().getAbsolutePath())))
                        .setPartitionComputer(
                                new RowPartitionComputer("default", new String[0], new String[0]))
                        .setStagingPath(alreadyExistingStagingDir);

        assertThatThrownBy(builder::build)
                .as("Reusing a folder should cause an error.")
                .isInstanceOf(IllegalStateException.class);
    }

    private void checkWriteAndCommit(
            boolean override,
            boolean partitioned,
            boolean dynamicGrouped,
            LinkedHashMap<String, String> staticPartitions,
            Supplier<List<StreamRecord<Row>>> inputSupplier,
            Supplier<List<String>> outputSupplier)
            throws Exception {
        List<String> expectedOutput = outputSupplier.get();
        int expectedFileNum = expectedOutput.size();
        AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
        try (OneInputStreamOperatorTestHarness<Row, Object> testHarness =
                createTestHarness(override, partitioned, dynamicGrouped, staticPartitions, ref)) {
            testHarness.setup();
            testHarness.open();
            for (StreamRecord<Row> record : inputSupplier.get()) {
                testHarness.processElement(record);
            }
            assertThat(getStagingFileContent(ref)).hasSize(expectedFileNum);
        }

        ref.get().finalizeGlobal(finalizationContext);
        assertThat(Paths.get(ref.get().getStagingPath().getPath())).doesNotExist();

        Map<File, String> fileToContent = getFileContentByPath(outputPath);
        assertThat(fileToContent).hasSize(expectedFileNum);
        if (partitioned) {
            assertPartitionedOutput(expectedOutput, fileToContent);
        } else {
            assertThat(fileToContent.values()).containsExactlyInAnyOrderElementsOf(expectedOutput);
        }
    }

    private void assertPartitionedOutput(
            List<String> expectedOutput, Map<File, String> fileToContent) {
        final String delimiter = ":";
        Map<String, String> partitionToContent =
                fileToContent.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        e -> e.getKey().getParentFile().getName(),
                                        Map.Entry::getValue));

        Map<String, String> expectedPartitionContent =
                expectedOutput.stream()
                        .map(r -> r.split(delimiter))
                        .collect(Collectors.toMap(splits -> splits[0], splits -> splits[1]));

        assertThat(partitionToContent).containsExactlyInAnyOrderEntriesOf(expectedPartitionContent);
    }

    private FileSystemOutputFormat<Row> createSinkFormat(
            boolean override,
            boolean partition,
            boolean dynamicGrouped,
            LinkedHashMap<String, String> staticPartitions) {
        String[] columnNames = new String[] {"a", "b", "c"};
        String[] partitionColumns = partition ? new String[] {"c"} : new String[0];
        Path path = new Path(outputPath.toString());
        TableMetaStoreFactory msFactory = new FileSystemCommitterTest.TestMetaStoreFactory(path);
        return new FileSystemOutputFormat.Builder<Row>()
                .setMetaStoreFactory(msFactory)
                .setPath(path)
                .setOverwrite(override)
                .setPartitionColumns(partitionColumns)
                .setPartitionComputer(
                        new RowPartitionComputer("default", columnNames, partitionColumns))
                .setFormatFactory(TextOutputFormat::new)
                .setDynamicGrouped(dynamicGrouped)
                .setStaticPartitions(staticPartitions)
                .build();
    }

    private OneInputStreamOperatorTestHarness<Row, Object> createTestHarness(
            boolean override,
            boolean partition,
            boolean dynamicGrouped,
            LinkedHashMap<String, String> staticPartitions,
            AtomicReference<FileSystemOutputFormat<Row>> sinkRef)
            throws Exception {
        FileSystemOutputFormat<Row> sink =
                createSinkFormat(override, partition, dynamicGrouped, staticPartitions);
        sinkRef.set(sink);

        return new OneInputStreamOperatorTestHarness<>(
                new StreamSink<>(new OutputFormatSinkFunction<>(sink)),
                // test parallelism
                3,
                3,
                0);
    }

    private static class TestingFinalizationContext implements FinalizationContext {

        @Override
        public int getParallelism() {
            return 1;
        }

        @Override
        public int getFinishedAttempt(int subtaskIndex) {
            return 0;
        }
    }
}
