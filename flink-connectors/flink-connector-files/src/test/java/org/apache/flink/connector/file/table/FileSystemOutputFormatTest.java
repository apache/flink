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
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Test for {@link FileSystemOutputFormat}. */
class FileSystemOutputFormatTest {

    @TempDir private java.nio.file.Path tmpPath;
    @TempDir private java.nio.file.Path outputPath;

    private final TestingFinalizationContext finalizationContext = new TestingFinalizationContext();

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
                createSink(false, false, false, new LinkedHashMap<>(), new AtomicReference<>())) {
            testHarness.setup();
            testHarness.open();
        }
    }

    @Test
    void testNonPartition() throws Exception {
        AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
        try (OneInputStreamOperatorTestHarness<Row, Object> testHarness =
                createSink(false, false, false, new LinkedHashMap<>(), ref)) {
            writeUnorderedRecords(testHarness);
            assertThat(getFileContentByPath(tmpPath)).hasSize(1);
        }

        ref.get().finalizeGlobal(finalizationContext);
        Map<File, String> content = getFileContentByPath(outputPath);
        assertThat(content.values())
                .containsExactly("a1,1,p1\n" + "a2,2,p1\n" + "a2,2,p2\n" + "a3,3,p1\n");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testStagingDirBehavior(boolean shareStagingDir) throws Exception {
        // sink1
        AtomicReference<FileSystemOutputFormat<Row>> sinkRef1 = new AtomicReference<>();
        AtomicReference<Map<File, String>> fileToCommitRef1 = new AtomicReference<>();
        writeRowsToSink(
                sinkRef1,
                fileToCommitRef1,
                getStagingDir(shareStagingDir),
                Row.of("a1", 1, "x1"),
                Row.of("a2", 2, "x2"));

        // sink2
        AtomicReference<FileSystemOutputFormat<Row>> sinkRef2 = new AtomicReference<>();
        AtomicReference<Map<File, String>> fileToCommitRef2 = new AtomicReference<>();
        writeRowsToSink(
                sinkRef2,
                fileToCommitRef2,
                getStagingDir(shareStagingDir),
                Row.of("b1", 1, "y1"),
                Row.of("b2", 2, "y2"));

        assertSinkBehavior(sinkRef1, fileToCommitRef1, sinkRef2, fileToCommitRef2, shareStagingDir);
    }

    private void writeRowsToSink(
            AtomicReference<FileSystemOutputFormat<Row>> sinkRef,
            AtomicReference<Map<File, String>> contentRef,
            Path stagingDir,
            Row... records)
            throws Exception {
        try (OneInputStreamOperatorTestHarness<Row, Object> testHarness =
                createSink(false, false, false, stagingDir, new LinkedHashMap<>(), sinkRef)) {
            writeUnorderedRecords(testHarness, Arrays.asList(records));
            contentRef.set(getFileContentByPath(Paths.get(stagingDir.getPath())));
        }
    }

    private Path getStagingDir(boolean shareStagingDir) {
        String pathPostfix = FileSystemTableSink.getStagingPathPostfix(shareStagingDir);
        return Path.fromLocalFile(tmpPath.resolve(pathPostfix).toFile());
    }

    private void assertSinkBehavior(
            AtomicReference<FileSystemOutputFormat<Row>> sinkRef1,
            AtomicReference<Map<File, String>> fileToCommitRef1,
            AtomicReference<FileSystemOutputFormat<Row>> sinkRef2,
            AtomicReference<Map<File, String>> fileToCommitRef2,
            boolean shareStagingDir)
            throws Exception {
        Map<File, String> fileToCommit1 = fileToCommitRef1.get();
        Map<File, String> fileToCommit2 = fileToCommitRef2.get();
        assertThat(fileToCommit2.keySet()).allMatch(File::exists);
        if (shareStagingDir) {
            assertThat(fileToCommit1.keySet()).noneMatch(File::exists);
        } else {
            assertThat(fileToCommit1.keySet()).allMatch(File::exists);
        }
        sinkRef1.get().finalizeGlobal(finalizationContext);
        sinkRef2.get().finalizeGlobal(finalizationContext);
        Map<File, String> committedFiles = getFileContentByPath(outputPath);
        if (shareStagingDir) {
            assertThat(committedFiles).hasSize(1);
            assertThat(committedFiles.values()).containsExactlyElementsOf(fileToCommit2.values());
        } else {
            assertThat(committedFiles).hasSize(2);
            assertThat(committedFiles.values())
                    .containsExactlyInAnyOrderElementsOf(
                            Stream.concat(
                                            fileToCommit1.values().stream(),
                                            fileToCommit2.values().stream())
                                    .collect(Collectors.toList()));
        }
    }

    private void writeUnorderedRecords(OneInputStreamOperatorTestHarness<Row, Object> testHarness)
            throws Exception {
        List<Row> records =
                Arrays.asList(
                        Row.of("a1", 1, "p1"),
                        Row.of("a2", 2, "p1"),
                        Row.of("a2", 2, "p2"),
                        Row.of("a3", 3, "p1"));
        writeUnorderedRecords(testHarness, records);
    }

    private void writeUnorderedRecords(
            OneInputStreamOperatorTestHarness<Row, Object> testHarness, List<Row> records)
            throws Exception {
        testHarness.setup();
        testHarness.open();
        for (Row record : records) {
            testHarness.processElement(new StreamRecord<>(record, 1L));
        }
    }

    @Test
    void testOverrideNonPartition() throws Exception {
        testNonPartition();

        AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
        try (OneInputStreamOperatorTestHarness<Row, Object> testHarness =
                createSink(true, false, false, new LinkedHashMap<>(), ref)) {
            writeUnorderedRecords(testHarness);
            assertThat(getFileContentByPath(tmpPath)).hasSize(1);
        }

        ref.get().finalizeGlobal(finalizationContext);
        Map<File, String> content = getFileContentByPath(outputPath);
        assertThat(content).hasSize(1);
        assertThat(content.values())
                .containsExactly("a1,1,p1\n" + "a2,2,p1\n" + "a2,2,p2\n" + "a3,3,p1\n");
        assertThat(new File(tmpPath.toUri())).doesNotExist();
    }

    @Test
    void testStaticPartition() throws Exception {
        AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
        LinkedHashMap<String, String> staticParts = new LinkedHashMap<>();
        staticParts.put("c", "p1");
        try (OneInputStreamOperatorTestHarness<Row, Object> testHarness =
                createSink(false, true, false, staticParts, ref)) {
            testHarness.setup();
            testHarness.open();

            testHarness.processElement(new StreamRecord<>(Row.of("a1", 1), 1L));
            testHarness.processElement(new StreamRecord<>(Row.of("a2", 2), 1L));
            testHarness.processElement(new StreamRecord<>(Row.of("a2", 2), 1L));
            testHarness.processElement(new StreamRecord<>(Row.of("a3", 3), 1L));
            assertThat(getFileContentByPath(tmpPath)).hasSize(1);
        }

        ref.get().finalizeGlobal(finalizationContext);
        Map<File, String> content = getFileContentByPath(outputPath);
        assertThat(content).hasSize(1);
        assertThat(content.keySet().iterator().next().getParentFile().getName()).isEqualTo("c=p1");
        assertThat(content.values()).containsExactly("a1,1\n" + "a2,2\n" + "a2,2\n" + "a3,3\n");
        assertThat(new File(tmpPath.toUri())).doesNotExist();
    }

    @Test
    void testDynamicPartition() throws Exception {
        AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
        try (OneInputStreamOperatorTestHarness<Row, Object> testHarness =
                createSink(false, true, false, new LinkedHashMap<>(), ref)) {
            writeUnorderedRecords(testHarness);
            assertThat(getFileContentByPath(tmpPath)).hasSize(2);
        }

        ref.get().finalizeGlobal(finalizationContext);
        Map<File, String> content = getFileContentByPath(outputPath);
        Map<String, String> sortedContent = new TreeMap<>();
        content.forEach((file, s) -> sortedContent.put(file.getParentFile().getName(), s));

        assertThat(sortedContent).hasSize(2);
        assertThat(sortedContent)
                .contains(entry("c=p1", "a1,1\n" + "a2,2\n" + "a3,3\n"), entry("c=p2", "a2,2\n"));
        assertThat(new File(tmpPath.toUri())).doesNotExist();
    }

    @Test
    void testGroupedDynamicPartition() throws Exception {
        AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
        try (OneInputStreamOperatorTestHarness<Row, Object> testHarness =
                createSink(false, true, true, new LinkedHashMap<>(), ref)) {
            testHarness.setup();
            testHarness.open();

            testHarness.processElement(new StreamRecord<>(Row.of("a1", 1, "p1"), 1L));
            testHarness.processElement(new StreamRecord<>(Row.of("a2", 2, "p1"), 1L));
            testHarness.processElement(new StreamRecord<>(Row.of("a3", 3, "p1"), 1L));
            testHarness.processElement(new StreamRecord<>(Row.of("a2", 2, "p2"), 1L));
            assertThat(getFileContentByPath(tmpPath)).hasSize(2);
        }

        ref.get().finalizeGlobal(finalizationContext);
        Map<File, String> content = getFileContentByPath(outputPath);
        Map<String, String> sortedContent = new TreeMap<>();
        content.forEach((file, s) -> sortedContent.put(file.getParentFile().getName(), s));

        assertThat(sortedContent).hasSize(2);
        assertThat(sortedContent.get("c=p1")).isEqualTo("a1,1\n" + "a2,2\n" + "a3,3\n");
        assertThat(sortedContent.get("c=p2")).isEqualTo("a2,2\n");
        assertThat(new File(tmpPath.toUri())).doesNotExist();
    }

    private OneInputStreamOperatorTestHarness<Row, Object> createSink(
            boolean override,
            boolean partition,
            boolean dynamicGrouped,
            Path stagingDir,
            LinkedHashMap<String, String> staticPartitions,
            AtomicReference<FileSystemOutputFormat<Row>> sinkRef)
            throws Exception {
        String[] columnNames = new String[] {"a", "b", "c"};
        String[] partitionColumns = partition ? new String[] {"c"} : new String[0];

        TableMetaStoreFactory msFactory =
                new FileSystemCommitterTest.TestMetaStoreFactory(new Path(outputPath.toString()));
        FileSystemOutputFormat<Row> sink =
                new FileSystemOutputFormat.Builder<Row>()
                        .setMetaStoreFactory(msFactory)
                        .setOutputFileConfig(
                                OutputFileConfig.builder()
                                        .withPartPrefix("part-" + UUID.randomUUID())
                                        .build())
                        .setTempPath(stagingDir)
                        .setOverwrite(override)
                        .setPartitionColumns(partitionColumns)
                        .setPartitionComputer(
                                new RowPartitionComputer("default", columnNames, partitionColumns))
                        .setFormatFactory(TextOutputFormat::new)
                        .setDynamicGrouped(dynamicGrouped)
                        .setStaticPartitions(staticPartitions)
                        .build();

        sinkRef.set(sink);

        return new OneInputStreamOperatorTestHarness<>(
                new StreamSink<>(new OutputFormatSinkFunction<>(sink)),
                // test parallelism
                3,
                3,
                0);
    }

    private OneInputStreamOperatorTestHarness<Row, Object> createSink(
            boolean override,
            boolean partition,
            boolean dynamicGrouped,
            LinkedHashMap<String, String> staticPartitions,
            AtomicReference<FileSystemOutputFormat<Row>> sinkRef)
            throws Exception {
        return createSink(
                override,
                partition,
                dynamicGrouped,
                new Path(tmpPath.toString()),
                staticPartitions,
                sinkRef);
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
