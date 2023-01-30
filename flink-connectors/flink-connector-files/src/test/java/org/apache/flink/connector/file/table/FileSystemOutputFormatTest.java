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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

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

    private void writeUnorderedRecords(OneInputStreamOperatorTestHarness<Row, Object> testHarness)
            throws Exception {
        testHarness.setup();
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(Row.of("a1", 1, "p1"), 1L));
        testHarness.processElement(new StreamRecord<>(Row.of("a2", 2, "p1"), 1L));
        testHarness.processElement(new StreamRecord<>(Row.of("a2", 2, "p2"), 1L));
        testHarness.processElement(new StreamRecord<>(Row.of("a3", 3, "p1"), 1L));
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
                        .setTempPath(new Path(tmpPath.toString()))
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
