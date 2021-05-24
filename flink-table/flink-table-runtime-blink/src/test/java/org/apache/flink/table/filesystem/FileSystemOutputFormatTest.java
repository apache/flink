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

package org.apache.flink.table.filesystem;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.utils.LegacyRowResource;
import org.apache.flink.types.Row;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** Test for {@link FileSystemOutputFormat}. */
public class FileSystemOutputFormatTest {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    private File tmpFile;
    private File outputFile;

    private static Map<File, String> getFileContentByPath(File directory) throws IOException {
        Map<File, String> contents = new HashMap<>(4);

        if (!directory.exists() || !directory.isDirectory()) {
            return contents;
        }

        final Collection<File> filesInBucket = FileUtils.listFiles(directory, null, true);
        for (File file : filesInBucket) {
            contents.put(file, FileUtils.readFileToString(file));
        }
        return contents;
    }

    @Rule public final LegacyRowResource usesLegacyRows = LegacyRowResource.INSTANCE;

    @Before
    public void before() throws IOException {
        tmpFile = TEMP_FOLDER.newFolder();
        outputFile = TEMP_FOLDER.newFolder();
    }

    @Test
    public void testClosingWithoutInput() throws Exception {
        try (OneInputStreamOperatorTestHarness<Row, Object> testHarness =
                createSink(false, false, false, new LinkedHashMap<>(), new AtomicReference<>())) {
            testHarness.setup();
            testHarness.open();
        }
    }

    @Test
    public void testNonPartition() throws Exception {
        AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
        try (OneInputStreamOperatorTestHarness<Row, Object> testHarness =
                createSink(false, false, false, new LinkedHashMap<>(), ref)) {
            writeUnorderedRecords(testHarness);
            assertEquals(1, getFileContentByPath(new File(tmpFile, "cp-0")).size());
        }

        ref.get().finalizeGlobal(1);
        Map<File, String> content = getFileContentByPath(outputFile);
        assertEquals(1, content.size());
        assertEquals(
                "a1,1,p1\n" + "a2,2,p1\n" + "a2,2,p2\n" + "a3,3,p1\n",
                content.values().iterator().next());
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
    public void testOverrideNonPartition() throws Exception {
        testNonPartition();

        AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
        try (OneInputStreamOperatorTestHarness<Row, Object> testHarness =
                createSink(true, false, false, new LinkedHashMap<>(), ref)) {
            writeUnorderedRecords(testHarness);
            assertEquals(1, getFileContentByPath(new File(tmpFile, "cp-0")).size());
        }

        ref.get().finalizeGlobal(1);
        Map<File, String> content = getFileContentByPath(outputFile);
        assertEquals(1, content.size());
        assertEquals(
                "a1,1,p1\n" + "a2,2,p1\n" + "a2,2,p2\n" + "a3,3,p1\n",
                content.values().iterator().next());
        assertFalse(new File(tmpFile.toURI()).exists());
    }

    @Test
    public void testStaticPartition() throws Exception {
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
            assertEquals(1, getFileContentByPath(new File(tmpFile, "cp-0")).size());
        }

        ref.get().finalizeGlobal(1);
        Map<File, String> content = getFileContentByPath(outputFile);
        assertEquals(1, content.size());
        assertEquals("c=p1", content.keySet().iterator().next().getParentFile().getName());
        assertEquals("a1,1\n" + "a2,2\n" + "a2,2\n" + "a3,3\n", content.values().iterator().next());
        assertFalse(new File(tmpFile.toURI()).exists());
    }

    @Test
    public void testDynamicPartition() throws Exception {
        AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
        try (OneInputStreamOperatorTestHarness<Row, Object> testHarness =
                createSink(false, true, false, new LinkedHashMap<>(), ref)) {
            writeUnorderedRecords(testHarness);
            assertEquals(2, getFileContentByPath(new File(tmpFile, "cp-0")).size());
        }

        ref.get().finalizeGlobal(1);
        Map<File, String> content = getFileContentByPath(outputFile);
        Map<String, String> sortedContent = new TreeMap<>();
        content.forEach((file, s) -> sortedContent.put(file.getParentFile().getName(), s));

        assertEquals(2, sortedContent.size());
        assertEquals("a1,1\n" + "a2,2\n" + "a3,3\n", sortedContent.get("c=p1"));
        assertEquals("a2,2\n", sortedContent.get("c=p2"));
        assertFalse(new File(tmpFile.toURI()).exists());
    }

    @Test
    public void testGroupedDynamicPartition() throws Exception {
        AtomicReference<FileSystemOutputFormat<Row>> ref = new AtomicReference<>();
        try (OneInputStreamOperatorTestHarness<Row, Object> testHarness =
                createSink(false, true, true, new LinkedHashMap<>(), ref)) {
            testHarness.setup();
            testHarness.open();

            testHarness.processElement(new StreamRecord<>(Row.of("a1", 1, "p1"), 1L));
            testHarness.processElement(new StreamRecord<>(Row.of("a2", 2, "p1"), 1L));
            testHarness.processElement(new StreamRecord<>(Row.of("a3", 3, "p1"), 1L));
            testHarness.processElement(new StreamRecord<>(Row.of("a2", 2, "p2"), 1L));
            assertEquals(2, getFileContentByPath(new File(tmpFile, "cp-0")).size());
        }

        ref.get().finalizeGlobal(1);
        Map<File, String> content = getFileContentByPath(outputFile);
        Map<String, String> sortedContent = new TreeMap<>();
        content.forEach((file, s) -> sortedContent.put(file.getParentFile().getName(), s));

        assertEquals(2, sortedContent.size());
        assertEquals("a1,1\n" + "a2,2\n" + "a3,3\n", sortedContent.get("c=p1"));
        assertEquals("a2,2\n", sortedContent.get("c=p2"));
        assertFalse(new File(tmpFile.toURI()).exists());
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
                new FileSystemCommitterTest.TestMetaStoreFactory(new Path(outputFile.getPath()));
        FileSystemOutputFormat<Row> sink =
                new FileSystemOutputFormat.Builder<Row>()
                        .setMetaStoreFactory(msFactory)
                        .setTempPath(new Path(tmpFile.getPath()))
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
}
