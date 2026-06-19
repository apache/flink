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

import org.apache.flink.connector.file.table.stream.compact.AbstractCompactTestBase;
import org.apache.flink.connector.file.table.stream.compact.CompactBulkReader;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CompactOutput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CompactionUnit;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorOutput;
import org.apache.flink.connector.file.table.stream.compact.CompactWriter;
import org.apache.flink.connector.file.table.stream.compact.TestByteFormat;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BatchCompactOperator}. */
public class BatchCompactOperatorTest extends AbstractCompactTestBase {

    @Test
    public void testCompact() throws Exception {
        // test compact
        BatchCompactOperator<Byte> compactOperator = createBatchCompactOperator();
        try (OneInputStreamOperatorTestHarness<CoordinatorOutput, CompactOutput> testHarness =
                new OneInputStreamOperatorTestHarness<>(compactOperator)) {
            testHarness.setup();
            testHarness.open();

            Path f0 = newFile("uncompacted-f0", 3);
            Path f1 = newFile("uncompacted-f1", 2);
            Path f2 = newFile("uncompacted-f2", 2);

            Path f3 = newFile("uncompacted-f3", 10);

            testHarness.processElement(
                    new StreamRecord<>(new CompactionUnit(1, "p=p1/", Arrays.asList(f0, f1, f2))));
            testHarness.processElement(
                    new StreamRecord<>(
                            new CompactionUnit(2, "p=p2/", Collections.singletonList(f3))));

            testHarness.endInput();
            List<CompactOutput> compactOutputs = testHarness.extractOutputValues();
            Map<String, List<Path>> expectCompactedFiles = new HashMap<>();

            expectCompactedFiles.put(
                    "p=p1/",
                    Collections.singletonList(new Path(folder + "/compacted-attempt-0-f0")));
            // for single file, we won't compact, so the name won't change
            expectCompactedFiles.put(
                    "p=p2/", Collections.singletonList(new Path(folder + "/uncompacted-f3")));

            // check compacted file
            byte[] bytes =
                    FileUtils.readAllBytes(
                            new File(folder.getPath(), "compacted-attempt-0-f0").toPath());
            Arrays.sort(bytes);
            assertThat(bytes).isEqualTo(new byte[] {0, 0, 0, 1, 1, 1, 2});

            bytes = FileUtils.readAllBytes(new File(folder.getPath(), "uncompacted-f3").toPath());
            assertThat(bytes).isEqualTo(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

            assertCompactOutput(
                    compactOutputs,
                    Collections.singletonList(new CompactOutput(expectCompactedFiles)));
        }
    }

    private void assertCompactOutput(
            List<CompactOutput> actualCompactOutputs, List<CompactOutput> expectCompactOutputs) {
        assertThat(actualCompactOutputs.size()).isEqualTo(expectCompactOutputs.size());
        for (int i = 0; i < actualCompactOutputs.size(); i++) {
            CompactOutput actualCompactOutput = actualCompactOutputs.get(i);
            CompactOutput expectCompactOutput = expectCompactOutputs.get(i);
            assertThat(actualCompactOutput.getCompactedFiles())
                    .isEqualTo(expectCompactOutput.getCompactedFiles());
        }
    }

    private BatchCompactOperator<Byte> createBatchCompactOperator() {
        return new BatchCompactOperator<>(
                () -> folder.getFileSystem(),
                CompactBulkReader.factory(TestByteFormat.bulkFormat()),
                context -> {
                    Path path = context.getPath();
                    Path tempPath = new Path(path.getParent(), "." + path.getName());
                    FSDataOutputStream out =
                            context.getFileSystem()
                                    .create(tempPath, FileSystem.WriteMode.OVERWRITE);
                    return new CompactWriter<Byte>() {
                        @Override
                        public void write(Byte record) throws IOException {
                            out.write(record);
                        }

                        @Override
                        public void commit() throws IOException {
                            out.close();
                            context.getFileSystem().rename(tempPath, path);
                        }
                    };
                });
    }
}
