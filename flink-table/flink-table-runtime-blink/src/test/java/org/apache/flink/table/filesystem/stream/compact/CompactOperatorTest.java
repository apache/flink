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

package org.apache.flink.table.filesystem.stream.compact;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.filesystem.stream.PartitionCommitInfo;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.CompactionUnit;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.CoordinatorOutput;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.EndCompaction;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/** Test for {@link CompactOperator}. */
public class CompactOperatorTest extends AbstractCompactTestBase {

    @Test
    public void testCompactOperator() throws Exception {
        AtomicReference<OperatorSubtaskState> state = new AtomicReference<>();
        Path f0 = newFile(".uncompacted-f0", 3);
        Path f1 = newFile(".uncompacted-f1", 2);
        Path f2 = newFile(".uncompacted-f2", 2);
        Path f3 = newFile(".uncompacted-f3", 5);
        Path f4 = newFile(".uncompacted-f4", 1);
        Path f5 = newFile(".uncompacted-f5", 5);
        Path f6 = newFile(".uncompacted-f6", 4);
        FileSystem fs = f0.getFileSystem();
        runCompact(
                harness -> {
                    harness.setup();
                    harness.open();

                    harness.processElement(
                            new CompactionUnit(0, "p0", Arrays.asList(f0, f1, f4)), 0);
                    harness.processElement(
                            new CompactionUnit(1, "p0", Collections.singletonList(f3)), 0);
                    harness.processElement(new CompactionUnit(2, "p1", Arrays.asList(f2, f5)), 0);
                    harness.processElement(
                            new CompactionUnit(3, "p0", Collections.singletonList(f6)), 0);

                    harness.processElement(new EndCompaction(1), 0);

                    state.set(harness.snapshot(2, 0));

                    // check output commit info
                    List<PartitionCommitInfo> outputs = harness.extractOutputValues();
                    Assert.assertEquals(1, outputs.size());
                    Assert.assertEquals(1, outputs.get(0).getCheckpointId());
                    Assert.assertEquals(Arrays.asList("p0", "p1"), outputs.get(0).getPartitions());

                    // check all compacted file generated
                    Assert.assertTrue(fs.exists(new Path(folder, "compacted-f0")));
                    Assert.assertTrue(fs.exists(new Path(folder, "compacted-f2")));
                    Assert.assertTrue(fs.exists(new Path(folder, "compacted-f3")));
                    Assert.assertTrue(fs.exists(new Path(folder, "compacted-f6")));

                    // check one compacted file
                    byte[] bytes =
                            FileUtils.readAllBytes(
                                    new File(folder.getPath(), "compacted-f0").toPath());
                    Arrays.sort(bytes);
                    Assert.assertArrayEquals(new byte[] {0, 0, 0, 1, 1, 2}, bytes);
                });

        runCompact(
                harness -> {
                    harness.setup();
                    harness.initializeState(state.get());
                    harness.open();

                    harness.notifyOfCompletedCheckpoint(2);

                    // check all temp files have been deleted
                    Assert.assertFalse(fs.exists(f0));
                    Assert.assertFalse(fs.exists(f1));
                    Assert.assertFalse(fs.exists(f2));
                    Assert.assertFalse(fs.exists(f3));
                    Assert.assertFalse(fs.exists(f4));
                    Assert.assertFalse(fs.exists(f5));
                    Assert.assertFalse(fs.exists(f6));
                });
    }

    @Test
    public void testEndInput() throws Exception {
        Path f0 = newFile(".uncompacted-f0", 3);
        Path f1 = newFile(".uncompacted-f1", 4);
        Path f2 = newFile(".uncompacted-f2", 2);

        FileSystem fs = f0.getFileSystem();

        runCompact(
                harness -> {
                    harness.setup();
                    harness.open();

                    harness.processElement(new CompactionUnit(0, "p0", Arrays.asList(f0, f1)), 0);
                    harness.processElement(
                            new CompactionUnit(1, "p0", Collections.singletonList(f2)), 0);

                    // test without snapshot
                    harness.endInput();

                    // check all compacted file generated
                    Assert.assertTrue(fs.exists(new Path(folder, "compacted-f0")));
                    Assert.assertTrue(fs.exists(new Path(folder, "compacted-f2")));

                    // check all temp files have been deleted
                    Assert.assertFalse(fs.exists(f0));
                    Assert.assertFalse(fs.exists(f1));
                    Assert.assertFalse(fs.exists(f2));
                });
    }

    @Test
    public void testUnitSelection() throws Exception {
        OneInputStreamOperatorTestHarness<CoordinatorOutput, PartitionCommitInfo> harness0 =
                create(2, 0);
        harness0.setup();
        harness0.open();

        OneInputStreamOperatorTestHarness<CoordinatorOutput, PartitionCommitInfo> harness1 =
                create(2, 1);
        harness1.setup();
        harness1.open();

        Path f0 = newFile(".uncompacted-f0", 3);
        Path f1 = newFile(".uncompacted-f1", 2);
        Path f2 = newFile(".uncompacted-f2", 2);
        Path f3 = newFile(".uncompacted-f3", 5);
        Path f4 = newFile(".uncompacted-f4", 1);
        Path f5 = newFile(".uncompacted-f5", 5);
        Path f6 = newFile(".uncompacted-f6", 4);
        FileSystem fs = f0.getFileSystem();

        // broadcast
        harness0.processElement(new CompactionUnit(0, "p0", Arrays.asList(f0, f1, f4)), 0);
        harness0.processElement(new CompactionUnit(1, "p0", Collections.singletonList(f3)), 0);
        harness0.processElement(new CompactionUnit(2, "p0", Arrays.asList(f2, f5)), 0);
        harness0.processElement(new CompactionUnit(3, "p0", Collections.singletonList(f6)), 0);

        harness0.processElement(new EndCompaction(1), 0);

        // check compacted file generated
        Assert.assertTrue(fs.exists(new Path(folder, "compacted-f0")));
        Assert.assertTrue(fs.exists(new Path(folder, "compacted-f2")));

        // f3 and f6 are in the charge of another task
        Assert.assertFalse(fs.exists(new Path(folder, "compacted-f3")));
        Assert.assertFalse(fs.exists(new Path(folder, "compacted-f6")));

        harness1.processElement(new CompactionUnit(0, "p0", Arrays.asList(f0, f1, f4)), 0);
        harness1.processElement(new CompactionUnit(1, "p0", Collections.singletonList(f3)), 0);
        harness1.processElement(new CompactionUnit(2, "p0", Arrays.asList(f2, f5)), 0);
        harness1.processElement(new CompactionUnit(3, "p0", Collections.singletonList(f6)), 0);

        harness1.processElement(new EndCompaction(1), 0);

        // check compacted file generated
        Assert.assertTrue(fs.exists(new Path(folder, "compacted-f3")));
        Assert.assertTrue(fs.exists(new Path(folder, "compacted-f6")));

        harness0.close();
        harness1.close();
    }

    private void runCompact(
            ThrowingConsumer<
                            OneInputStreamOperatorTestHarness<
                                    CoordinatorOutput, PartitionCommitInfo>,
                            Exception>
                    consumer)
            throws Exception {
        try (OneInputStreamOperatorTestHarness<CoordinatorOutput, PartitionCommitInfo> harness =
                create(1, 0)) {
            consumer.accept(harness);
        }
    }

    private OneInputStreamOperatorTestHarness<CoordinatorOutput, PartitionCommitInfo> create(
            int parallelism, int subtaskIndex) throws Exception {
        CompactOperator<Byte> operator =
                new CompactOperator<>(
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
        return new OneInputStreamOperatorTestHarness<>(
                operator, parallelism, parallelism, subtaskIndex);
    }
}
