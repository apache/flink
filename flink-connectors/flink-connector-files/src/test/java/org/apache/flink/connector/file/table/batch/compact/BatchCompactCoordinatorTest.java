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
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CompactionUnit;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorInput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorOutput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.InputFile;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for batch compact coordinator. */
public class BatchCompactCoordinatorTest extends AbstractCompactTestBase {

    @Test
    public void testCompactIsNotNeeded() throws Exception {
        long averageSize = 5;
        long targetSize = 50;
        BatchCompactCoordinator compactCoordinator =
                new BatchCompactCoordinator(() -> folder.getFileSystem(), averageSize, targetSize);

        try (OneInputStreamOperatorTestHarness<CoordinatorInput, CoordinatorOutput> testHarness =
                new OneInputStreamOperatorTestHarness<>(compactCoordinator)) {
            testHarness.setup();
            testHarness.open();

            Path f1 = newFile("f1", 10);
            Path f2 = newFile("f2", 5);
            Path f3 = newFile("f3", 16);

            testHarness.processElement(new StreamRecord<>(new InputFile("", f1)));
            testHarness.processElement(new StreamRecord<>(new InputFile("", f2)));
            testHarness.processElement(new StreamRecord<>(new InputFile("", f3)));
            testHarness.endInput();

            // the file average size is not less than 5, so it's no need to compact, the output
            // compact unit should only contain one file
            assertCompactUnits(
                    testHarness.extractOutputValues(),
                    Arrays.asList(
                            new CompactionUnit(0, "", Collections.singletonList(f1)),
                            new CompactionUnit(1, "", Collections.singletonList(f2)),
                            new CompactionUnit(2, "", Collections.singletonList(f3))));
        }
    }

    @Test
    public void testCompactNonPartitionedTable() throws Exception {
        long averageSize = 14;
        long targetSize = 16;
        BatchCompactCoordinator compactCoordinator =
                new BatchCompactCoordinator(() -> folder.getFileSystem(), averageSize, targetSize);
        try (OneInputStreamOperatorTestHarness<CoordinatorInput, CoordinatorOutput> testHarness =
                new OneInputStreamOperatorTestHarness<>(compactCoordinator)) {
            testHarness.setup();
            testHarness.open();

            Path f1 = newFile("f1", 10);
            Path f2 = newFile("f2", 5);
            Path f3 = newFile("f3", 20);

            testHarness.processElement(new StreamRecord<>(new InputFile("", f1)));
            testHarness.processElement(new StreamRecord<>(new InputFile("", f2)));
            testHarness.processElement(new StreamRecord<>(new InputFile("", f3)));
            testHarness.endInput();

            List<CoordinatorOutput> coordinatorOutputs = testHarness.extractOutputValues();
            // f1 + f2 should be merged
            assertCompactUnits(
                    coordinatorOutputs,
                    Arrays.asList(
                            new CompactionUnit(0, "", Arrays.asList(f1, f2)),
                            new CompactionUnit(1, "", Collections.singletonList(f3))));
        }
    }

    @Test
    public void testCompactPartitionedTable() throws Exception {
        long averageSize = 10;
        long targetSize = 16;
        BatchCompactCoordinator compactCoordinator =
                new BatchCompactCoordinator(() -> folder.getFileSystem(), averageSize, targetSize);

        try (OneInputStreamOperatorTestHarness<CoordinatorInput, CoordinatorOutput> testHarness =
                new OneInputStreamOperatorTestHarness<>(compactCoordinator)) {
            testHarness.setup();
            testHarness.open();

            // the files for partition "p1=1/", average size is 7.5, should be merged
            Path f1 = newFile("f1", 10);
            Path f2 = newFile("f2", 5);

            // the files for partition "p1=2/", average size is 14, shouldn't be merged
            Path f3 = newFile("f3", 20);
            Path f4 = newFile("f4", 8);

            // partition "p1=1/" should be merged
            testHarness.processElement(new StreamRecord<>(new InputFile("p1=1/", f1)));
            testHarness.processElement(new StreamRecord<>(new InputFile("p1=1/", f2)));

            // partition "p1=2/" should be merged
            testHarness.processElement(new StreamRecord<>(new InputFile("p1=2/", f3)));
            testHarness.processElement(new StreamRecord<>(new InputFile("p1=2/", f4)));

            testHarness.endInput();

            List<CoordinatorOutput> coordinatorOutputs = testHarness.extractOutputValues();
            // f1, f2 should be packed to compact unit
            // f3/f4 is a single compact unit
            assertCompactUnits(
                    coordinatorOutputs,
                    Arrays.asList(
                            new CompactionUnit(0, "p1=1/", Arrays.asList(f1, f2)),
                            new CompactionUnit(1, "p1=2/", Collections.singletonList(f3)),
                            new CompactionUnit(2, "p1=2/", Collections.singletonList(f4))));
        }
    }

    private void assertCompactUnits(
            List<CoordinatorOutput> coordinatorOutputs,
            List<CompactionUnit> expectCompactionUnits) {
        assertThat(coordinatorOutputs.size()).isEqualTo(expectCompactionUnits.size());
        coordinatorOutputs.sort(Comparator.comparing(o -> ((CompactionUnit) o).getPartition()));
        expectCompactionUnits.sort(Comparator.comparing(CompactionUnit::getPartition));
        for (int i = 0; i < coordinatorOutputs.size(); i++) {
            CoordinatorOutput coordinatorOutput = coordinatorOutputs.get(i);
            assertThat(coordinatorOutput).isInstanceOf(CompactionUnit.class);

            CompactionUnit compactionUnit = (CompactionUnit) coordinatorOutput;
            CompactionUnit expectCompactionUnit = expectCompactionUnits.get(i);

            // assert compact unit is equal
            assertThat(compactionUnit.getPartition())
                    .isEqualTo(expectCompactionUnit.getPartition());
            assertThat(compactionUnit.getPaths()).isEqualTo(expectCompactionUnit.getPaths());
        }
    }
}
