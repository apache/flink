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

package org.apache.flink.connector.file.table.stream.compact;

import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CompactionUnit;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorInput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorOutput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.EndCheckpoint;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.EndCompaction;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.InputFile;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompactCoordinator}. */
class CompactCoordinatorTest extends AbstractCompactTestBase {

    @Test
    void testCoordinatorCrossCheckpoints() throws Exception {
        AtomicReference<OperatorSubtaskState> state = new AtomicReference<>();
        runCoordinator(
                harness -> {
                    harness.setup();
                    harness.open();

                    harness.processElement(new InputFile("p0", newFile("f0", 3)), 0);
                    harness.processElement(new InputFile("p0", newFile("f1", 2)), 0);

                    harness.processElement(new InputFile("p1", newFile("f2", 2)), 0);

                    harness.processElement(new InputFile("p0", newFile("f3", 5)), 0);
                    harness.processElement(new InputFile("p0", newFile("f4", 1)), 0);

                    harness.processElement(new InputFile("p1", newFile("f5", 5)), 0);
                    harness.processElement(new InputFile("p1", newFile("f6", 4)), 0);

                    state.set(harness.snapshot(1, 0));
                });

        runCoordinator(
                harness -> {
                    harness.setup();
                    harness.initializeState(state.get());
                    harness.open();

                    harness.processElement(new InputFile("p0", newFile("f7", 3)), 0);
                    harness.processElement(new InputFile("p0", newFile("f8", 2)), 0);

                    state.set(harness.snapshot(2, 0));
                });

        runCoordinator(
                harness -> {
                    harness.setup();
                    harness.initializeState(state.get());
                    harness.open();

                    harness.processElement(new EndCheckpoint(2, 0, 1), 0);

                    List<CoordinatorOutput> outputs = harness.extractOutputValues();

                    assertThat(outputs).hasSize(7);

                    List<CompactionUnit> cp1Units = new ArrayList<>();
                    for (int i = 0; i < 4; i++) {
                        CoordinatorOutput output = outputs.get(i);
                        assertThat(output).isInstanceOf(CompactionUnit.class);
                        cp1Units.add((CompactionUnit) output);
                    }
                    cp1Units.sort(
                            Comparator.comparing(CompactionUnit::getPartition)
                                    .thenComparingInt(CompactionUnit::getUnitId));

                    assertUnit(cp1Units.get(0), 0, "p0", Arrays.asList("f0", "f1", "f4"));
                    assertUnit(cp1Units.get(1), 1, "p0", Collections.singletonList("f3"));
                    assertUnit(cp1Units.get(2), 2, "p1", Arrays.asList("f2", "f5"));
                    assertUnit(cp1Units.get(3), 3, "p1", Collections.singletonList("f6"));

                    assertEndCompaction(outputs.get(4), 1);

                    assertUnit(outputs.get(5), 0, "p0", Arrays.asList("f7", "f8"));

                    assertEndCompaction(outputs.get(6), 2);
                });
    }

    private void runCoordinator(
            ThrowingConsumer<
                            OneInputStreamOperatorTestHarness<CoordinatorInput, CoordinatorOutput>,
                            Exception>
                    consumer)
            throws Exception {
        CompactCoordinator coordinator =
                new CompactCoordinator(() -> folder.getFileSystem(), TARGET_SIZE);
        try (OneInputStreamOperatorTestHarness<CoordinatorInput, CoordinatorOutput> harness =
                new OneInputStreamOperatorTestHarness<>(coordinator)) {
            consumer.accept(harness);
        }
    }

    private void assertEndCompaction(CoordinatorOutput output, long checkpointId) {
        assertThat(output).isInstanceOf(EndCompaction.class);
        EndCompaction end = (EndCompaction) output;

        assertThat(end.getCheckpointId()).isEqualTo(checkpointId);
    }

    private void assertUnit(
            CoordinatorOutput output, int unitId, String partition, List<String> fileNames) {
        assertThat(output).isInstanceOf(CompactionUnit.class);
        CompactionUnit unit = (CompactionUnit) output;

        assertThat(unit.getUnitId()).isEqualTo(unitId);
        assertThat(unit.getPartition()).isEqualTo(partition);
        assertThat(unit.getPaths().stream().map(Path::getName))
                .containsExactlyElementsOf(fileNames);
    }
}
