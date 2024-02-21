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
 * limitations under the License
 */

package org.apache.flink.state.benchmark;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Supplier;

/** The benchmark of rescaling from checkpoint. */
public class RescalingBenchmark<KEY> {
    private final int maxParallelism;

    private final int parallelismBefore;
    private final int parallelismAfter;

    private final int managedMemorySize;

    private final StateBackend stateBackend;
    private final CheckpointStorageAccess checkpointStorageAccess;

    private OperatorSubtaskState stateForRescaling;
    private OperatorSubtaskState stateForSubtask;
    private KeyedOneInputStreamOperatorTestHarness subtaskHarness;

    private final StreamRecordGenerator<KEY> streamRecordGenerator;
    private final Supplier<KeyedProcessFunction<KEY, KEY, Void>> stateProcessFunctionSupplier;

    public RescalingBenchmark(
            final int parallelismBefore,
            final int parallelismAfter,
            final int maxParallelism,
            final int managedMemorySize,
            final StateBackend stateBackend,
            final CheckpointStorageAccess checkpointStorageAccess,
            final StreamRecordGenerator<KEY> streamRecordGenerator,
            final Supplier<KeyedProcessFunction<KEY, KEY, Void>> stateProcessFunctionSupplier) {
        this.parallelismBefore = parallelismBefore;
        this.parallelismAfter = parallelismAfter;
        this.maxParallelism = maxParallelism;
        this.managedMemorySize = managedMemorySize;
        this.stateBackend = stateBackend;
        this.checkpointStorageAccess = checkpointStorageAccess;
        this.streamRecordGenerator = streamRecordGenerator;
        this.stateProcessFunctionSupplier = stateProcessFunctionSupplier;
    }

    public void setUp() throws Exception {
        stateForRescaling = prepareState();
    }

    public void tearDown() throws IOException {
        stateForRescaling.discardState();
    }

    /** rescaling on one subtask, this is the benchmark entrance. */
    public void rescale() throws Exception {
        subtaskHarness.initializeState(stateForSubtask);
    }

    /** close operator of one subtask. */
    public void closeOperator() throws Exception {
        subtaskHarness.close();
    }

    /** prepare state for operator of one subtask. */
    public void prepareStateForOperator(int subtaskIndex) throws Exception {
        stateForSubtask =
                AbstractStreamOperatorTestHarness.repartitionOperatorState(
                        stateForRescaling,
                        maxParallelism,
                        parallelismBefore,
                        parallelismAfter,
                        subtaskIndex);
        subtaskHarness = getTestHarness(x -> x, maxParallelism, parallelismAfter, subtaskIndex);
        subtaskHarness.setStateBackend(stateBackend);
        subtaskHarness.setup();
    }

    private OperatorSubtaskState prepareState() throws Exception {

        KeyedOneInputStreamOperatorTestHarness<KEY, KEY, Void>[] harnessBefore =
                new KeyedOneInputStreamOperatorTestHarness[parallelismBefore];
        try {
            for (int i = 0; i < parallelismBefore; i++) {

                harnessBefore[i] = getTestHarness(x -> x, maxParallelism, parallelismBefore, i);
                harnessBefore[i].setStateBackend(stateBackend);
                harnessBefore[i].setup();
                harnessBefore[i].open();
            }

            Iterator<StreamRecord<KEY>> iterator = streamRecordGenerator.generate();
            while (iterator.hasNext()) {
                StreamRecord<KEY> next = iterator.next();
                int keyGroupIndex =
                        KeyGroupRangeAssignment.assignToKeyGroup(next.getValue(), maxParallelism);
                int subtaskIndex =
                        KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                maxParallelism, parallelismBefore, keyGroupIndex);
                harnessBefore[subtaskIndex].processElement(next);
            }

            OperatorSubtaskState[] subtaskState = new OperatorSubtaskState[parallelismBefore];

            for (int i = 0; i < parallelismBefore; i++) {
                subtaskState[i] = harnessBefore[i].snapshot(0, 1);
            }
            return AbstractStreamOperatorTestHarness.repackageState(subtaskState);
        } finally {
            closeHarnessArray(harnessBefore);
        }
    }

    private KeyedOneInputStreamOperatorTestHarness<KEY, KEY, Void> getTestHarness(
            KeySelector<KEY, KEY> keySelector,
            int maxParallelism,
            int taskParallelism,
            int subtaskIdx)
            throws Exception {
        MockEnvironment env =
                new MockEnvironmentBuilder()
                        .setTaskName("RescalingTask")
                        .setManagedMemorySize(managedMemorySize)
                        .setMaxParallelism(maxParallelism)
                        .setParallelism(taskParallelism)
                        .setSubtaskIndex(subtaskIdx)
                        .build();
        env.setCheckpointStorageAccess(checkpointStorageAccess);
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(stateProcessFunctionSupplier.get()),
                keySelector,
                streamRecordGenerator.getTypeInformation(),
                env);
    }

    private void closeHarnessArray(KeyedOneInputStreamOperatorTestHarness<?, ?, ?>[] harnessArr)
            throws Exception {
        for (KeyedOneInputStreamOperatorTestHarness<?, ?, ?> harness : harnessArr) {
            if (harness != null) {
                harness.close();
            }
        }
    }

    /** To use RescalingBenchmark, need to implement StreamRecordGenerator. */
    public interface StreamRecordGenerator<T> {
        Iterator<StreamRecord<T>> generate();

        TypeInformation<T> getTypeInformation();
    }
}
