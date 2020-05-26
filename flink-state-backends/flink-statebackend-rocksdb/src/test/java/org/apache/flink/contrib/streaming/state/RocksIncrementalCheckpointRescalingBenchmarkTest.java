/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.testutils.junit.RetryOnFailure;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collection;
import java.util.List;

/** Test runs the benchmark for incremental checkpoint rescaling. */
public class RocksIncrementalCheckpointRescalingBenchmarkTest extends TestLogger {

    @Rule public TemporaryFolder rootFolder = new TemporaryFolder();

    private static final int maxParallelism = 10;

    private static final int recordCount = 1_000;

    /** partitionParallelism is the parallelism to use for creating the partitionedSnapshot. */
    private static final int partitionParallelism = 2;

    /**
     * repartitionParallelism is the parallelism to use during the test for the repartition step.
     *
     * <p>NOTE: To trigger {@link
     * org.apache.flink.contrib.streaming.state.restore.RocksDBIncrementalRestoreOperation#restoreWithRescaling(Collection)},
     * where the improvement code is exercised, the target parallelism must not be divisible by
     * {@link partitionParallelism}. If this parallelism was instead 4, then there is no rescale.
     */
    private static final int repartitionParallelism = 3;

    /** partitionedSnapshot is a partitioned incremental RocksDB snapshot. */
    private OperatorSubtaskState partitionedSnapshot;

    private KeySelector<Integer, Integer> keySelector = new TestKeySelector();

    /**
     * The benchmark's preparation will:
     *
     * <ol>
     *   <li>Create a stateful operator and process records to persist state.
     *   <li>Snapshot the state and re-partition it so the test operates on a partitioned state.
     * </ol>
     *
     * @throws Exception
     */
    @Before
    public void before() throws Exception {
        OperatorSubtaskState snapshot;
        // Initialize the test harness with a a task parallelism of 1.
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> harness =
                getHarnessTest(keySelector, maxParallelism, 1, 0)) {
            // Set the state backend of the harness to RocksDB.
            harness.setStateBackend(getStateBackend());
            // Initialize the harness.
            harness.open();
            // Push the test records into the operator to trigger state updates.
            Integer[] records = new Integer[recordCount];
            for (int i = 0; i < recordCount; i++) {
                harness.processElement(new StreamRecord<>(i, 1));
            }
            // Grab a snapshot of the state.
            snapshot = harness.snapshot(0, 0);
        }

        // Now, re-partition to create a partitioned state.
        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer>[] partitionedTestHarness =
                new KeyedOneInputStreamOperatorTestHarness[partitionParallelism];
        List<KeyGroupRange> keyGroupPartitions =
                StateAssignmentOperation.createKeyGroupPartitions(
                        maxParallelism, partitionParallelism);
        try {
            for (int i = 0; i < partitionParallelism; i++) {
                // Initialize, open, and then re-snapshot the two subtasks to create a partitioned
                // incremental RocksDB snapshot.
                OperatorSubtaskState subtaskState =
                        AbstractStreamOperatorTestHarness.repartitionOperatorState(
                                snapshot, maxParallelism, 1, partitionParallelism, i);
                KeyGroupRange localKeyGroupRange20 = keyGroupPartitions.get(i);

                partitionedTestHarness[i] =
                        getHarnessTest(keySelector, maxParallelism, partitionParallelism, i);
                partitionedTestHarness[i].setStateBackend(getStateBackend());
                partitionedTestHarness[i].setup();
                partitionedTestHarness[i].initializeState(subtaskState);
                partitionedTestHarness[i].open();
            }

            partitionedSnapshot =
                    AbstractStreamOperatorTestHarness.repackageState(
                            partitionedTestHarness[0].snapshot(1, 2),
                            partitionedTestHarness[1].snapshot(1, 2));

        } finally {
            closeHarness(partitionedTestHarness);
        }
    }

    @Test(timeout = 1000)
    @RetryOnFailure(times = 3)
    public void benchmarkScalingUp() throws Exception {
        long benchmarkTime = 0;

        // Trigger the incremental re-scaling via restoreWithRescaling by repartitioning it from
        // parallelism of >1 to a higher parallelism. Time spent during this step includes the cost
        // of incremental rescaling.
        List<KeyGroupRange> keyGroupPartitions =
                StateAssignmentOperation.createKeyGroupPartitions(
                        maxParallelism, repartitionParallelism);

        long fullStateSize = partitionedSnapshot.getStateSize();

        for (int i = 0; i < repartitionParallelism; i++) {
            OperatorSubtaskState subtaskState =
                    AbstractStreamOperatorTestHarness.repartitionOperatorState(
                            partitionedSnapshot,
                            maxParallelism,
                            partitionParallelism,
                            repartitionParallelism,
                            i);
            KeyGroupRange localKeyGroupRange20 = keyGroupPartitions.get(i);

            try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> subtaskHarness =
                    getHarnessTest(keySelector, maxParallelism, repartitionParallelism, i)) {
                RocksDBStateBackend backend = getStateBackend();
                subtaskHarness.setStateBackend(backend);
                subtaskHarness.setup();

                // Precisely measure the call-site that triggers the restore operation.
                long startingTime = System.nanoTime();
                subtaskHarness.initializeState(subtaskState);
                benchmarkTime += System.nanoTime() - startingTime;
            }
        }

        log.error(
                "--------------> performance for incremental checkpoint re-scaling <--------------");
        log.error(
                "rescale from {} to {} with {} records took: {} nanoseconds",
                partitionParallelism,
                repartitionParallelism,
                recordCount,
                benchmarkTime);
    }

    private KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> getHarnessTest(
            KeySelector<Integer, Integer> keySelector,
            int maxParallelism,
            int taskParallelism,
            int subtaskIdx)
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new TestKeyedFunction()),
                keySelector,
                BasicTypeInfo.INT_TYPE_INFO,
                maxParallelism,
                taskParallelism,
                subtaskIdx);
    }

    private void closeHarness(KeyedOneInputStreamOperatorTestHarness<?, ?, ?>[] harnessArr)
            throws Exception {
        for (KeyedOneInputStreamOperatorTestHarness<?, ?, ?> harness : harnessArr) {
            if (harness != null) {
                harness.close();
            }
        }
    }

    private RocksDBStateBackend getStateBackend() throws Exception {
        return new RocksDBStateBackend("file://" + rootFolder.newFolder().getAbsolutePath(), true);
    }

    /** A simple keyed function for tests. */
    private class TestKeyedFunction extends KeyedProcessFunction<Integer, Integer, Integer> {

        public ValueStateDescriptor<Integer> stateDescriptor;
        private ValueState<Integer> counterState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            stateDescriptor = new ValueStateDescriptor<Integer>("counter", Integer.class);
            counterState = this.getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(Integer incomingValue, Context ctx, Collector<Integer> out)
                throws Exception {
            Integer oldValue = counterState.value();
            Integer newValue = oldValue != null ? oldValue + incomingValue : incomingValue;
            counterState.update(newValue);
            out.collect(newValue);
        }
    }

    /** A simple key selector for tests. */
    private class TestKeySelector implements KeySelector<Integer, Integer> {
        @Override
        public Integer getKey(Integer value) throws Exception {
            return value;
        }
    }
}
