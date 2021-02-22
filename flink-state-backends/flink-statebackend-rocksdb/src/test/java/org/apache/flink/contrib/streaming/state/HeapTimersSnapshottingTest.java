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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend.PriorityQueueStateType;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StateMigrationException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;

/**
 * The RocksDB based timers are ignored when restored with RocksDB and heap timers enabled. These
 * tests verify that an exception is thrown when users try to perform that incompatible migration.
 */
public class HeapTimersSnapshottingTest {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testThrowExceptionWhenRestoringRocksTimersWithHeapTimers() throws Exception {
        OperatorSubtaskState state;
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                getTestHarness()) {
            RocksDBStateBackend backend =
                    new RocksDBStateBackend(temporaryFolder.newFolder().toURI());
            backend.setPriorityQueueStateType(PriorityQueueStateType.ROCKSDB);
            testHarness.setStateBackend(backend);
            testHarness.open();
            testHarness.processElement(0, 0L);

            state =
                    testHarness
                            .snapshotWithLocalState(0L, 1L, CheckpointType.SAVEPOINT)
                            .getJobManagerOwnedState();
        }

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                getTestHarness()) {
            RocksDBStateBackend backend =
                    new RocksDBStateBackend(temporaryFolder.newFolder().toURI());
            backend.setPriorityQueueStateType(PriorityQueueStateType.HEAP);
            testHarness.setStateBackend(backend);

            thrown.expect(
                    containsCause(
                            new StateMigrationException(
                                    "Can not restore savepoint taken with RocksDB timers enabled"
                                            + " with Heap timers!")));
            testHarness.initializeState(state);
        }
    }

    @Test
    public void testThrowExceptionWhenRestoringRocksTimersWithHeapTimersIncrementalCheckpoints()
            throws Exception {
        OperatorSubtaskState state;
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                getTestHarness()) {
            RocksDBStateBackend backend =
                    new RocksDBStateBackend(temporaryFolder.newFolder().toURI(), true);
            backend.setPriorityQueueStateType(PriorityQueueStateType.ROCKSDB);
            testHarness.setStateBackend(backend);
            testHarness.open();
            testHarness.processElement(0, 0L);

            state =
                    testHarness
                            .snapshotWithLocalState(0L, 1L, CheckpointType.SAVEPOINT)
                            .getJobManagerOwnedState();
        }

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                getTestHarness()) {
            RocksDBStateBackend backend =
                    new RocksDBStateBackend(temporaryFolder.newFolder().toURI(), true);
            backend.setPriorityQueueStateType(PriorityQueueStateType.HEAP);
            testHarness.setStateBackend(backend);

            thrown.expect(
                    containsCause(
                            new StateMigrationException(
                                    "Can not restore savepoint taken with RocksDB timers enabled"
                                            + " with Heap timers!")));
            testHarness.initializeState(state);
        }
    }

    private KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> getTestHarness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(
                        new KeyedProcessFunction<Integer, Integer, Integer>() {
                            @Override
                            public void processElement(
                                    Integer value, Context ctx, Collector<Integer> out) {
                                ctx.timerService().registerEventTimeTimer(0L);
                            }
                        }),
                (KeySelector<Integer, Integer>) value -> value,
                BasicTypeInfo.INT_TYPE_INFO,
                1,
                1,
                0);
    }
}
