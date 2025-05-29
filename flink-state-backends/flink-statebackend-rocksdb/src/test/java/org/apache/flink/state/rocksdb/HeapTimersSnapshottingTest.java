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

package org.apache.flink.state.rocksdb;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend.PriorityQueueStateType;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * The tests verify that {@link PriorityQueueStateType#HEAP heap timers} are not serialized into raw
 * keyed operator state when taking a savepoint, but they are serialized for checkpoints. The heap
 * timers still need to be serialized into the raw operator state because of RocksDB incremental
 * checkpoints.
 */
public class HeapTimersSnapshottingTest {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testNotSerializingTimersInRawStateForSavepoints() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                getTestHarness()) {
            EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
            backend.setPriorityQueueStateType(PriorityQueueStateType.HEAP);
            testHarness.setStateBackend(backend);
            testHarness.setCheckpointStorage(
                    new FileSystemCheckpointStorage(temporaryFolder.newFolder().toURI()));
            testHarness.open();
            testHarness.processElement(0, 0L);

            OperatorSubtaskState state =
                    testHarness
                            .snapshotWithLocalState(
                                    0L, 1L, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                            .getJobManagerOwnedState();
            assertThat(state.getRawKeyedState().isEmpty(), equalTo(true));
        }
    }

    @Test
    public void testSerializingTimersInRawStateForCheckpoints() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                getTestHarness()) {
            EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
            backend.setPriorityQueueStateType(PriorityQueueStateType.HEAP);
            testHarness.setStateBackend(backend);
            testHarness.setCheckpointStorage(
                    new FileSystemCheckpointStorage(temporaryFolder.newFolder().toURI()));
            testHarness.open();
            testHarness.processElement(0, 0L);

            OperatorSubtaskState state =
                    testHarness
                            .snapshotWithLocalState(0L, 1L, CheckpointType.CHECKPOINT)
                            .getJobManagerOwnedState();
            assertThat(state.getRawKeyedState().isEmpty(), equalTo(false));
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
