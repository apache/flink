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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/** Tests for the {@link org.apache.flink.runtime.state.OperatorStateRestoreOperation}. */
public class OperatorStateRestoreOperationTest {

    @Nullable
    private static OperatorStateHandle createOperatorStateHandle(
            ExecutionConfig cfg,
            CloseableRegistry cancelStreamRegistry,
            ClassLoader classLoader,
            List<String> stateNames,
            List<String> broadcastStateNames)
            throws Exception {

        try (OperatorStateBackend operatorStateBackend =
                new DefaultOperatorStateBackendBuilder(
                                classLoader,
                                cfg,
                                false,
                                Collections.emptyList(),
                                cancelStreamRegistry)
                        .build()) {
            CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(4096);

            for (String stateName : stateNames) {
                ListStateDescriptor<String> descriptor =
                        new ListStateDescriptor<>(stateName, String.class);
                PartitionableListState<String> state =
                        (PartitionableListState<String>)
                                operatorStateBackend.getListState(descriptor);
                state.add("value1");
            }

            for (String broadcastStateName : broadcastStateNames) {
                MapStateDescriptor<String, String> descriptor =
                        new MapStateDescriptor<>(broadcastStateName, String.class, String.class);
                BroadcastState<String, String> state =
                        operatorStateBackend.getBroadcastState(descriptor);
                state.put("key1", "value1");
            }

            SnapshotResult<OperatorStateHandle> result =
                    operatorStateBackend
                            .snapshot(
                                    1,
                                    1,
                                    streamFactory,
                                    CheckpointOptions.forCheckpointWithDefaultLocation())
                            .get();
            return result.getJobManagerOwnedSnapshot();
        }
    }

    @Test
    public void testRestoringMixedOperatorStateWhenSnapshotCompressionIsEnabled() throws Exception {
        ExecutionConfig cfg = new ExecutionConfig();
        cfg.setUseSnapshotCompression(true);
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        ClassLoader classLoader = this.getClass().getClassLoader();

        OperatorStateHandle handle =
                createOperatorStateHandle(
                        cfg,
                        cancelStreamRegistry,
                        classLoader,
                        Arrays.asList("s1", "s2"),
                        Collections.singletonList("b2"));

        OperatorStateRestoreOperation operatorStateRestoreOperation =
                new OperatorStateRestoreOperation(
                        cancelStreamRegistry,
                        classLoader,
                        new HashMap<>(),
                        new HashMap<>(),
                        Collections.singletonList(handle));

        operatorStateRestoreOperation.restore();
    }
}
