/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.operators.lifecycle.TestJobExecutor;
import org.apache.flink.runtime.operators.lifecycle.TestJobWithDescription;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointCompletedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointStartedEvent;
import org.apache.flink.runtime.operators.lifecycle.graph.TestJobBuilders;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackend.MockSnapshotSupplier;
import org.apache.flink.runtime.state.ttl.mock.MockStateBackend;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.SharedObjects;

import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.UUID.randomUUID;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.FINISH_SOURCES;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher.TestCommandScope.ALL_SUBTASKS;
import static org.apache.flink.runtime.state.KeyGroupRange.EMPTY_KEY_GROUP_RANGE;

/**
 * Verifies that using the same {@link KeyedStateHandle} object is possible for {@link
 * org.apache.flink.runtime.state.KeyedStateBackend backends}. This relies on (de)serializing
 * handles while sending via RPC. Without it, some validation might fail in {@link
 * org.apache.flink.runtime.checkpoint.CheckpointCoordinator} which doesn't expect to receive the
 * same object.
 */
public class StateHandleReuseITCase extends AbstractTestBase {
    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Test
    public void runTest() throws Exception {
        TestJobExecutor.execute(buildJob(), MINI_CLUSTER_RESOURCE)
                // register once: should succeed
                .waitForEvent(CheckpointCompletedEvent.class)
                // register again: might fail without serialization
                .waitForEvent(CheckpointStartedEvent.class)
                .waitForEvent(CheckpointCompletedEvent.class)
                .sendBroadcastCommand(FINISH_SOURCES, ALL_SUBTASKS)
                .waitForTermination()
                .assertFinishedSuccessfully();
    }

    private TestJobWithDescription buildJob() throws Exception {
        return TestJobBuilders.COMPLEX_GRAPH_BUILDER.build(
                sharedObjects,
                cfg -> {},
                env -> {
                    env.setParallelism(1);
                    env.enableCheckpointing(10);
                    env.setRestartStrategy(RestartStrategies.noRestart());
                    env.setStateBackend(new MockStateBackend(new SingleHandleSnapshotSupplier()));
                    // changelog backend doesn't work with the mock backend
                    env.enableChangelogStateBackend(false);
                });
    }

    /** {@link MockSnapshotSupplier} that always sends the same handle (object). */
    private static class SingleHandleSnapshotSupplier implements MockSnapshotSupplier {
        private static final long serialVersionUID = 1L;
        private static final IncrementalRemoteKeyedStateHandle handle =
                new IncrementalRemoteKeyedStateHandle(
                        randomUUID(),
                        EMPTY_KEY_GROUP_RANGE,
                        1L,
                        emptyList(),
                        emptyList(),
                        new ByteStreamStateHandle("meta", new byte[] {0}),
                        0L);

        @Override
        public <K> SnapshotResult<KeyedStateHandle> snapshot(
                Map<String, Map<K, Map<Object, Object>>> stateValues,
                Map<String, StateSnapshotTransformer<Object>> stateSnapshotFilters) {
            return SnapshotResult.of(handle);
        }
    }
}
