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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.metrics.SizeTrackingStateConfig;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.testutils.StateSchemaEvolvingTestSerializer;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackend;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackend.MockSnapshotSupplier;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackendBuilder;
import org.apache.flink.util.StateMigrationException;

import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests how {@link ChangelogKeyedStateBackend} treats state schema evolution. */
class ChangelogKeyedStateBackendSchemaEvolutionTest {

    /**
     * The changelog backend creates states directly on the backend it wraps, bypassing that
     * backend's own registration check, and does not migrate restored values itself, so it has to
     * reject the armed serializer on its own.
     */
    @Test
    void descriptorArmedOnAMigratingBackendIsRejected() throws Exception {
        ValueStateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<>("value", StateSchemaEvolvingTestSerializer.typeInfo());
        MockKeyedStateBackend<Integer> migrating = createMigratingMock();
        try (MockEnvironment env = createEnvironment()) {
            ChangelogKeyedStateBackend<Integer> changelog =
                    (ChangelogKeyedStateBackend<Integer>)
                            ChangelogStateBackendTestUtils.createKeyedBackend(
                                    new ChangelogStateBackend(new HashMapStateBackend()), env);
            try {
                migrating.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);

                assertThatThrownBy(
                                () ->
                                        changelog.getOrCreateKeyedState(
                                                VoidNamespaceSerializer.INSTANCE, descriptor))
                        .isInstanceOf(StateMigrationException.class)
                        .hasMessageContaining("'value'");
            } finally {
                changelog.close();
                changelog.dispose();
            }
        } finally {
            migrating.dispose();
        }
    }

    private MockKeyedStateBackend<Integer> createMigratingMock() {
        return new MockKeyedStateBackendBuilder<>(
                        new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()),
                        IntSerializer.INSTANCE,
                        getClass().getClassLoader(),
                        1,
                        KeyGroupRange.of(0, 0),
                        new ExecutionConfig(),
                        TtlTimeProvider.DEFAULT,
                        LatencyTrackingStateConfig.disabled(),
                        SizeTrackingStateConfig.disabled(),
                        emptyList(),
                        UncompressedStreamCompressionDecorator.INSTANCE,
                        new CloseableRegistry(),
                        MockSnapshotSupplier.EMPTY)
                .setSupportsObjectLevelValueMigration(true)
                .build();
    }

    private static MockEnvironment createEnvironment() throws Exception {
        MockEnvironment env =
                MockEnvironment.builder()
                        .setTaskStateManager(TestTaskStateManager.builder().build())
                        .build();
        env.setCheckpointStorageAccess(
                new JobManagerCheckpointStorage().createCheckpointStorage(new JobID()));
        return env;
    }
}
