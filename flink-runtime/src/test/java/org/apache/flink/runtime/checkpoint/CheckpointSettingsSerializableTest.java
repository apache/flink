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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test validates that the checkpoint settings serialize correctly in the presence of
 * user-defined objects.
 */
public class CheckpointSettingsSerializableTest extends TestLogger {

    @Test
    public void testDeserializationOfUserCodeWithUserClassLoader() throws Exception {
        final ClassLoaderUtils.ObjectAndClassLoader<Serializable> outsideClassLoading =
                ClassLoaderUtils.createSerializableObjectFromNewClassLoader();
        final ClassLoader classLoader = outsideClassLoading.getClassLoader();
        final Serializable outOfClassPath = outsideClassLoading.getObject();

        final MasterTriggerRestoreHook.Factory[] hooks = {new TestFactory(outOfClassPath)};
        final SerializedValue<MasterTriggerRestoreHook.Factory[]> serHooks =
                new SerializedValue<>(hooks);

        final JobCheckpointingSettings checkpointingSettings =
                new JobCheckpointingSettings(
                        Collections.<JobVertexID>emptyList(),
                        Collections.<JobVertexID>emptyList(),
                        Collections.<JobVertexID>emptyList(),
                        new CheckpointCoordinatorConfiguration(
                                1000L,
                                10000L,
                                0L,
                                1,
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
                                true,
                                false,
                                false,
                                0),
                        new SerializedValue<StateBackend>(new CustomStateBackend(outOfClassPath)),
                        serHooks);

        final JobGraph jobGraph = new JobGraph(new JobID(), "test job");
        jobGraph.setSnapshotSettings(checkpointingSettings);

        // to serialize/deserialize the job graph to see if the behavior is correct under
        // distributed execution
        final JobGraph copy = CommonTestUtils.createCopySerializable(jobGraph);

        final ExecutionGraph eg =
                TestingExecutionGraphBuilder.newBuilder()
                        .setJobGraph(copy)
                        .setUserClassLoader(classLoader)
                        .build();

        assertEquals(1, eg.getCheckpointCoordinator().getNumberOfRegisteredMasterHooks());
        assertTrue(
                jobGraph.getCheckpointingSettings()
                                .getDefaultStateBackend()
                                .deserializeValue(classLoader)
                        instanceof CustomStateBackend);
    }

    // ------------------------------------------------------------------------

    private static final class TestFactory implements MasterTriggerRestoreHook.Factory {

        private static final long serialVersionUID = -612969579110202607L;

        private final Serializable payload;

        TestFactory(Serializable payload) {
            this.payload = payload;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <V> MasterTriggerRestoreHook<V> create() {
            MasterTriggerRestoreHook<V> hook = mock(MasterTriggerRestoreHook.class);
            when(hook.getIdentifier()).thenReturn("id");
            return hook;
        }
    }

    private static final class CustomStateBackend implements StateBackend {

        private static final long serialVersionUID = -6107964383429395816L;
        /** Simulate a custom option that is not in the normal classpath. */
        @SuppressWarnings("unused")
        private Serializable customOption;

        public CustomStateBackend(Serializable customOption) {
            this.customOption = customOption;
        }

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
            return mock(CheckpointStorageAccess.class);
        }

        @Override
        public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
                Environment env,
                JobID jobID,
                String operatorIdentifier,
                TypeSerializer<K> keySerializer,
                int numberOfKeyGroups,
                KeyGroupRange keyGroupRange,
                TaskKvStateRegistry kvStateRegistry,
                TtlTimeProvider ttlTimeProvider,
                MetricGroup metricGroup,
                @Nonnull Collection<KeyedStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                Environment env,
                String operatorIdentifier,
                @Nonnull Collection<OperatorStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws Exception {
            throw new UnsupportedOperationException();
        }
    }
}
