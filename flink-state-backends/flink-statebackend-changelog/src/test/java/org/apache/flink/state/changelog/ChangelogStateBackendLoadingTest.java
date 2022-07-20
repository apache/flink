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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.ChangelogTestUtils;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLoader;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.delegate.DelegatingStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TernaryBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Verify Changelog StateBackend is properly loaded. */
public class ChangelogStateBackendLoadingTest {
    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private final ClassLoader cl = getClass().getClassLoader();

    private final String backendKey = StateBackendOptions.STATE_BACKEND.key();

    @Test
    public void testLoadingDefault() throws Exception {
        final StateBackend backend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        null, TernaryBoolean.UNDEFINED, config(), cl, null);
        final CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, backend, config(), cl, null);

        assertTrue(backend instanceof HashMapStateBackend);
    }

    @Test
    public void testApplicationDefinedHasPrecedence() throws Exception {
        final StateBackend appBackend = new MockStateBackend();
        // "rocksdb" should not take effect
        final StateBackend backend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        appBackend, TernaryBoolean.UNDEFINED, config("rocksdb", true), cl, null);
        final CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, backend, config(), cl, null);

        assertDelegateStateBackend(
                backend, MockStateBackend.class, storage, MockStateBackend.class);
        assertTrue(
                ((MockStateBackend) (((ChangelogStateBackend) backend).getDelegatedStateBackend()))
                        .isConfigUpdated());
    }

    @Test
    public void testApplicationDefinedChangelogStateBackend() throws Exception {
        final StateBackend appBackend = new MockStateBackend();
        // "rocksdb" should not take effect
        final StateBackend backend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        appBackend, TernaryBoolean.TRUE, config("rocksdb", false), cl, null);
        final CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, backend, config(), cl, null);

        assertDelegateStateBackend(
                backend, MockStateBackend.class, storage, MockStateBackend.class);
        assertTrue(
                ((MockStateBackend) (((ChangelogStateBackend) backend).getDelegatedStateBackend()))
                        .isConfigUpdated());
    }

    @Test
    public void testApplicationEnableChangelogStateBackend() throws Exception {
        final StateBackend backend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        null, TernaryBoolean.TRUE, config(false), cl, null);
        final CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, backend, config(), cl, null);

        assertDelegateStateBackend(
                backend, HashMapStateBackend.class, storage, JobManagerCheckpointStorage.class);
    }

    @Test
    public void testApplicationDisableChangelogStateBackend() throws Exception {
        final StateBackend backend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        null, TernaryBoolean.FALSE, config(true), cl, null);

        assertTrue(backend instanceof HashMapStateBackend);
    }

    @Test
    public void testLoadingChangelogForRecovery() throws Exception {
        final StateBackend backend =
                StateBackendLoader.loadStateBackendFromKeyedStateHandles(
                        new MockStateBackend(),
                        cl,
                        Collections.singletonList(
                                ChangelogTestUtils.createChangelogStateBackendHandle()));

        assertTrue(backend instanceof DeactivatedChangelogStateBackend);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRecursiveDelegation() throws Exception {
        final StateBackend appBackend =
                new ChangelogStateBackend(new ChangelogStateBackend(new MockStateBackend()));

        StateBackendLoader.fromApplicationOrConfigOrDefault(
                appBackend, TernaryBoolean.UNDEFINED, config("rocksdb", true), cl, null);
    }

    // ----------------------------------------------------------
    // The following tests are testing different combinations of
    // state backend and checkpointStorage after FLINK-19463
    // disentangles Checkpointing from state backends.
    // After "jobmanager" and "filesystem" state backends are removed,
    // These tests can be simplified.
    //
    @Test
    public void testLoadingMemoryStateBackendFromConfig() throws Exception {
        testLoadingStateBackend(
                "jobmanager", MemoryStateBackend.class, MemoryStateBackend.class, true);
    }

    @Test
    public void testLoadingMemoryStateBackend() throws Exception {
        testLoadingStateBackend(
                "jobmanager", MemoryStateBackend.class, MemoryStateBackend.class, false);
    }

    @Test
    public void testLoadingFsStateBackendFromConfig() throws Exception {
        testLoadingStateBackend(
                "filesystem", HashMapStateBackend.class, JobManagerCheckpointStorage.class, true);
    }

    @Test
    public void testLoadingFsStateBackend() throws Exception {
        testLoadingStateBackend(
                "filesystem", HashMapStateBackend.class, JobManagerCheckpointStorage.class, false);
    }

    @Test
    public void testLoadingHashMapStateBackendFromConfig() throws Exception {
        testLoadingStateBackend(
                "hashmap", HashMapStateBackend.class, JobManagerCheckpointStorage.class, true);
    }

    @Test
    public void testLoadingHashMapStateBackend() throws Exception {
        testLoadingStateBackend(
                "hashmap", HashMapStateBackend.class, JobManagerCheckpointStorage.class, false);
    }

    @Test
    public void testLoadingRocksDBStateBackendFromConfig() throws Exception {
        testLoadingStateBackend(
                "rocksdb",
                EmbeddedRocksDBStateBackend.class,
                JobManagerCheckpointStorage.class,
                true);
    }

    @Test
    public void testLoadingRocksDBStateBackend() throws Exception {
        testLoadingStateBackend(
                "rocksdb",
                EmbeddedRocksDBStateBackend.class,
                JobManagerCheckpointStorage.class,
                false);
    }

    @Test
    public void testEnableChangelogStateBackendInStreamExecutionEnvironment() throws Exception {
        StreamExecutionEnvironment env = getEnvironment();
        assertStateBackendAndChangelogInEnvironmentAndStreamGraphAndJobGraph(
                env, TernaryBoolean.UNDEFINED, null);

        // set back and force
        env.setStateBackend(new MemoryStateBackend());
        assertTrue(env.getStateBackend() instanceof MemoryStateBackend);
        assertStateBackendAndChangelogInEnvironmentAndStreamGraphAndJobGraph(
                env, TernaryBoolean.UNDEFINED, MemoryStateBackend.class);
        env.enableChangelogStateBackend(true);
        assertStateBackendAndChangelogInEnvironmentAndStreamGraphAndJobGraph(
                env, TernaryBoolean.TRUE, MemoryStateBackend.class);
        env.enableChangelogStateBackend(false);
        assertStateBackendAndChangelogInEnvironmentAndStreamGraphAndJobGraph(
                env, TernaryBoolean.FALSE, MemoryStateBackend.class);

        // enable changelog before set statebackend
        env = getEnvironment();
        env.enableChangelogStateBackend(true);
        assertStateBackendAndChangelogInEnvironmentAndStreamGraphAndJobGraph(
                env, TernaryBoolean.TRUE, null);
        env.setStateBackend(new MemoryStateBackend());
        assertStateBackendAndChangelogInEnvironmentAndStreamGraphAndJobGraph(
                env, TernaryBoolean.TRUE, MemoryStateBackend.class);
    }

    private Configuration config(String stateBackend, boolean enableChangelogStateBackend) {
        final Configuration config = new Configuration();
        config.setBoolean(
                StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, enableChangelogStateBackend);
        config.setString(backendKey, stateBackend);

        return config;
    }

    private Configuration config(boolean enableChangelogStateBackend) {
        final Configuration config = new Configuration();
        config.setBoolean(
                StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, enableChangelogStateBackend);

        return config;
    }

    private Configuration config(String stateBackend) {
        final Configuration config = new Configuration();
        config.setString(backendKey, stateBackend);

        return config;
    }

    private Configuration config() {
        final Configuration config = new Configuration();

        return config;
    }

    private void assertDelegateStateBackend(
            StateBackend backend,
            Class<?> delegatedStateBackendClass,
            CheckpointStorage storage,
            Class<?> storageClass) {
        assertTrue(backend instanceof ChangelogStateBackend);
        assertSame(
                ((DelegatingStateBackend) backend).getDelegatedStateBackend().getClass(),
                delegatedStateBackendClass);
        assertSame(storage.getClass(), storageClass);
    }

    private void testLoadingStateBackend(
            String backendName,
            Class<?> delegatedStateBackendClass,
            Class<?> storageClass,
            boolean configOnly)
            throws Exception {
        final Configuration config = config(backendName, true);
        StateBackend backend;
        StateBackend appBackend = StateBackendLoader.loadStateBackendFromConfig(config, cl, null);

        if (configOnly) {
            backend =
                    StateBackendLoader.fromApplicationOrConfigOrDefault(
                            null, TernaryBoolean.UNDEFINED, config, cl, null);
        } else {
            backend =
                    StateBackendLoader.fromApplicationOrConfigOrDefault(
                            appBackend, TernaryBoolean.TRUE, config, cl, null);
        }

        final CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, backend, config, cl, null);

        assertDelegateStateBackend(backend, delegatedStateBackendClass, storage, storageClass);
    }

    private StreamExecutionEnvironment getEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SourceFunction<Integer> srcFun =
                new SourceFunction<Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {}

                    @Override
                    public void cancel() {}
                };

        SingleOutputStreamOperator<Object> operator =
                env.addSource(srcFun)
                        .flatMap(
                                new FlatMapFunction<Integer, Object>() {

                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void flatMap(Integer value, Collector<Object> out)
                                            throws Exception {}
                                });
        operator.setParallelism(1);
        return env;
    }

    private void assertStateBackendAndChangelogInEnvironmentAndStreamGraphAndJobGraph(
            StreamExecutionEnvironment env,
            TernaryBoolean isChangelogEnabled,
            Class<?> rootStateBackendClass)
            throws Exception {
        assertEquals(isChangelogEnabled, env.isChangelogStateBackendEnabled());
        if (rootStateBackendClass == null) {
            assertNull(env.getStateBackend());
        } else {
            assertSame(rootStateBackendClass, env.getStateBackend().getClass());
        }

        StreamGraph streamGraph = env.getStreamGraph(false);
        assertEquals(isChangelogEnabled, streamGraph.isChangelogStateBackendEnabled());
        if (rootStateBackendClass == null) {
            assertNull(streamGraph.getStateBackend());
        } else {
            assertSame(rootStateBackendClass, streamGraph.getStateBackend().getClass());
        }
        JobCheckpointingSettings checkpointingSettings =
                streamGraph.getJobGraph().getCheckpointingSettings();
        assertEquals(isChangelogEnabled, checkpointingSettings.isChangelogStateBackendEnabled());
        if (rootStateBackendClass == null) {
            assertNull(checkpointingSettings.getDefaultStateBackend());
        } else {
            assertSame(
                    rootStateBackendClass,
                    checkpointingSettings.getDefaultStateBackend().deserializeValue(cl).getClass());
            assertSame(
                    rootStateBackendClass,
                    unwrapFromDelegatingStateBackend(
                                    checkpointingSettings
                                            .getDefaultStateBackend()
                                            .deserializeValue(cl))
                            .getClass());
        }
    }

    private StateBackend unwrapFromDelegatingStateBackend(StateBackend backend) {
        if (backend instanceof DelegatingStateBackend) {
            return ((DelegatingStateBackend) backend).getDelegatedStateBackend();
        } else {
            return backend;
        }
    }

    private static class MockStateBackend extends AbstractStateBackend
            implements CheckpointStorage, ConfigurableStateBackend {
        private boolean configUpdated = false;

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
                CloseableRegistry cancelStreamRegistry) {
            return null;
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                Environment env,
                String operatorIdentifier,
                @Nonnull Collection<OperatorStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry) {
            return null;
        }

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) {
            return null;
        }

        @Override
        public CheckpointStorageAccess createCheckpointStorage(JobID jobId) {
            return null;
        }

        @Override
        public StateBackend configure(ReadableConfig config, ClassLoader classLoader)
                throws IllegalConfigurationException {
            configUpdated = true;
            return this;
        }

        boolean isConfigUpdated() {
            return configUpdated;
        }
    }
}
