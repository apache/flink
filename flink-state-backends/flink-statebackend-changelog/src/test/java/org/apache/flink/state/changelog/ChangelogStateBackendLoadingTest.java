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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.util.Collection;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Verify Changelog StateBackend is properly loaded. */
public class ChangelogStateBackendLoadingTest {
    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private final ClassLoader cl = getClass().getClassLoader();

    private final String backendKey = CheckpointingOptions.STATE_BACKEND.key();

    @Test
    public void testLoadingDefault() throws Exception {
        final StateBackend backend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(null, config(), cl, null);
        final CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, backend, config(), cl, null);

        assertDelegateStateBackend(
                backend, HashMapStateBackend.class, storage, JobManagerCheckpointStorage.class);
    }

    @Test
    public void testApplicationDefinedHasPrecedence() throws Exception {
        final StateBackend appBackend = new MockStateBackend();
        // "rocksdb" should not take effect
        final StateBackend backend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        appBackend, config("rocksdb"), cl, null);
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
        final StateBackend appBackend = new ChangelogStateBackend(new MockStateBackend());
        // "rocksdb" should not take effect
        final StateBackend backend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        appBackend, config("rocksdb"), cl, null);
        final CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, backend, config(), cl, null);

        assertDelegateStateBackend(
                backend, MockStateBackend.class, storage, MockStateBackend.class);
        assertTrue(
                ((MockStateBackend) (((ChangelogStateBackend) backend).getDelegatedStateBackend()))
                        .isConfigUpdated());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRecursiveDelegation() throws Exception {
        final StateBackend appBackend =
                new ChangelogStateBackend(new ChangelogStateBackend(new MockStateBackend()));

        StateBackendLoader.fromApplicationOrConfigOrDefault(
                appBackend, config("rocksdb"), cl, null);
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

    private Configuration config(String stateBackend) {
        final Configuration config = config();
        config.setString(backendKey, stateBackend);

        return config;
    }

    private Configuration config() {
        final Configuration config = new Configuration();
        config.setBoolean(CheckpointingOptions.ENABLE_STATE_CHANGE_LOG, true);

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
        final Configuration config = config(backendName);
        StateBackend backend;

        if (configOnly) {
            backend = StateBackendLoader.loadStateBackendFromConfig(config, cl, null);
        } else {
            backend = StateBackendLoader.fromApplicationOrConfigOrDefault(null, config, cl, null);
        }

        final CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, backend, config, cl, null);

        assertDelegateStateBackend(backend, delegatedStateBackendClass, storage, storageClass);
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
