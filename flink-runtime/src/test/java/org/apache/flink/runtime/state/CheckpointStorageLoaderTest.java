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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

/** This test validates that checkpoint storage is properly loaded from configuration. */
public class CheckpointStorageLoaderTest {

    private final ClassLoader cl = getClass().getClassLoader();

    @Test
    public void testNoCheckpointStorageDefined() throws Exception {
        Assert.assertFalse(
                CheckpointStorageLoader.fromConfig(new Configuration(), cl, null).isPresent());
    }

    @Test
    public void testLegacyStateBackendTakesPrecedence() throws Exception {
        StateBackend legacy = new LegacyStateBackend();
        CheckpointStorage storage = new MockStorage();

        CheckpointStorage configured =
                CheckpointStorageLoader.load(storage, legacy, new Configuration(), cl, null);

        Assert.assertEquals(
                "Legacy state backends should always take precedence", legacy, configured);
    }

    @Test
    public void testModernStateBackendDoesNotTakePrecedence() throws Exception {
        StateBackend modern = new ModernStateBackend();
        CheckpointStorage storage = new MockStorage();

        CheckpointStorage configured =
                CheckpointStorageLoader.load(storage, modern, new Configuration(), cl, null);

        Assert.assertEquals(
                "Modern state backends should never take precedence", storage, configured);
    }

    @Test
    public void testLoadingFromFactory() throws Exception {
        final Configuration config = new Configuration();

        config.setString(CheckpointingOptions.CHECKPOINT_STORAGE, WorkingFactory.class.getName());
        CheckpointStorage storage =
                CheckpointStorageLoader.load(null, new ModernStateBackend(), config, cl, null);
        Assert.assertThat(storage, Matchers.instanceOf(MockStorage.class));
    }

    @Test
    public void testLoadingFails() throws Exception {
        final Configuration config = new Configuration();

        config.setString(CheckpointingOptions.CHECKPOINT_STORAGE, "does.not.exist");
        try {
            CheckpointStorageLoader.load(null, new ModernStateBackend(), config, cl, null);
            Assert.fail("should fail with exception");
        } catch (DynamicCodeLoadingException e) {
            // expected
        }

        // try a class that is not a factory
        config.setString(CheckpointingOptions.CHECKPOINT_STORAGE, java.io.File.class.getName());
        try {
            CheckpointStorageLoader.load(null, new ModernStateBackend(), config, cl, null);
            Assert.fail("should fail with exception");
        } catch (DynamicCodeLoadingException e) {
            // expected
        }

        // try a factory that fails
        config.setString(CheckpointingOptions.CHECKPOINT_STORAGE, FailingFactory.class.getName());
        try {
            CheckpointStorageLoader.load(null, new ModernStateBackend(), config, cl, null);
            Assert.fail("should fail with exception");
        } catch (IllegalConfigurationException e) {
            // expected
        }
    }

    // A state backend that also implements checkpoint storage.
    static final class LegacyStateBackend implements StateBackend, CheckpointStorage {
        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer)
                throws IOException {
            return null;
        }

        @Override
        public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
            return null;
        }

        @Override
        public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
                Environment env,
                JobID jobID,
                String operatorIdentifier,
                TypeSerializer<K> keySerializer,
                int numberOfKeyGroups,
                KeyGroupRange keyGroupRange,
                TaskKvStateRegistry kvStateRegistry,
                TtlTimeProvider ttlTimeProvider,
                MetricGroup metricGroup,
                Collection<KeyedStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws Exception {
            return null;
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                Environment env,
                String operatorIdentifier,
                Collection<OperatorStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws Exception {
            return null;
        }
    }

    static final class ModernStateBackend implements StateBackend {

        @Override
        public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
                Environment env,
                JobID jobID,
                String operatorIdentifier,
                TypeSerializer<K> keySerializer,
                int numberOfKeyGroups,
                KeyGroupRange keyGroupRange,
                TaskKvStateRegistry kvStateRegistry,
                TtlTimeProvider ttlTimeProvider,
                MetricGroup metricGroup,
                Collection<KeyedStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws Exception {
            return null;
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                Environment env,
                String operatorIdentifier,
                Collection<OperatorStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws Exception {
            return null;
        }
    }

    static final class MockStorage implements CheckpointStorage {

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer)
                throws IOException {
            return null;
        }

        @Override
        public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
            return null;
        }
    }

    static final class WorkingFactory implements CheckpointStorageFactory<MockStorage> {

        @Override
        public MockStorage createFromConfig(ReadableConfig config, ClassLoader classLoader)
                throws IllegalConfigurationException {
            return new MockStorage();
        }
    }

    static final class FailingFactory implements CheckpointStorageFactory<CheckpointStorage> {

        @Override
        public CheckpointStorage createFromConfig(ReadableConfig config, ClassLoader classLoader)
                throws IllegalConfigurationException {
            throw new IllegalConfigurationException("fail!");
        }
    }
}
