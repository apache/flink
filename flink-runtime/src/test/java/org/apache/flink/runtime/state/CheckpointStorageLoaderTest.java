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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.HamcrestCondition.matching;

/** This test validates that checkpoint storage is properly loaded from configuration. */
class CheckpointStorageLoaderTest {

    private final Logger LOG = LoggerFactory.getLogger(CheckpointStorageLoaderTest.class);

    @TempDir private java.nio.file.Path tmp;

    private final ClassLoader cl = getClass().getClassLoader();

    @Test
    void testNoCheckpointStorageDefined() throws Exception {
        assertThat(CheckpointStorageLoader.fromConfig(new Configuration(), cl, null))
                .isNotPresent();
    }

    @Test
    void testLegacyStateBackendTakesPrecedence() throws Exception {
        StateBackend legacy = new LegacyStateBackend();
        CheckpointStorage storage = new MockStorage();

        CheckpointStorage configured =
                CheckpointStorageLoader.load(storage, null, legacy, new Configuration(), cl, LOG);

        assertThat(configured)
                .withFailMessage("Legacy state backends should always take precedence")
                .isEqualTo(legacy);
    }

    @Test
    void testModernStateBackendDoesNotTakePrecedence() throws Exception {
        StateBackend modern = new ModernStateBackend();
        CheckpointStorage storage = new MockStorage();

        CheckpointStorage configured =
                CheckpointStorageLoader.load(storage, null, modern, new Configuration(), cl, LOG);

        assertThat(configured)
                .withFailMessage("Modern state backends should never take precedence")
                .isEqualTo(storage);
    }

    @Test
    void testLoadingFromFactory() throws Exception {
        final Configuration config = new Configuration();

        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, WorkingFactory.class.getName());
        CheckpointStorage storage =
                CheckpointStorageLoader.load(null, null, new ModernStateBackend(), config, cl, LOG);
        assertThat(storage).isInstanceOf(MockStorage.class);
    }

    @Test
    void testDefaultCheckpointStorage() throws Exception {
        CheckpointStorage storage1 =
                CheckpointStorageLoader.load(
                        null, null, new ModernStateBackend(), new Configuration(), cl, LOG);

        assertThat(storage1).isInstanceOf(JobManagerCheckpointStorage.class);

        final String checkpointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        CheckpointStorage storage2 =
                CheckpointStorageLoader.load(null, null, new ModernStateBackend(), config, cl, LOG);

        assertThat(storage2).isInstanceOf(FileSystemCheckpointStorage.class);
    }

    @Test
    void testLoadingFails() throws Exception {
        final Configuration config = new Configuration();

        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "does.not.exist");
        assertThatThrownBy(
                        () ->
                                CheckpointStorageLoader.load(
                                        null, null, new ModernStateBackend(), config, cl, LOG))
                .isInstanceOf(DynamicCodeLoadingException.class);

        // try a class that is not a factory
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, java.io.File.class.getName());
        assertThatThrownBy(
                        () ->
                                CheckpointStorageLoader.load(
                                        null, null, new ModernStateBackend(), config, cl, LOG))
                .isInstanceOf(DynamicCodeLoadingException.class);

        // try a factory that fails
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, FailingFactory.class.getName());
        assertThatThrownBy(
                        () ->
                                CheckpointStorageLoader.load(
                                        null, null, new ModernStateBackend(), config, cl, LOG))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    // ------------------------------------------------------------------------
    //  Job Manager Checkpoint Storage
    // ------------------------------------------------------------------------

    /** Validates loading a job manager checkpoint storage from the cluster configuration. */
    @Test
    void testLoadJobManagerStorageNoParameters() throws Exception {
        // we configure with the explicit string (rather than
        // AbstractStateBackend#X_STATE_BACKEND_NAME)
        // to guard against config-breaking changes of the name

        final Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");

        CheckpointStorage storage = CheckpointStorageLoader.fromConfig(config, cl, null).get();
        assertThat(storage).isInstanceOf(JobManagerCheckpointStorage.class);
    }

    /**
     * Validates loading a job manager checkpoint storage with additional parameters from the
     * cluster configuration.
     */
    @Test
    void testLoadJobManagerStorageWithParameters() throws Exception {
        final String savepointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        final Path expectedSavepointPath = new Path(savepointDir);

        // we configure with the explicit string (rather than
        // AbstractStateBackend#X_STATE_BACKEND_NAME)
        // to guard against config-breaking changes of the name

        final Configuration config1 = new Configuration();
        config1.set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");
        config1.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

        CheckpointStorage storage1 = CheckpointStorageLoader.fromConfig(config1, cl, null).get();

        assertThat(storage1).isInstanceOf(JobManagerCheckpointStorage.class);

        assertThat(((JobManagerCheckpointStorage) storage1).getSavepointPath())
                .isEqualTo(expectedSavepointPath);
    }

    /**
     * Validates taking the application-defined job manager checkpoint storage and adding additional
     * parameters from the cluster configuration.
     */
    @Test
    void testConfigureJobManagerStorage() throws Exception {
        final String savepointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        final Path expectedSavepointPath = new Path(savepointDir);

        final int maxSize = 100;

        final Configuration config = new Configuration();
        config.set(
                CheckpointingOptions.CHECKPOINT_STORAGE,
                "filesystem"); // check that this is not accidentally picked up
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

        CheckpointStorage storage =
                CheckpointStorageLoader.load(
                        new JobManagerCheckpointStorage(maxSize),
                        null,
                        new ModernStateBackend(),
                        config,
                        cl,
                        LOG);

        assertThat(storage).isInstanceOf(JobManagerCheckpointStorage.class);
        JobManagerCheckpointStorage jmStorage = (JobManagerCheckpointStorage) storage;

        assertThat(jmStorage.getSavepointPath())
                .is(matching(normalizedPath(expectedSavepointPath)));
        assertThat(jmStorage.getMaxStateSize()).isEqualTo(maxSize);
    }

    /** Tests that job parameters take precedence over cluster configurations. */
    @Test
    void testConfigureJobManagerStorageWithParameters() throws Exception {
        final String savepointDirConfig = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        final Path savepointDirJob = new Path(TempDirUtils.newFolder(tmp).toURI());

        final Configuration config = new Configuration();
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDirConfig);

        CheckpointStorage storage =
                CheckpointStorageLoader.load(
                        new JobManagerCheckpointStorage(),
                        savepointDirJob,
                        new ModernStateBackend(),
                        config,
                        cl,
                        LOG);

        assertThat(storage).isInstanceOf(JobManagerCheckpointStorage.class);
        JobManagerCheckpointStorage jmStorage = (JobManagerCheckpointStorage) storage;
        assertThat(jmStorage.getSavepointPath()).is(matching(normalizedPath(savepointDirJob)));
    }

    // ------------------------------------------------------------------------
    //  File System Checkpoint Storage
    // ------------------------------------------------------------------------

    /**
     * Validates loading a file system checkpoint storage with additional parameters from the
     * cluster configuration.
     */
    @Test
    void testLoadFileSystemCheckpointStorage() throws Exception {
        final String checkpointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        final String savepointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        final Path expectedCheckpointsPath = new Path(checkpointDir);
        final Path expectedSavepointsPath = new Path(savepointDir);
        final MemorySize threshold = MemorySize.parse("900kb");
        final int minWriteBufferSize = 1024;

        // we configure with the explicit string (rather than
        // AbstractStateBackend#X_STATE_BACKEND_NAME)
        // to guard against config-breaking changes of the name
        final Configuration config1 = new Configuration();
        config1.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config1.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        config1.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
        config1.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, threshold);
        config1.setInteger(CheckpointingOptions.FS_WRITE_BUFFER_SIZE, minWriteBufferSize);

        CheckpointStorage storage1 = CheckpointStorageLoader.fromConfig(config1, cl, null).get();

        assertThat(storage1).isInstanceOf(FileSystemCheckpointStorage.class);

        FileSystemCheckpointStorage fs1 = (FileSystemCheckpointStorage) storage1;

        assertThat(fs1.getCheckpointPath()).is(matching(normalizedPath(expectedCheckpointsPath)));
        assertThat(fs1.getSavepointPath()).is(matching(normalizedPath(expectedSavepointsPath)));
        assertThat(fs1.getMinFileSizeThreshold()).isEqualTo(threshold.getBytes());
        assertThat(fs1.getWriteBufferSize())
                .isEqualTo(Math.max(threshold.getBytes(), minWriteBufferSize));
    }

    /**
     * Validates taking the application-defined file system state backend and adding with additional
     * parameters from the cluster configuration, but giving precedence to application-defined
     * parameters over configuration-defined parameters.
     */
    @Test
    void testLoadFileSystemCheckpointStorageMixed() throws Exception {
        final Path appCheckpointDir = new Path(TempDirUtils.newFolder(tmp).toURI());
        final String checkpointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        final String savepointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();

        final Path expectedSavepointsPath = new Path(savepointDir);

        final int threshold = 1000000;
        final int writeBufferSize = 4000000;

        final FileSystemCheckpointStorage storage =
                new FileSystemCheckpointStorage(appCheckpointDir, threshold, writeBufferSize);

        final Configuration config = new Configuration();
        config.set(
                CheckpointingOptions.CHECKPOINT_STORAGE,
                "jobmanager"); // this should not be picked up
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                checkpointDir); // this should not be picked up
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
        config.set(
                CheckpointingOptions.FS_SMALL_FILE_THRESHOLD,
                MemorySize.parse("20")); // this should not be picked up
        config.setInteger(
                CheckpointingOptions.FS_WRITE_BUFFER_SIZE, 3000000); // this should not be picked up

        final CheckpointStorage loadedStorage =
                CheckpointStorageLoader.load(
                        storage, null, new ModernStateBackend(), config, cl, LOG);
        assertThat(loadedStorage).isInstanceOf(FileSystemCheckpointStorage.class);

        final FileSystemCheckpointStorage fs = (FileSystemCheckpointStorage) loadedStorage;
        assertThat(fs.getCheckpointPath()).is(matching(normalizedPath(appCheckpointDir)));
        assertThat(fs.getSavepointPath()).is(matching(normalizedPath(expectedSavepointsPath)));
        assertThat(fs.getMinFileSizeThreshold()).isEqualTo(threshold);
        assertThat(fs.getWriteBufferSize()).isEqualTo(writeBufferSize);
    }

    // ------------------------------------------------------------------------
    //  High-availability default
    // ------------------------------------------------------------------------

    /**
     * This tests the default behaviour in the case of configured high-availability. Specially, if
     * not configured checkpoint directory, the memory state backend would not create arbitrary
     * directory under HA persistence directory.
     */
    @Test
    void testHighAvailabilityDefault() throws Exception {
        final String haPersistenceDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        testMemoryBackendHighAvailabilityDefault(haPersistenceDir, null);

        final Path checkpointPath = new Path(TempDirUtils.newFolder(tmp).toURI().toString());
        testMemoryBackendHighAvailabilityDefault(haPersistenceDir, checkpointPath);
    }

    @Test
    void testHighAvailabilityDefaultLocalPaths() throws Exception {
        final String haPersistenceDir =
                new Path(TempDirUtils.newFolder(tmp).getAbsolutePath()).toString();
        testMemoryBackendHighAvailabilityDefault(haPersistenceDir, null);

        final Path checkpointPath =
                new Path(TempDirUtils.newFolder(tmp).toURI().toString())
                        .makeQualified(FileSystem.getLocalFileSystem());
        testMemoryBackendHighAvailabilityDefault(haPersistenceDir, checkpointPath);
    }

    private void testMemoryBackendHighAvailabilityDefault(
            String haPersistenceDir, Path checkpointPath) throws Exception {
        final Configuration config1 = new Configuration();
        config1.set(HighAvailabilityOptions.HA_MODE, "zookeeper");
        config1.set(HighAvailabilityOptions.HA_CLUSTER_ID, "myCluster");
        config1.set(HighAvailabilityOptions.HA_STORAGE_PATH, haPersistenceDir);

        final Configuration config2 = new Configuration();
        config2.set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");
        config2.set(HighAvailabilityOptions.HA_MODE, "zookeeper");
        config2.set(HighAvailabilityOptions.HA_CLUSTER_ID, "myCluster");
        config2.set(HighAvailabilityOptions.HA_STORAGE_PATH, haPersistenceDir);

        if (checkpointPath != null) {
            config1.set(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointPath.toUri().toString());
            config2.set(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointPath.toUri().toString());
        }

        final JobManagerCheckpointStorage storage = new JobManagerCheckpointStorage();

        final CheckpointStorage loaded1 =
                CheckpointStorageLoader.load(
                        storage, null, new ModernStateBackend(), config1, cl, LOG);
        final CheckpointStorage loaded2 =
                CheckpointStorageLoader.load(
                        null, null, new ModernStateBackend(), config2, cl, LOG);

        assertThat(loaded1).isInstanceOf(JobManagerCheckpointStorage.class);
        assertThat(loaded2).isInstanceOf(JobManagerCheckpointStorage.class);

        final JobManagerCheckpointStorage memStorage1 = (JobManagerCheckpointStorage) loaded1;
        final JobManagerCheckpointStorage memStorage2 = (JobManagerCheckpointStorage) loaded2;

        assertThat(memStorage1.getSavepointPath()).isNull();
        assertThat(memStorage2.getSavepointPath()).isNull();

        if (checkpointPath != null) {
            assertThat(memStorage1.getCheckpointPath())
                    .is(matching(normalizedPath(checkpointPath)));
            assertThat(memStorage2.getCheckpointPath())
                    .is(matching(normalizedPath(checkpointPath)));
        } else {
            assertThat(memStorage1.getCheckpointPath()).isNull();
            assertThat(memStorage2.getCheckpointPath()).isNull();
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

    private static Matcher<Path> normalizedPath(Path expected) {
        return new NormalizedPathMatcher(expected);
    }

    /** Due to canonicalization, paths need to be renormalized before comparison. */
    private static class NormalizedPathMatcher extends TypeSafeMatcher<Path> {

        private final Path reNormalizedExpected;

        private NormalizedPathMatcher(Path expected) {
            this.reNormalizedExpected = expected == null ? null : new Path(expected.toString());
        }

        @Override
        protected boolean matchesSafely(Path actual) {
            if (reNormalizedExpected == null) {
                return actual == null;
            }

            Path reNormalizedActual = new Path(actual.toString());
            return reNormalizedExpected.equals(reNormalizedActual);
        }

        @Override
        public void describeTo(Description description) {
            description.appendValue(reNormalizedExpected);
        }
    }
}
