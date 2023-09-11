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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackendFactory;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.TernaryBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** This test validates that state backends are properly loaded from configuration. */
class StateBackendLoadingTest {

    @TempDir private java.nio.file.Path tmp;

    private final ClassLoader cl = getClass().getClassLoader();

    private final String backendKey = StateBackendOptions.STATE_BACKEND.key();

    // ------------------------------------------------------------------------
    //  defaults
    // ------------------------------------------------------------------------

    @Test
    void testNoStateBackendDefined() throws Exception {
        assertThat(StateBackendLoader.loadStateBackendFromConfig(new Configuration(), cl, null))
                .isNull();
    }

    @Test
    void testInstantiateHashMapStateBackendBackendByDefault() throws Exception {
        StateBackend backend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        null, TernaryBoolean.UNDEFINED, new Configuration(), cl, null);

        assertThat(backend).isInstanceOf(HashMapStateBackend.class);
    }

    @Test
    void testApplicationDefinedHasPrecedence() throws Exception {
        final StateBackend appBackend = Mockito.mock(StateBackend.class);

        final Configuration config = new Configuration();
        config.setString(backendKey, "jobmanager");

        StateBackend backend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        appBackend, TernaryBoolean.UNDEFINED, config, cl, null);
        assertThat(backend).isEqualTo(appBackend);
    }

    // ------------------------------------------------------------------------
    //  Memory State Backend
    // ------------------------------------------------------------------------

    /** Validates loading a memory state backend from the cluster configuration. */
    @Test
    void testLoadMemoryStateBackendNoParameters() throws Exception {
        // we configure with the explicit string (rather than
        // AbstractStateBackend#X_STATE_BACKEND_NAME)
        // to guard against config-breaking changes of the name

        final Configuration config1 = new Configuration();
        config1.setString(backendKey, "jobmanager");

        final Configuration config2 = new Configuration();
        config2.setString(backendKey, MemoryStateBackendFactory.class.getName());

        StateBackend backend1 = StateBackendLoader.loadStateBackendFromConfig(config1, cl, null);
        StateBackend backend2 = StateBackendLoader.loadStateBackendFromConfig(config2, cl, null);

        assertThat(backend1).isInstanceOf(MemoryStateBackend.class);
        assertThat(backend2).isInstanceOf(MemoryStateBackend.class);
    }

    /**
     * Validates loading a memory state backend with additional parameters from the cluster
     * configuration.
     */
    @Test
    void testLoadMemoryStateWithParameters() throws Exception {
        final String checkpointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        final String savepointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        final Path expectedCheckpointPath = new Path(checkpointDir);
        final Path expectedSavepointPath = new Path(savepointDir);

        // we configure with the explicit string (rather than
        // AbstractStateBackend#X_STATE_BACKEND_NAME)
        // to guard against config-breaking changes of the name

        final Configuration config1 = new Configuration();
        config1.setString(backendKey, "jobmanager");
        config1.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        config1.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

        final Configuration config2 = new Configuration();
        config2.setString(backendKey, MemoryStateBackendFactory.class.getName());
        config2.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        config2.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

        MemoryStateBackend backend1 =
                (MemoryStateBackend)
                        StateBackendLoader.loadStateBackendFromConfig(config1, cl, null);
        MemoryStateBackend backend2 =
                (MemoryStateBackend)
                        StateBackendLoader.loadStateBackendFromConfig(config2, cl, null);

        assertThat(backend1).isNotNull();
        assertThat(backend2).isNotNull();

        assertThat(backend1.getCheckpointPath()).isEqualTo(expectedCheckpointPath);
        assertThat(backend1.getSavepointPath()).isEqualTo(expectedSavepointPath);
        assertThat(backend2.getCheckpointPath()).isEqualTo(expectedCheckpointPath);
        assertThat(backend2.getSavepointPath()).isEqualTo(expectedSavepointPath);
    }

    /**
     * Validates taking the application-defined memory state backend and adding additional
     * parameters from the cluster configuration.
     */
    @Test
    void testConfigureMemoryStateBackend() throws Exception {
        final String checkpointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        final String savepointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        final Path expectedCheckpointPath = new Path(checkpointDir);
        final Path expectedSavepointPath = new Path(savepointDir);

        final int maxSize = 100;

        final MemoryStateBackend backend = new MemoryStateBackend(maxSize);

        final Configuration config = new Configuration();
        config.setString(backendKey, "filesystem"); // check that this is not accidentally picked up
        config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

        StateBackend loadedBackend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        backend, TernaryBoolean.UNDEFINED, config, cl, null);
        assertThat(loadedBackend).isInstanceOf(MemoryStateBackend.class);

        final MemoryStateBackend memBackend = (MemoryStateBackend) loadedBackend;
        assertThat(memBackend.getCheckpointPath()).isEqualTo(expectedCheckpointPath);
        assertThat(memBackend.getSavepointPath()).isEqualTo(expectedSavepointPath);
        assertThat(memBackend.getMaxStateSize()).isEqualTo(maxSize);
    }

    /**
     * Validates taking the application-defined memory state backend and adding additional
     * parameters from the cluster configuration, but giving precedence to application-defined
     * parameters over configuration-defined parameters.
     */
    @Test
    void testConfigureMemoryStateBackendMixed() throws Exception {
        final String appCheckpointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        final String checkpointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        final String savepointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();

        final Path expectedCheckpointPath = new Path(appCheckpointDir);
        final Path expectedSavepointPath = new Path(savepointDir);

        final MemoryStateBackend backend = new MemoryStateBackend(appCheckpointDir, null);

        final Configuration config = new Configuration();
        config.setString(backendKey, "filesystem"); // check that this is not accidentally picked up
        config.setString(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                checkpointDir); // this parameter should not be picked up
        config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

        StateBackend loadedBackend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        backend, TernaryBoolean.UNDEFINED, config, cl, null);
        assertThat(loadedBackend).isInstanceOf(MemoryStateBackend.class);

        final MemoryStateBackend memBackend = (MemoryStateBackend) loadedBackend;
        assertThat(memBackend.getCheckpointPath()).isEqualTo(expectedCheckpointPath);
        assertThat(memBackend.getSavepointPath()).isEqualTo(expectedSavepointPath);
    }

    // ------------------------------------------------------------------------
    //  File System State Backend
    // ------------------------------------------------------------------------

    /**
     * Validates loading a file system state backend with additional parameters from the cluster
     * configuration.
     */
    @Test
    void testLoadFileSystemStateBackend() throws Exception {
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
        config1.setString(backendKey, "filesystem");
        config1.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        config1.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
        config1.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, threshold);
        config1.setInteger(CheckpointingOptions.FS_WRITE_BUFFER_SIZE, minWriteBufferSize);

        final Configuration config2 = new Configuration();
        config2.setString(backendKey, FsStateBackendFactory.class.getName());
        config2.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        config2.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
        config2.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, threshold);
        config1.setInteger(CheckpointingOptions.FS_WRITE_BUFFER_SIZE, minWriteBufferSize);

        StateBackend backend1 = StateBackendLoader.loadStateBackendFromConfig(config1, cl, null);
        StateBackend backend2 = StateBackendLoader.loadStateBackendFromConfig(config2, cl, null);

        assertThat(backend1).isInstanceOf(HashMapStateBackend.class);
        assertThat(backend2).isInstanceOf(FsStateBackend.class);

        HashMapStateBackend fs1 = (HashMapStateBackend) backend1;
        FsStateBackend fs2 = (FsStateBackend) backend2;

        assertThat(fs2.getCheckpointPath()).isEqualTo(expectedCheckpointsPath);
        assertThat(fs2.getSavepointPath()).isEqualTo(expectedSavepointsPath);
        assertThat(fs2.getMinFileSizeThreshold()).isEqualTo(threshold.getBytes());
        assertThat(fs2.getWriteBufferSize())
                .isEqualTo(Math.max(threshold.getBytes(), minWriteBufferSize));
    }

    /**
     * Validates taking the application-defined file system state backend and adding with additional
     * parameters from the cluster configuration, but giving precedence to application-defined
     * parameters over configuration-defined parameters.
     */
    @Test
    void testLoadFileSystemStateBackendMixed() throws Exception {
        final String appCheckpointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        final String checkpointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();
        final String savepointDir = new Path(TempDirUtils.newFolder(tmp).toURI()).toString();

        final Path expectedCheckpointsPath = new Path(new URI(appCheckpointDir));
        final Path expectedSavepointsPath = new Path(savepointDir);

        final int threshold = 1000000;
        final int writeBufferSize = 4000000;

        final FsStateBackend backend =
                new FsStateBackend(
                        new URI(appCheckpointDir),
                        null,
                        threshold,
                        writeBufferSize,
                        TernaryBoolean.TRUE);

        final Configuration config = new Configuration();
        config.setString(backendKey, "jobmanager"); // this should not be picked up
        config.setString(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                checkpointDir); // this should not be picked up
        config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
        config.set(
                CheckpointingOptions.FS_SMALL_FILE_THRESHOLD,
                MemorySize.parse("20")); // this should not be picked up
        config.setInteger(
                CheckpointingOptions.FS_WRITE_BUFFER_SIZE, 3000000); // this should not be picked up

        final StateBackend loadedBackend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        backend, TernaryBoolean.UNDEFINED, config, cl, null);
        assertThat(loadedBackend).isInstanceOf(FsStateBackend.class);

        final FsStateBackend fs = (FsStateBackend) loadedBackend;
        assertThat(fs.getCheckpointPath()).isEqualTo(expectedCheckpointsPath);
        assertThat(fs.getSavepointPath()).isEqualTo(expectedSavepointsPath);
        assertThat(fs.getMinFileSizeThreshold()).isEqualTo(threshold);
        assertThat(fs.getWriteBufferSize()).isEqualTo(writeBufferSize);
    }

    // ------------------------------------------------------------------------
    //  Failures
    // ------------------------------------------------------------------------

    /**
     * This test makes sure that failures properly manifest when the state backend could not be
     * loaded.
     */
    @Test
    void testLoadingFails() throws Exception {
        final Configuration config = new Configuration();

        // try a value that is neither recognized as a name, nor corresponds to a class
        config.setString(backendKey, "does.not.exist");
        assertThatThrownBy(
                        () ->
                                StateBackendLoader.fromApplicationOrConfigOrDefault(
                                        null, TernaryBoolean.UNDEFINED, config, cl, null))
                .isInstanceOf(DynamicCodeLoadingException.class);

        // try a class that is not a factory
        config.setString(backendKey, java.io.File.class.getName());
        assertThatThrownBy(
                        () ->
                                StateBackendLoader.fromApplicationOrConfigOrDefault(
                                        null, TernaryBoolean.UNDEFINED, config, cl, null))
                .isInstanceOf(DynamicCodeLoadingException.class);

        // a factory that fails
        config.setString(backendKey, FailingFactory.class.getName());
        assertThatThrownBy(
                        () ->
                                StateBackendLoader.fromApplicationOrConfigOrDefault(
                                        null, TernaryBoolean.UNDEFINED, config, cl, null))
                .isInstanceOf(IOException.class);
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
        config1.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
        config1.setString(HighAvailabilityOptions.HA_CLUSTER_ID, "myCluster");
        config1.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haPersistenceDir);

        final Configuration config2 = new Configuration();
        config2.setString(backendKey, "jobmanager");
        config2.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
        config2.setString(HighAvailabilityOptions.HA_CLUSTER_ID, "myCluster");
        config2.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haPersistenceDir);

        if (checkpointPath != null) {
            config1.setString(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointPath.toUri().toString());
            config2.setString(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointPath.toUri().toString());
        }

        final MemoryStateBackend appBackend = new MemoryStateBackend();

        final StateBackend loaded1 =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        appBackend, TernaryBoolean.UNDEFINED, config1, cl, null);
        final StateBackend loaded2 =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        null, TernaryBoolean.UNDEFINED, config1, cl, null);
        final StateBackend loaded3 =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        null, TernaryBoolean.UNDEFINED, config2, cl, null);

        assertThat(loaded1).isInstanceOf(MemoryStateBackend.class);
        assertThat(loaded2).isInstanceOf(HashMapStateBackend.class);
        assertThat(loaded3).isInstanceOf(MemoryStateBackend.class);

        final MemoryStateBackend memBackend1 = (MemoryStateBackend) loaded1;
        final MemoryStateBackend memBackend2 = (MemoryStateBackend) loaded3;

        assertThat(memBackend1.getSavepointPath()).isNull();

        if (checkpointPath != null) {
            assertThat(memBackend1.getCheckpointPath()).isNotNull();
            assertThat(memBackend2.getCheckpointPath()).isNotNull();

            assertThat(memBackend1.getCheckpointPath()).isEqualTo(checkpointPath);
            assertThat(memBackend2.getCheckpointPath()).isEqualTo(checkpointPath);
        } else {
            assertThat(memBackend1.getCheckpointPath()).isNull();
            assertThat(memBackend2.getCheckpointPath()).isNull();
        }
    }

    // ------------------------------------------------------------------------

    static final class FailingFactory implements StateBackendFactory<StateBackend> {

        @Override
        public StateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader)
                throws IOException {
            throw new IOException("fail!");
        }
    }
}
