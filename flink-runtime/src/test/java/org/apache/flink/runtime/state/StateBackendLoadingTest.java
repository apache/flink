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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackendFactory;
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
    void testDefaultStateBackend() throws Exception {
        assertThat(StateBackendLoader.loadStateBackendFromConfig(new Configuration(), cl, null))
                .isInstanceOf(HashMapStateBackend.class);
    }

    @Test
    void testInstantiateHashMapStateBackendBackendByDefault() throws Exception {
        StateBackend backend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        null, new Configuration(), new Configuration(), cl, null);

        assertThat(backend).isInstanceOf(HashMapStateBackend.class);
    }

    @Test
    void testApplicationDefinedHasPrecedence() throws Exception {
        final StateBackend appBackend = Mockito.mock(StateBackend.class);

        final Configuration config = new Configuration();
        config.setString(backendKey, "hashmap");

        StateBackend backend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        appBackend, config, config, cl, null);
        assertThat(backend).isEqualTo(appBackend);
    }

    // ------------------------------------------------------------------------
    //  HashMap State Backend
    // ------------------------------------------------------------------------

    /** Validates loading a HashMapStateBackend from the cluster configuration. */
    @Test
    void testLoadHashMapStateBackendNoParameters() throws Exception {
        // we configure with the explicit string (rather than
        // AbstractStateBackend#X_STATE_BACKEND_NAME)
        // to guard against config-breaking changes of the name

        final Configuration config = new Configuration();
        config.setString(backendKey, HashMapStateBackendFactory.class.getName());

        StateBackend backend = StateBackendLoader.loadStateBackendFromConfig(config, cl, null);

        assertThat(backend).isInstanceOf(HashMapStateBackend.class);
    }

    /**
     * Validates loading a hashMap state backend with additional parameters from the cluster
     * configuration.
     */
    @Test
    void testLoadHashMapStateBackendWithParameters() throws Exception {
        // we configure with the explicit string (rather than
        // AbstractStateBackend#X_STATE_BACKEND_NAME)
        // to guard against config-breaking changes of the name

        final Configuration config1 = new Configuration();
        config1.setString(backendKey, "hashmap");

        final Configuration config2 = new Configuration();
        config2.setString(backendKey, HashMapStateBackendFactory.class.getName());

        HashMapStateBackend backend1 =
                (HashMapStateBackend)
                        StateBackendLoader.loadStateBackendFromConfig(config1, cl, null);
        HashMapStateBackend backend2 =
                (HashMapStateBackend)
                        StateBackendLoader.loadStateBackendFromConfig(config2, cl, null);

        assertThat(backend1).isNotNull();
        assertThat(backend2).isNotNull();
    }

    // ------------------------------------------------------------------------
    //  File System State Backend
    // ------------------------------------------------------------------------

    /**
     * Validates taking the application-defined file system state backend and adding with additional
     * parameters from configuration, but giving precedence to application-defined parameters over
     * configuration-defined parameters.
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
        config.setString(backendKey, "hashmap"); // this should not be picked up
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                checkpointDir); // this should not be picked up
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
        config.set(
                CheckpointingOptions.FS_SMALL_FILE_THRESHOLD,
                MemorySize.parse("20")); // this should not be picked up
        config.set(
                CheckpointingOptions.FS_WRITE_BUFFER_SIZE, 3000000); // this should not be picked up

        final StateBackend loadedBackend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        backend, config, config, cl, null);
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
                                        null, config, new Configuration(), cl, null))
                .isInstanceOf(DynamicCodeLoadingException.class);

        // try a class that is not a factory
        config.setString(backendKey, java.io.File.class.getName());
        assertThatThrownBy(
                        () ->
                                StateBackendLoader.fromApplicationOrConfigOrDefault(
                                        null, config, new Configuration(), cl, null))
                .isInstanceOf(DynamicCodeLoadingException.class);

        // a factory that fails
        config.setString(backendKey, FailingFactory.class.getName());
        assertThatThrownBy(
                        () ->
                                StateBackendLoader.fromApplicationOrConfigOrDefault(
                                        null, config, new Configuration(), cl, null))
                .isInstanceOf(IOException.class);
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
