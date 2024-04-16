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

package org.apache.flink.state.forst;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests for the ForStStateBackendFactory. */
public class ForStStateBackendFactoryTest {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private final ClassLoader cl = getClass().getClassLoader();

    private final String backendKey = StateBackendOptions.STATE_BACKEND.key();

    // ------------------------------------------------------------------------

    @Test
    public void testEmbeddedFactoryName() {
        // construct the name such that it will not be automatically adjusted on refactorings
        String factoryName = "org.apache.flink.state.forst.For";
        factoryName += "StStateBackendFactory";

        // !!! if this fails, the code in StateBackendLoader must be adjusted
        assertEquals(factoryName, ForStStateBackendFactory.class.getName());
    }

    /**
     * Validates loading a file system state backend with additional parameters from the cluster
     * configuration.
     */
    @Test
    public void testLoadForStStateBackend() throws Exception {
        final String localDir1 = tmp.newFolder().getAbsolutePath();
        final String localDir2 = tmp.newFolder().getAbsolutePath();
        final String localDirs = localDir1 + File.pathSeparator + localDir2;
        final boolean incremental = !CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue();

        // TODO: Support short name of backendKey

        final Configuration config1 = new Configuration();
        config1.setString(backendKey, ForStStateBackendFactory.class.getName());
        config1.set(ForStOptions.LOCAL_DIRECTORIES, localDirs);
        config1.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, incremental);

        StateBackend backend1 = StateBackendLoader.loadStateBackendFromConfig(config1, cl, null);

        assertTrue(backend1 instanceof ForStStateBackend);

        ForStStateBackend fs1 = (ForStStateBackend) backend1;

        checkPaths(fs1.getLocalDbStoragePaths(), localDir1, localDir2);
    }

    /**
     * Validates taking the application-defined ForSt state backend and adding with additional
     * parameters from the cluster configuration, but giving precedence to application-defined
     * parameters over configuration-defined parameters.
     */
    @Test
    public void testLoadForStStateBackendMixed() throws Exception {
        final String localDir1 = tmp.newFolder().getAbsolutePath();
        final String localDir2 = tmp.newFolder().getAbsolutePath();
        final String localDir3 = tmp.newFolder().getAbsolutePath();
        final String localDir4 = tmp.newFolder().getAbsolutePath();

        final ForStStateBackend backend = new ForStStateBackend();
        backend.setLocalDbStoragePaths(localDir1, localDir2);

        final Configuration config = new Configuration();
        config.setString(backendKey, "hashmap"); // this should not be picked up
        config.set(
                ForStOptions.LOCAL_DIRECTORIES,
                localDir3 + ":" + localDir4); // this should not be picked up

        final StateBackend loadedBackend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        backend, new Configuration(), config, cl, null);
        assertTrue(loadedBackend instanceof ForStStateBackend);

        final ForStStateBackend loadedRocks = (ForStStateBackend) loadedBackend;

        checkPaths(loadedRocks.getLocalDbStoragePaths(), localDir1, localDir2);
    }

    // ------------------------------------------------------------------------

    private static void checkPaths(String[] pathsArray, String... paths) {
        assertNotNull(pathsArray);
        assertNotNull(paths);

        assertEquals(pathsArray.length, paths.length);

        HashSet<String> pathsSet = new HashSet<>(Arrays.asList(pathsArray));

        for (String path : paths) {
            assertTrue(pathsSet.contains(path));
        }
    }
}
