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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.filesystem.AbstractFileStateBackend;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the RocksDBStateBackendFactory.
 */
public class RocksDBStateBackendFactoryTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private final ClassLoader cl = getClass().getClassLoader();

	private final String backendKey = CheckpointingOptions.STATE_BACKEND.key();

	// ------------------------------------------------------------------------

	@Test
	public void testFactoryName() {
		// construct the name such that it will not be automatically adjusted on refactorings
		String factoryName = "org.apache.flink.contrib.streaming.state.Roc";
		factoryName += "ksDBStateBackendFactory";

		// !!! if this fails, the code in StateBackendLoader must be adjusted
		assertEquals(factoryName, RocksDBStateBackendFactory.class.getName());
	}

	/**
	 * Validates loading a file system state backend with additional parameters from the cluster configuration.
	 */
	@Test
	public void testLoadFileSystemStateBackend() throws Exception {
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();
		final String localDir1 = tmp.newFolder().getAbsolutePath();
		final String localDir2 = tmp.newFolder().getAbsolutePath();
		final String localDirs = localDir1 + File.pathSeparator + localDir2;
		final boolean incremental = !CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue();

		final Path expectedCheckpointsPath = new Path(checkpointDir);
		final Path expectedSavepointsPath = new Path(savepointDir);

		// we configure with the explicit string (rather than AbstractStateBackend#X_STATE_BACKEND_NAME)
		// to guard against config-breaking changes of the name
		final Configuration config1 = new Configuration();
		config1.setString(backendKey, "rocksdb");
		config1.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config1.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config1.setString(RocksDBOptions.LOCAL_DIRECTORIES, localDirs);
		config1.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, incremental);

		final Configuration config2 = new Configuration();
		config2.setString(backendKey, RocksDBStateBackendFactory.class.getName());
		config2.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config2.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config2.setString(RocksDBOptions.LOCAL_DIRECTORIES, localDirs);
		config2.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, incremental);

		StateBackend backend1 = StateBackendLoader.loadStateBackendFromConfig(config1, cl, null);
		StateBackend backend2 = StateBackendLoader.loadStateBackendFromConfig(config2, cl, null);

		assertTrue(backend1 instanceof RocksDBStateBackend);
		assertTrue(backend2 instanceof RocksDBStateBackend);

		RocksDBStateBackend fs1 = (RocksDBStateBackend) backend1;
		RocksDBStateBackend fs2 = (RocksDBStateBackend) backend2;

		AbstractFileStateBackend fs1back = (AbstractFileStateBackend) fs1.getCheckpointBackend();
		AbstractFileStateBackend fs2back = (AbstractFileStateBackend) fs2.getCheckpointBackend();

		assertEquals(expectedCheckpointsPath, fs1back.getCheckpointPath());
		assertEquals(expectedCheckpointsPath, fs2back.getCheckpointPath());
		assertEquals(expectedSavepointsPath, fs1back.getSavepointPath());
		assertEquals(expectedSavepointsPath, fs2back.getSavepointPath());
		assertEquals(incremental, fs1.isIncrementalCheckpointsEnabled());
		assertEquals(incremental, fs2.isIncrementalCheckpointsEnabled());
		checkPaths(fs1.getDbStoragePaths(), localDir1, localDir2);
		checkPaths(fs2.getDbStoragePaths(), localDir1, localDir2);
	}

	/**
	 * Validates taking the application-defined file system state backend and adding with additional
	 * parameters from the cluster configuration, but giving precedence to application-defined
	 * parameters over configuration-defined parameters.
	 */
	@Test
	public void testLoadFileSystemStateBackendMixed() throws Exception {
		final String appCheckpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();

		final String localDir1 = tmp.newFolder().getAbsolutePath();
		final String localDir2 = tmp.newFolder().getAbsolutePath();
		final String localDir3 = tmp.newFolder().getAbsolutePath();
		final String localDir4 = tmp.newFolder().getAbsolutePath();

		final boolean incremental = !CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue();

		final Path expectedCheckpointsPath = new Path(appCheckpointDir);
		final Path expectedSavepointsPath = new Path(savepointDir);

		final RocksDBStateBackend backend = new RocksDBStateBackend(appCheckpointDir, incremental);
		backend.setDbStoragePaths(localDir1, localDir2);

		final Configuration config = new Configuration();
		config.setString(backendKey, "jobmanager"); // this should not be picked up
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir); // this should not be picked up
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, !incremental);  // this should not be picked up
		config.setString(RocksDBOptions.LOCAL_DIRECTORIES, localDir3 + ":" + localDir4);  // this should not be picked up

		final StateBackend loadedBackend =
				StateBackendLoader.fromApplicationOrConfigOrDefault(backend, config, cl, null);
		assertTrue(loadedBackend instanceof RocksDBStateBackend);

		final RocksDBStateBackend loadedRocks = (RocksDBStateBackend) loadedBackend;

		assertEquals(incremental, loadedRocks.isIncrementalCheckpointsEnabled());
		checkPaths(loadedRocks.getDbStoragePaths(), localDir1, localDir2);

		AbstractFileStateBackend fsBackend = (AbstractFileStateBackend) loadedRocks.getCheckpointBackend();
		assertEquals(expectedCheckpointsPath, fsBackend.getCheckpointPath());
		assertEquals(expectedSavepointsPath, fsBackend.getSavepointPath());
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
