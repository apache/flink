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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackendFactory;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test validates that state backends are properly loaded from configuration.
 */
public class StateBackendLoadingTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private final ClassLoader cl = getClass().getClassLoader();

	private final String backendKey = CheckpointingOptions.STATE_BACKEND.key();

	// ------------------------------------------------------------------------
	//  defaults
	// ------------------------------------------------------------------------

	@Test
	public void testNoStateBackendDefined() throws Exception {
		assertNull(StateBackendLoader.loadStateBackendFromConfig(new Configuration(), cl, null));
	}

	@Test
	public void testInstantiateMemoryBackendByDefault() throws Exception {
		StateBackend backend =
				StateBackendLoader.fromApplicationOrConfigOrDefault(null, new Configuration(), cl, null);

		assertTrue(backend instanceof MemoryStateBackend);
	}

	@Test
	public void testApplicationDefinedHasPrecedence() throws Exception {
		final StateBackend appBackend = Mockito.mock(StateBackend.class);

		final Configuration config = new Configuration();
		config.setString(backendKey, "jobmanager");

		StateBackend backend = StateBackendLoader.fromApplicationOrConfigOrDefault(appBackend, config, cl, null);
		assertEquals(appBackend, backend);
	}

	// ------------------------------------------------------------------------
	//  Memory State Backend
	// ------------------------------------------------------------------------

	/**
	 * Validates loading a memory state backend from the cluster configuration.
	 */
	@Test
	public void testLoadMemoryStateBackendNoParameters() throws Exception {
		// we configure with the explicit string (rather than AbstractStateBackend#X_STATE_BACKEND_NAME)
		// to guard against config-breaking changes of the name

		final Configuration config1 = new Configuration();
		config1.setString(backendKey, "jobmanager");

		final Configuration config2 = new Configuration();
		config2.setString(backendKey, MemoryStateBackendFactory.class.getName());

		StateBackend backend1 = StateBackendLoader.loadStateBackendFromConfig(config1, cl, null);
		StateBackend backend2 = StateBackendLoader.loadStateBackendFromConfig(config2, cl, null);

		assertTrue(backend1 instanceof MemoryStateBackend);
		assertTrue(backend2 instanceof MemoryStateBackend);
	}

	/**
	 * Validates loading a memory state backend with additional parameters from the cluster configuration.
	 */
	@Test
	public void testLoadMemoryStateWithParameters() throws Exception {
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();
		final Path expectedCheckpointPath = new Path(checkpointDir);
		final Path expectedSavepointPath = new Path(savepointDir);

		final boolean async = !CheckpointingOptions.ASYNC_SNAPSHOTS.defaultValue();

		// we configure with the explicit string (rather than AbstractStateBackend#X_STATE_BACKEND_NAME)
		// to guard against config-breaking changes of the name

		final Configuration config1 = new Configuration();
		config1.setString(backendKey, "jobmanager");
		config1.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config1.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config1.setBoolean(CheckpointingOptions.ASYNC_SNAPSHOTS, async);

		final Configuration config2 = new Configuration();
		config2.setString(backendKey, MemoryStateBackendFactory.class.getName());
		config2.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config2.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config2.setBoolean(CheckpointingOptions.ASYNC_SNAPSHOTS, async);

		MemoryStateBackend backend1 = (MemoryStateBackend)
				StateBackendLoader.loadStateBackendFromConfig(config1, cl, null);
		MemoryStateBackend backend2 = (MemoryStateBackend)
				StateBackendLoader.loadStateBackendFromConfig(config2, cl, null);

		assertNotNull(backend1);
		assertNotNull(backend2);

		assertEquals(expectedCheckpointPath, backend1.getCheckpointPath());
		assertEquals(expectedCheckpointPath, backend2.getCheckpointPath());
		assertEquals(expectedSavepointPath, backend1.getSavepointPath());
		assertEquals(expectedSavepointPath, backend2.getSavepointPath());
		assertEquals(async, backend1.isUsingAsynchronousSnapshots());
		assertEquals(async, backend2.isUsingAsynchronousSnapshots());
	}

	/**
	 * Validates taking the application-defined memory state backend and adding additional
	 * parameters from the cluster configuration.
	 */
	@Test
	public void testConfigureMemoryStateBackend() throws Exception {
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();
		final Path expectedCheckpointPath = new Path(checkpointDir);
		final Path expectedSavepointPath = new Path(savepointDir);

		final int maxSize = 100;
		final boolean async = !CheckpointingOptions.ASYNC_SNAPSHOTS.defaultValue();

		final MemoryStateBackend backend = new MemoryStateBackend(maxSize, async);

		final Configuration config = new Configuration();
		config.setString(backendKey, "filesystem"); // check that this is not accidentally picked up
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config.setBoolean(CheckpointingOptions.ASYNC_SNAPSHOTS, !async);

		StateBackend loadedBackend = StateBackendLoader.fromApplicationOrConfigOrDefault(backend, config, cl, null);
		assertTrue(loadedBackend instanceof MemoryStateBackend);

		final MemoryStateBackend memBackend = (MemoryStateBackend) loadedBackend;
		assertEquals(expectedCheckpointPath, memBackend.getCheckpointPath());
		assertEquals(expectedSavepointPath, memBackend.getSavepointPath());
		assertEquals(maxSize, memBackend.getMaxStateSize());
		assertEquals(async, memBackend.isUsingAsynchronousSnapshots());
	}

	/**
	 * Validates taking the application-defined memory state backend and adding additional
	 * parameters from the cluster configuration, but giving precedence to application-defined
	 * parameters over configuration-defined parameters.
	 */
	@Test
	public void testConfigureMemoryStateBackendMixed() throws Exception {
		final String appCheckpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();

		final Path expectedCheckpointPath = new Path(appCheckpointDir);
		final Path expectedSavepointPath = new Path(savepointDir);

		final MemoryStateBackend backend = new MemoryStateBackend(appCheckpointDir, null);

		final Configuration config = new Configuration();
		config.setString(backendKey, "filesystem"); // check that this is not accidentally picked up
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir); // this parameter should not be picked up
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		StateBackend loadedBackend = StateBackendLoader.fromApplicationOrConfigOrDefault(backend, config, cl, null);
		assertTrue(loadedBackend instanceof MemoryStateBackend);

		final MemoryStateBackend memBackend = (MemoryStateBackend) loadedBackend;
		assertEquals(expectedCheckpointPath, memBackend.getCheckpointPath());
		assertEquals(expectedSavepointPath, memBackend.getSavepointPath());
	}

	// ------------------------------------------------------------------------
	//  File System State Backend
	// ------------------------------------------------------------------------

	/**
	 * Validates loading a file system state backend with additional parameters from the cluster configuration.
	 */
	@Test
	public void testLoadFileSystemStateBackend() throws Exception {
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();
		final Path expectedCheckpointsPath = new Path(checkpointDir);
		final Path expectedSavepointsPath = new Path(savepointDir);
		final int threshold = 1000000;
		final boolean async = !CheckpointingOptions.ASYNC_SNAPSHOTS.defaultValue();

		// we configure with the explicit string (rather than AbstractStateBackend#X_STATE_BACKEND_NAME)
		// to guard against config-breaking changes of the name 
		final Configuration config1 = new Configuration();
		config1.setString(backendKey, "filesystem");
		config1.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config1.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config1.setInteger(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, threshold);
		config1.setBoolean(CheckpointingOptions.ASYNC_SNAPSHOTS, async);

		final Configuration config2 = new Configuration();
		config2.setString(backendKey, FsStateBackendFactory.class.getName());
		config2.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config2.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config2.setInteger(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, threshold);
		config2.setBoolean(CheckpointingOptions.ASYNC_SNAPSHOTS, async);

		StateBackend backend1 = StateBackendLoader.loadStateBackendFromConfig(config1, cl, null);
		StateBackend backend2 = StateBackendLoader.loadStateBackendFromConfig(config2, cl, null);

		assertTrue(backend1 instanceof FsStateBackend);
		assertTrue(backend2 instanceof FsStateBackend);

		FsStateBackend fs1 = (FsStateBackend) backend1;
		FsStateBackend fs2 = (FsStateBackend) backend2;

		assertEquals(expectedCheckpointsPath, fs1.getCheckpointPath());
		assertEquals(expectedCheckpointsPath, fs2.getCheckpointPath());
		assertEquals(expectedSavepointsPath, fs1.getSavepointPath());
		assertEquals(expectedSavepointsPath, fs2.getSavepointPath());
		assertEquals(threshold, fs1.getMinFileSizeThreshold());
		assertEquals(threshold, fs2.getMinFileSizeThreshold());
		assertEquals(async, fs1.isUsingAsynchronousSnapshots());
		assertEquals(async, fs2.isUsingAsynchronousSnapshots());
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

		final Path expectedCheckpointsPath = new Path(new URI(appCheckpointDir));
		final Path expectedSavepointsPath = new Path(savepointDir);

		final int threshold = 1000000;

		final FsStateBackend backend = new FsStateBackend(new URI(appCheckpointDir), threshold);

		final Configuration config = new Configuration();
		config.setString(backendKey, "jobmanager"); // this should not be picked up 
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir); // this should not be picked up
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config.setInteger(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, 20); // this should not be picked up

		final StateBackend loadedBackend =
				StateBackendLoader.fromApplicationOrConfigOrDefault(backend, config, cl, null);
		assertTrue(loadedBackend instanceof FsStateBackend);

		final FsStateBackend fs = (FsStateBackend) loadedBackend;
		assertEquals(expectedCheckpointsPath, fs.getCheckpointPath());
		assertEquals(expectedSavepointsPath, fs.getSavepointPath());
		assertEquals(threshold, fs.getMinFileSizeThreshold());
	}

	// ------------------------------------------------------------------------
	//  Failures
	// ------------------------------------------------------------------------

	/**
	 * This test makes sure that failures properly manifest when the state backend could not be loaded.
	 */
	@Test
	public void testLoadingFails() throws Exception {
		final Configuration config = new Configuration();

		// try a value that is neither recognized as a name, nor corresponds to a class
		config.setString(backendKey, "does.not.exist");
		try {
			StateBackendLoader.fromApplicationOrConfigOrDefault(null, config, cl, null);
			fail("should fail with an exception");
		} catch (DynamicCodeLoadingException ignored) {
			// expected
		}

		// try a class that is not a factory
		config.setString(backendKey, java.io.File.class.getName());
		try {
			StateBackendLoader.fromApplicationOrConfigOrDefault(null, config, cl, null);
			fail("should fail with an exception");
		} catch (DynamicCodeLoadingException ignored) {
			// expected
		}

		// a factory that fails
		config.setString(backendKey, FailingFactory.class.getName());
		try {
			StateBackendLoader.fromApplicationOrConfigOrDefault(null, config, cl, null);
			fail("should fail with an exception");
		} catch (IOException ignored) {
			// expected
		}
	}

	// ------------------------------------------------------------------------
	//  High-availability default
	// ------------------------------------------------------------------------

	/**
	 * This tests that in the case of configured high-availability, the memory state backend
	 * automatically grabs the HA persistence directory.
	 */
	@Test
	public void testHighAvailabilityDefaultFallback() throws Exception {
		final String haPersistenceDir = new Path(tmp.newFolder().toURI()).toString();
		final Path expectedCheckpointPath = new Path(haPersistenceDir);

		final Configuration config1 = new Configuration();
		config1.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
		config1.setString(HighAvailabilityOptions.HA_CLUSTER_ID, "myCluster");
		config1.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haPersistenceDir);

		final Configuration config2 = new Configuration();
		config2.setString(backendKey, "jobmanager");
		config2.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
		config2.setString(HighAvailabilityOptions.HA_CLUSTER_ID, "myCluster");
		config2.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haPersistenceDir);

		final MemoryStateBackend appBackend = new MemoryStateBackend();

		final StateBackend loaded1 = StateBackendLoader.fromApplicationOrConfigOrDefault(appBackend, config1, cl, null);
		final StateBackend loaded2 = StateBackendLoader.fromApplicationOrConfigOrDefault(null, config1, cl, null);
		final StateBackend loaded3 = StateBackendLoader.fromApplicationOrConfigOrDefault(null, config2, cl, null);

		assertTrue(loaded1 instanceof MemoryStateBackend);
		assertTrue(loaded2 instanceof MemoryStateBackend);
		assertTrue(loaded3 instanceof MemoryStateBackend);

		final MemoryStateBackend memBackend1 = (MemoryStateBackend) loaded1;
		final MemoryStateBackend memBackend2 = (MemoryStateBackend) loaded2;
		final MemoryStateBackend memBackend3 = (MemoryStateBackend) loaded3;

		assertNotNull(memBackend1.getCheckpointPath());
		assertNotNull(memBackend2.getCheckpointPath());
		assertNotNull(memBackend3.getCheckpointPath());
		assertNull(memBackend1.getSavepointPath());
		assertNull(memBackend2.getSavepointPath());
		assertNull(memBackend3.getSavepointPath());

		assertEquals(expectedCheckpointPath, memBackend1.getCheckpointPath().getParent());
		assertEquals(expectedCheckpointPath, memBackend2.getCheckpointPath().getParent());
		assertEquals(expectedCheckpointPath, memBackend3.getCheckpointPath().getParent());
	}

	@Test
	public void testHighAvailabilityDefaultFallbackLocalPaths() throws Exception {
		final String haPersistenceDir = new Path(tmp.newFolder().getAbsolutePath()).toString();
		final Path expectedCheckpointPath = new Path(haPersistenceDir).makeQualified(FileSystem.getLocalFileSystem());

		final Configuration config1 = new Configuration();
		config1.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
		config1.setString(HighAvailabilityOptions.HA_CLUSTER_ID, "myCluster");
		config1.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haPersistenceDir);

		final Configuration config2 = new Configuration();
		config2.setString(backendKey, "jobmanager");
		config2.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
		config2.setString(HighAvailabilityOptions.HA_CLUSTER_ID, "myCluster");
		config2.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haPersistenceDir);

		final MemoryStateBackend appBackend = new MemoryStateBackend();

		final StateBackend loaded1 = StateBackendLoader.fromApplicationOrConfigOrDefault(appBackend, config1, cl, null);
		final StateBackend loaded2 = StateBackendLoader.fromApplicationOrConfigOrDefault(null, config1, cl, null);
		final StateBackend loaded3 = StateBackendLoader.fromApplicationOrConfigOrDefault(null, config2, cl, null);

		assertTrue(loaded1 instanceof MemoryStateBackend);
		assertTrue(loaded2 instanceof MemoryStateBackend);
		assertTrue(loaded3 instanceof MemoryStateBackend);

		final MemoryStateBackend memBackend1 = (MemoryStateBackend) loaded1;
		final MemoryStateBackend memBackend2 = (MemoryStateBackend) loaded2;
		final MemoryStateBackend memBackend3 = (MemoryStateBackend) loaded3;

		assertNotNull(memBackend1.getCheckpointPath());
		assertNotNull(memBackend2.getCheckpointPath());
		assertNotNull(memBackend3.getCheckpointPath());
		assertNull(memBackend1.getSavepointPath());
		assertNull(memBackend2.getSavepointPath());
		assertNull(memBackend3.getSavepointPath());

		assertEquals(expectedCheckpointPath, memBackend1.getCheckpointPath().getParent());
		assertEquals(expectedCheckpointPath, memBackend2.getCheckpointPath().getParent());
		assertEquals(expectedCheckpointPath, memBackend3.getCheckpointPath().getParent());
	}

	// ------------------------------------------------------------------------

	static final class FailingFactory implements StateBackendFactory<StateBackend> {

		@Override
		public StateBackend createFromConfig(Configuration config) throws IOException {
			throw new IOException("fail!");
		}
	}
}
