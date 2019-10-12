/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.filesystem.AbstractFileStateBackend;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpillableStateBackendFactoryTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private final ClassLoader cl = getClass().getClassLoader();

	private final String backendKey = CheckpointingOptions.STATE_BACKEND.key();

	// ------------------------------------------------------------------------

	@Test
	public void testFactoryName() {
		// construct the name such that it will not be automatically adjusted on refactorings
		String factoryName = "org.apache.flink.runtime.state.heap.Spill";
		factoryName += "ableStateBackendFactory";

		// !!! if this fails, the code in StateBackendLoader must be adjusted
		assertEquals(factoryName, SpillableStateBackendFactory.class.getName());
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

		final Path expectedCheckpointsPath = new Path(checkpointDir);
		final Path expectedSavepointsPath = new Path(savepointDir);

		// we configure with the explicit string (rather than AbstractStateBackend#X_STATE_BACKEND_NAME)
		// to guard against config-breaking changes of the name
		final Configuration config1 = new Configuration();
		config1.setString(backendKey, "spillable");
		config1.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config1.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		final Configuration config2 = new Configuration();
		config2.setString(backendKey, SpillableStateBackendFactory.class.getName());
		config2.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config2.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		StateBackend backend1 = StateBackendLoader.loadStateBackendFromConfig(config1, cl, null);
		StateBackend backend2 = StateBackendLoader.loadStateBackendFromConfig(config2, cl, null);

		assertTrue(backend1 instanceof SpillableStateBackend);
		assertTrue(backend2 instanceof SpillableStateBackend);

		SpillableStateBackend fs1 = (SpillableStateBackend) backend1;
		SpillableStateBackend fs2 = (SpillableStateBackend) backend2;

		AbstractFileStateBackend fs1back = (AbstractFileStateBackend) fs1.getCheckpointBackend();
		AbstractFileStateBackend fs2back = (AbstractFileStateBackend) fs2.getCheckpointBackend();

		assertEquals(expectedCheckpointsPath, fs1back.getCheckpointPath());
		assertEquals(expectedCheckpointsPath, fs2back.getCheckpointPath());
		assertEquals(expectedSavepointsPath, fs1back.getSavepointPath());
		assertEquals(expectedSavepointsPath, fs2back.getSavepointPath());
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

		final Path expectedCheckpointsPath = new Path(appCheckpointDir);
		final Path expectedSavepointsPath = new Path(savepointDir);

		final SpillableStateBackend backend = new SpillableStateBackend(appCheckpointDir);

		final Configuration config = new Configuration();
		config.setString(backendKey, "jobmanager"); // this should not be picked up
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir); // this should not be picked up
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		final StateBackend loadedBackend =
			StateBackendLoader.fromApplicationOrConfigOrDefault(backend, config, cl, null);
		assertTrue(loadedBackend instanceof SpillableStateBackend);

		final SpillableStateBackend loadedRocks = (SpillableStateBackend) loadedBackend;

		AbstractFileStateBackend fsBackend = (AbstractFileStateBackend) loadedRocks.getCheckpointBackend();
		assertEquals(expectedCheckpointsPath, fsBackend.getCheckpointPath());
		assertEquals(expectedSavepointsPath, fsBackend.getSavepointPath());
	}
}
