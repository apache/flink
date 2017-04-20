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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
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

	private final String backendKey = CoreOptions.STATE_BACKEND.key();

	// ------------------------------------------------------------------------

	@Test
	public void testNoStateBackendDefined() throws Exception {
		assertNull(AbstractStateBackend.loadStateBackendFromConfig(new Configuration(), cl, null));
	}

	@Test
	public void testInstantiateMemoryBackendByDefault() throws Exception {
		StateBackend backend = AbstractStateBackend
				.loadStateBackendFromConfigOrCreateDefault(new Configuration(), cl, null);

		assertTrue(backend instanceof MemoryStateBackend);
	}

	@Test
	public void testLoadMemoryStateBackend() throws Exception {
		// we configure with the explicit string (rather than AbstractStateBackend#X_STATE_BACKEND_NAME)
		// to guard against config-breaking changes of the name 
		final Configuration config = new Configuration();
		config.setString(backendKey, "jobmanager");

		StateBackend backend = AbstractStateBackend
				.loadStateBackendFromConfigOrCreateDefault(new Configuration(), cl, null);

		assertTrue(backend instanceof MemoryStateBackend);
	}

	@Test
	public void testLoadFileSystemStateBackend() throws Exception {
		final String checkpointDir = new Path(tmp.getRoot().toURI()).toString();
		final Path expectedPath = new Path(checkpointDir);
		final int threshold = 1000000;

		// we configure with the explicit string (rather than AbstractStateBackend#X_STATE_BACKEND_NAME)
		// to guard against config-breaking changes of the name 
		final Configuration config1 = new Configuration();
		config1.setString(backendKey, "filesystem");
		config1.setString("state.checkpoints.dir", checkpointDir);
		config1.setString("state.backend.fs.checkpointdir", checkpointDir);
		config1.setInteger("state.backend.fs.memory-threshold", threshold);

		final Configuration config2 = new Configuration();
		config2.setString(backendKey, FsStateBackendFactory.class.getName());
		config2.setString("state.checkpoints.dir", checkpointDir);
		config2.setString("state.backend.fs.checkpointdir", checkpointDir);
		config2.setInteger("state.backend.fs.memory-threshold", threshold);

		StateBackend backend1 = AbstractStateBackend
				.loadStateBackendFromConfigOrCreateDefault(config1, cl, null);

		StateBackend backend2 = AbstractStateBackend
				.loadStateBackendFromConfigOrCreateDefault(config2, cl, null);

		assertTrue(backend1 instanceof FsStateBackend);
		assertTrue(backend2 instanceof FsStateBackend);

		FsStateBackend fs1 = (FsStateBackend) backend1;
		FsStateBackend fs2 = (FsStateBackend) backend2;

		assertEquals(expectedPath, fs1.getBasePath());
		assertEquals(expectedPath, fs2.getBasePath());
		assertEquals(threshold, fs1.getMinFileSizeThreshold());
		assertEquals(threshold, fs2.getMinFileSizeThreshold());
	}

	/**
	 * This test makes sure that failures properly manifest when the state backend could not be loaded.
	 */
	@Test
	public void testLoadingFails() throws Exception {
		final Configuration config = new Configuration();

		// try a value that is neither recognized as a name, nor corresponds to a class
		config.setString(backendKey, "does.not.exist");
		try {
			AbstractStateBackend.loadStateBackendFromConfigOrCreateDefault(config, cl, null);
			fail("should fail with an exception");
		} catch (DynamicCodeLoadingException ignored) {
			// expected
		}

		// try a class that is not a factory
		config.setString(backendKey, java.io.File.class.getName());
		try {
			AbstractStateBackend.loadStateBackendFromConfigOrCreateDefault(config, cl, null);
			fail("should fail with an exception");
		} catch (DynamicCodeLoadingException ignored) {
			// expected
		}

		// a factory that fails
		config.setString(backendKey, FailingFactory.class.getName());
		try {
			AbstractStateBackend.loadStateBackendFromConfigOrCreateDefault(config, cl, null);
			fail("should fail with an exception");
		} catch (IOException ignored) {
			// expected
		}
	}

	// ------------------------------------------------------------------------

	static final class FailingFactory implements StateBackendFactory<StateBackend> {
		private static final long serialVersionUID = 1L;

		@Override
		public StateBackend createFromConfig(Configuration config) throws IllegalConfigurationException, IOException {
			throw new IOException("fail!");
		}
	}
}
