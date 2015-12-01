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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SavepointStoreFactoryTest {

	@Test
	public void testStateStoreWithDefaultConfig() throws Exception {
		SavepointStore store = SavepointStoreFactory.createFromConfig(new Configuration());
		assertTrue(store.getStateStore() instanceof HeapStateStore);
	}

	@Test
	public void testSavepointBackendJobManager() throws Exception {
		Configuration config = new Configuration();
		config.setString(SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "jobmanager");
		SavepointStore store = SavepointStoreFactory.createFromConfig(config);
		assertTrue(store.getStateStore() instanceof HeapStateStore);
	}

	@Test
	public void testSavepointBackendFileSystem() throws Exception {
		Configuration config = new Configuration();
		String rootPath = System.getProperty("java.io.tmpdir");
		config.setString(ConfigConstants.STATE_BACKEND, "filesystem");
		config.setString(SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "filesystem");
		config.setString(SavepointStoreFactory.SAVEPOINT_DIRECTORY_KEY, rootPath);

		SavepointStore store = SavepointStoreFactory.createFromConfig(config);
		assertTrue(store.getStateStore() instanceof FileSystemStateStore);

		FileSystemStateStore<Savepoint> stateStore = (FileSystemStateStore<Savepoint>)
				store.getStateStore();
		assertEquals(new Path(rootPath), stateStore.getRootPath());
	}

	@Test
	public void testSavepointBackendFileSystemButCheckpointBackendJobManager() throws Exception {
		Configuration config = new Configuration();

		// This combination does not make sense, because the checkpoints will be
		// lost after the job manager shuts down.
		config.setString(ConfigConstants.STATE_BACKEND, "jobmanager");
		config.setString(SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "filesystem");
		SavepointStore store = SavepointStoreFactory.createFromConfig(config);
		assertTrue(store.getStateStore() instanceof HeapStateStore);
	}

	@Test
	public void testSavepointBackendFileSystemButNoDirectory() throws Exception {
		Configuration config = new Configuration();
		config.setString(SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "filesystem");
		SavepointStore store = SavepointStoreFactory.createFromConfig(config);
		assertTrue(store.getStateStore() instanceof HeapStateStore);
	}

	@Test
	public void testUnexpectedSavepointBackend() throws Exception {
		Configuration config = new Configuration();
		config.setString(SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "unexpected");
		SavepointStore store = SavepointStoreFactory.createFromConfig(config);
		assertTrue(store.getStateStore() instanceof HeapStateStore);
	}
}
