/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.IOUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * This tests that all heap-based {@link StateBackend}s create {@link KeyedStateBackend}s that use asynchronous
 * snapshots by default.
 */
public class HeapKeyedStateBackendAsyncByDefaultTest {

	@Test
	public void testFsStateBackendDefaultsToAsync() throws Exception {
		TemporaryFolder temporaryFolder = new TemporaryFolder();
		temporaryFolder.create();

		File folder = temporaryFolder.newFolder();

		try {
			// This backend has two constructors that use a default value for async snapshots.
			FsStateBackend fsStateBackend = new FsStateBackend(folder.toURI());
			validateSupportForAsyncSnapshots(fsStateBackend);

			fsStateBackend = new FsStateBackend(folder.toURI(), 1024);
			validateSupportForAsyncSnapshots(fsStateBackend);
		} finally {
			folder.delete();
			temporaryFolder.delete();
		}
	}

	@Test
	public void testMemoryStateBackendDefaultsToAsync() throws Exception {
		MemoryStateBackend memoryStateBackend = new MemoryStateBackend();
		validateSupportForAsyncSnapshots(memoryStateBackend);
	}

	private void validateSupportForAsyncSnapshots(AbstractStateBackend backend) throws IOException {

		AbstractKeyedStateBackend<Integer> keyedStateBackend = backend.createKeyedStateBackend(
			new DummyEnvironment("Test", 1, 0),
			new JobID(),
			"testOperator",
			IntSerializer.INSTANCE,
			1,
			new KeyGroupRange(0, 0),
			null
		);

		try {
			Assert.assertTrue(keyedStateBackend.supportsAsynchronousSnapshots());
		} finally {
			IOUtils.closeQuietly(keyedStateBackend);
			keyedStateBackend.dispose();
		}
	}
}
