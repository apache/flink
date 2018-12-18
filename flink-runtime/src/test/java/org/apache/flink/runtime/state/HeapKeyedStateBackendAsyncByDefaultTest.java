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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.IOUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertTrue;

/**
 * This tests that all heap-based state backends use asynchronous snapshots by default.
 */
public class HeapKeyedStateBackendAsyncByDefaultTest {

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	@Test
	public void testConfigOptionDefaultsToAsync() {
		assertTrue(CheckpointingOptions.ASYNC_SNAPSHOTS.defaultValue());
	}

	@Test
	public void testFsStateBackendDefaultsToAsync() throws Exception {
		FsStateBackend backend = new FsStateBackend(tmpFolder.newFolder().toURI());
		assertTrue(backend.isUsingAsynchronousSnapshots());

		validateSupportForAsyncSnapshots(backend);
	}

	@Test
	public void testMemoryStateBackendDefaultsToAsync() throws Exception {
		MemoryStateBackend backend = new MemoryStateBackend();
		assertTrue(backend.isUsingAsynchronousSnapshots());

		validateSupportForAsyncSnapshots(backend);
	}

	private void validateSupportForAsyncSnapshots(StateBackend backend) throws Exception {

		AbstractKeyedStateBackend<Integer> keyedStateBackend = backend.createKeyedStateBackend(
			new DummyEnvironment("Test", 1, 0),
			new JobID(),
			"testOperator",
			IntSerializer.INSTANCE,
			1,
			new KeyGroupRange(0, 0),
			null,
			TtlTimeProvider.DEFAULT
		);

		assertTrue(keyedStateBackend.supportsAsynchronousSnapshots());

		IOUtils.closeQuietly(keyedStateBackend);
		keyedStateBackend.dispose();
	}
}
