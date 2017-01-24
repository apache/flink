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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.OperatingSystem;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the partitioned state part of {@link RocksDBStateBackend}.
 */
public class RocksDBStateBackendTest extends StateBackendTestBase<RocksDBStateBackend> {

	private File dbDir;
	private File chkDir;

	@Before
	public void checkOperatingSystem() {
		Assume.assumeTrue("This test can't run successfully on Windows.", !OperatingSystem.isWindows());
	}

	@Override
	protected RocksDBStateBackend getStateBackend() throws IOException {
		dbDir = new File(new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString()), "state");
		chkDir = new File(new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString()), "snapshots");

		RocksDBStateBackend backend = new RocksDBStateBackend(chkDir.getAbsoluteFile().toURI(), new MemoryStateBackend());
		backend.setDbStoragePath(dbDir.getAbsolutePath());
		return backend;
	}

	@Test
	public void testDisposeDeletesAllDirectories() throws Exception {

		backend.initializeForJob(
				new DummyEnvironment("test", 1, 0),
				"test_op",
				IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId =
				new ValueStateDescriptor<>("id", String.class, null);

		kvId.initializeSerializerUnlessSet(new ExecutionConfig());

		ValueState<String> state =
				backend.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

		backend.setCurrentKey(1);
		state.update("Hello");


		Collection<File> allFilesInDbDir =
				FileUtils.listFilesAndDirs(dbDir, new AcceptAllFilter(), new AcceptAllFilter());

		// more than just the root directory
		assertTrue(allFilesInDbDir.size() > 1);

		backend.dispose();

		allFilesInDbDir =
				FileUtils.listFilesAndDirs(dbDir, new AcceptAllFilter(), new AcceptAllFilter());

		// just the root directory left
		assertEquals(1, allFilesInDbDir.size());
	}

	@Override
	protected void cleanup() {
		try {
			FileUtils.deleteDirectory(dbDir);
			FileUtils.deleteDirectory(chkDir);
		} catch (IOException ignore) {}
	}

	private static class AcceptAllFilter implements IOFileFilter {
		@Override
		public boolean accept(File file) {
			return true;
		}

		@Override
		public boolean accept(File file, String s) {
			return true;
		}
	}
}
