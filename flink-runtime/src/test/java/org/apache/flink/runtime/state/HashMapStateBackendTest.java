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

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for the keyed state backend and operator state backend, as created by the
 * {@link HashMapStateBackend}.
 */
@RunWith(Parameterized.class)
public class HashMapStateBackendTest extends StateBackendTestBase<HashMapStateBackend> {

	@Parameterized.Parameters
	public static List<Object[]> modes() {
		return Arrays.asList(new Object[][] {
				{true, CheckpointStorageType.JM},
				{false, CheckpointStorageType.FILE}
		});
	}

	@Parameterized.Parameter(value = 0)
	public boolean useAsyncMode;

	@Parameterized.Parameter(value = 1)
	public CheckpointStorageType storageType;

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Override
	protected HashMapStateBackend getStateBackend() {
		return new HashMapStateBackend(useAsyncMode);
	}

	@Override
	protected CheckpointStorage getCheckpointStorage() throws Exception {
		switch (storageType) {
			case JM:
				return new JobManagerCheckpointStorage();
			case FILE:
				String checkpointPath = tempFolder.newFolder().toURI().toString();
				return new FileSystemCheckpointStorage(checkpointPath);
		}

		throw new IllegalStateException("Unknown storage type " + storageType);
	}

	@Override
	protected boolean isSerializerPresenceRequiredOnRestore() {
		return true;
	}

	// disable these because the verification does not work for this state backend
	@Override
	@Test
	public void testValueStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testListStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testReducingStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testMapStateRestoreWithWrongSerializers() {}

	@Ignore
	@Test
	public void testConcurrentMapIfQueryable() throws Exception {
		super.testConcurrentMapIfQueryable();
	}

	enum CheckpointStorageType {
		JM,
		FILE
	}
}

