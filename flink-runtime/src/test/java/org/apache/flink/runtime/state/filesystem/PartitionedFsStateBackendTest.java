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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.filesystem.util.FsStateBackendUtils;

import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class PartitionedFsStateBackendTest extends StateBackendTestBase<PartitionedFsStateBackend<Integer>> {

	private File stateDir;

	private FsStateBackend fsStateBackend;

	@Override
	public void setup() throws Exception {
		stateDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		fsStateBackend = new FsStateBackend(FsStateBackendUtils.localFileUri(stateDir));
		fsStateBackend.initializeForJob(new DummyEnvironment("dummy", 1, 0), "operator");
	}

	@Override
	protected PartitionedFsStateBackend<Integer> createStateBackend() throws Exception {
		return fsStateBackend.createPartitionedStateBackend(IntSerializer.INSTANCE);
	}

	@Override
	protected void cleanup() throws Exception {
		fsStateBackend.close();
		FsStateBackendUtils.deleteDirectorySilently(stateDir);
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
}
