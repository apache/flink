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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Tests for the partitioned state part of {@link RocksDBStateBackend}.
 */
public class RocksDBStateBackendTest extends StateBackendTestBase<RocksDBStateBackend> {

	private File dbDir;
	private File chkDir;

	@Override
	protected RocksDBStateBackend getStateBackend() throws IOException {
		dbDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		chkDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());

		return new RocksDBStateBackend(dbDir.getAbsolutePath(), "file://" + chkDir.getAbsolutePath(), new MemoryStateBackend());
	}

	@Override
	protected void cleanup() {
		try {
			FileUtils.deleteDirectory(dbDir);
			FileUtils.deleteDirectory(chkDir);
		} catch (IOException ignore) {}
	}
}
