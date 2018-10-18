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

import org.apache.flink.runtime.state.StateBackendMigrationTestBase;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.rocksdb.RocksObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * Tests for the partitioned state part of {@link RocksDBStateBackend}.
 */
@RunWith(Parameterized.class)
public class RocksDBStateBackendMigrationTest extends StateBackendMigrationTestBase<RocksDBStateBackend> {

	private RocksDBKeyedStateBackend<Integer> keyedStateBackend;
	private List<RocksObject> allCreatedCloseables;

	@Parameterized.Parameters(name = "Incremental checkpointing: {0}")
	public static Collection<Boolean> parameters() {
		return Arrays.asList(false, true);
	}

	@Parameterized.Parameter
	public boolean enableIncrementalCheckpointing;

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	// Store it because we need it for the cleanup test.
	private String dbPath;

	@Override
	protected RocksDBStateBackend getStateBackend() throws IOException {
		dbPath = tempFolder.newFolder().getAbsolutePath();
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend backend = new RocksDBStateBackend(new FsStateBackend(checkpointPath), enableIncrementalCheckpointing);
		backend.setDbStoragePath(dbPath);
		return backend;
	}

	@Override
	protected BackendSerializationTimeliness getStateBackendSerializationTimeliness() {
		return BackendSerializationTimeliness.ON_ACCESS;
	}

	// small safety net for instance cleanups, so that no native objects are left
	@After
	public void cleanupRocksDB() {
		if (keyedStateBackend != null) {
			IOUtils.closeQuietly(keyedStateBackend);
			keyedStateBackend.dispose();
		}
		if (allCreatedCloseables != null) {
			for (RocksObject rocksCloseable : allCreatedCloseables) {
				verify(rocksCloseable, times(1)).close();
			}
			allCreatedCloseables = null;
		}
	}
}
