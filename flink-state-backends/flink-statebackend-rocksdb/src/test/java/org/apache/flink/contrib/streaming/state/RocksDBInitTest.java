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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.contrib.streaming.state.RocksDBStateBackend.ROCKSDB_LIB_LOADING_ATTEMPTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;

/**
 * Tests for {@link RocksDBStateBackend} on initialization.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({RocksDBStateBackend.class })
public class RocksDBInitTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * This test checks that the RocksDB native code loader still responds to resetting the init flag.
	 */
	@Test
	public void testResetInitFlag() throws Exception {
		RocksDBStateBackend.resetRocksDBLoadedFlag();
	}

	@Test
	public void testTempLibFolderDeletedOnFail() throws Exception {
		final DummyEnvironment env = new DummyEnvironment();

		RocksDBStateBackend backend = spy(
			new RocksDBStateBackend(temporaryFolder.newFolder().toURI()));
		List<File> tempFolders = new ArrayList<>();

		PowerMockito.spy(RocksDBStateBackend.class);
		PowerMockito.when(RocksDBStateBackend.class, "createRocksDBLibDir", any(File.class))
			.then(invocation -> {
				File folder = (File) invocation.callRealMethod();
				// create non-empty folder
				new File(folder, UUID.randomUUID().toString()).createNewFile();
				// set folder not writable to let loading library unsuccessfully
				folder.setWritable(false);
				tempFolders.add(folder);
				return folder;
			});
		PowerMockito.when(RocksDBStateBackend.class, "resetRocksDBLoadedFlag")
			.then(invocationOnMock -> {
				for (File tempFolder : tempFolders) {
					if (tempFolder.exists()) {
						// set folder writable to let that folder could be removed
						tempFolder.setWritable(true);
					}
				}
				invocationOnMock.callRealMethod();
				return null;
			});

		try {
			backend.createKeyedStateBackend(
				env,
				env.getJobID(),
				"test",
				IntSerializer.INSTANCE,
				1,
				KeyGroupRange.of(0, 0),
				env.getTaskKvStateRegistry(),
				TtlTimeProvider.DEFAULT,
				new UnregisteredMetricsGroup(),
				Collections.emptyList(),
				new CloseableRegistry());
			fail("Not throwing expected exception.");
		} catch (IOException expectedException) {
			assertEquals(ROCKSDB_LIB_LOADING_ATTEMPTS, tempFolders.size());
			for (File folder : tempFolders) {
				// ensure all folders could be removed in time
				assertFalse(folder.exists());
			}
		}
	}
}
