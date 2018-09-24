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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.WriteOptions;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointedStateScope;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests verifying that the FsStateBackend passes the entropy injection option
 * to the FileSystem for state payload files, but not for metadata files.
 */
public class FsStateBackendEntropyTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@After
	public void resetFileSystems() throws Exception {
		FileSystem.initialize(new Configuration());
	}

	@Test
	public void testInjection() throws Exception {
		FileSystem fs = spy(LocalFileSystem.getSharedInstance());
		ArgumentCaptor<WriteOptions> optionsCaptor = ArgumentCaptor.forClass(WriteOptions.class);

		Path checkpointDir = Path.fromLocalFile(tmp.newFolder());

		FsCheckpointStorage storage = new FsCheckpointStorage(
				fs, checkpointDir, null, new JobID(), 1024);

		CheckpointStorageLocation location = storage.initializeLocationForCheckpoint(96562);

		// check entropy in task-owned state
		try (CheckpointStateOutputStream stream = storage.createTaskOwnedStateStream()) {
			stream.flush();
		}

		verify(fs, times(1)).create(any(Path.class), optionsCaptor.capture());
		assertTrue(optionsCaptor.getValue().isInjectEntropy());
		reset(fs);

		// check entropy in the exclusive/shared state
		try (CheckpointStateOutputStream stream =
				location.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE)) {

			stream.flush();
		}

		verify(fs, times(1)).create(any(Path.class), optionsCaptor.capture());
		assertTrue(optionsCaptor.getValue().isInjectEntropy());
		reset(fs);

		// check that there is no entropy in the metadata
		// check entropy in the exclusive/shared state
		try (CheckpointMetadataOutputStream stream = location.createMetadataOutputStream()) {
			stream.flush();
		}

		verify(fs, times(0)).create(any(Path.class), any(WriteOptions.class));
	}
}
