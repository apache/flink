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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;

import org.junit.Test;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link FsCheckpointStorage}, which implements the checkpoint storage
 * aspects of the {@link FsStateBackend}.
 */
public class FsCheckpointStorageTest extends AbstractFileCheckpointStorageTestBase {

	private static final int FILE_SIZE_THRESHOLD = 1024;

	// ------------------------------------------------------------------------
	//  General Fs-based checkpoint storage tests, inherited
	// ------------------------------------------------------------------------

	@Override
	protected CheckpointStorage createCheckpointStorage(Path checkpointDir) throws Exception {
		return new FsCheckpointStorage(checkpointDir, null, new JobID(), FILE_SIZE_THRESHOLD);
	}

	@Override
	protected CheckpointStorage createCheckpointStorageWithSavepointDir(Path checkpointDir, Path savepointDir) throws Exception {
		return new FsCheckpointStorage(checkpointDir, savepointDir, new JobID(), FILE_SIZE_THRESHOLD);
	}

	// ------------------------------------------------------------------------
	//  FsCheckpointStorage-specific tests
	// ------------------------------------------------------------------------

	@Test
	public void testSavepointsInOneDirectoryDefaultLocation() throws Exception {
		final Path defaultSavepointDir = Path.fromLocalFile(tmp.newFolder());

		final FsCheckpointStorage storage = new FsCheckpointStorage(
				Path.fromLocalFile(tmp.newFolder()), defaultSavepointDir, new JobID(), FILE_SIZE_THRESHOLD);

		final FsCheckpointStorageLocation savepointLocation = (FsCheckpointStorageLocation)
				storage.initializeLocationForSavepoint(52452L, null);

		// all state types should be in the expected location
		assertParent(defaultSavepointDir, savepointLocation.getCheckpointDirectory());
		assertParent(defaultSavepointDir, savepointLocation.getSharedStateDirectory());
		assertParent(defaultSavepointDir, savepointLocation.getTaskOwnedStateDirectory());

		// cleanup
		savepointLocation.disposeOnFailure();
	}

	@Test
	public void testSavepointsInOneDirectoryCustomLocation() throws Exception {
		final Path savepointDir = Path.fromLocalFile(tmp.newFolder());

		final FsCheckpointStorage storage = new FsCheckpointStorage(
				Path.fromLocalFile(tmp.newFolder()), null, new JobID(), FILE_SIZE_THRESHOLD);

		final FsCheckpointStorageLocation savepointLocation = (FsCheckpointStorageLocation)
				storage.initializeLocationForSavepoint(52452L, savepointDir.toString());

		// all state types should be in the expected location
		assertParent(savepointDir, savepointLocation.getCheckpointDirectory());
		assertParent(savepointDir, savepointLocation.getSharedStateDirectory());
		assertParent(savepointDir, savepointLocation.getTaskOwnedStateDirectory());

		// cleanup
		savepointLocation.disposeOnFailure();
	}

	@Test
	public void testTaskOwnedStateStream() throws Exception {
		final List<String> state = Arrays.asList("Flopsy", "Mopsy", "Cotton Tail", "Peter");

		// we chose a small size threshold here to force the state to disk
		final FsCheckpointStorage storage = new FsCheckpointStorage(
				Path.fromLocalFile(tmp.newFolder()),  null, new JobID(), 10);

		final StreamStateHandle stateHandle;

		try (CheckpointStateOutputStream stream = storage.createTaskOwnedStateStream()) {
			assertTrue(stream instanceof FsCheckpointStateOutputStream);

			new ObjectOutputStream(stream).writeObject(state);
			stateHandle = stream.closeAndGetHandle();
		}

		// the state must have gone to disk
		FileStateHandle fileStateHandle = (FileStateHandle) stateHandle;

		// check that the state is in the correct directory
		String parentDirName = fileStateHandle.getFilePath().getParent().getName();
		assertEquals(FsCheckpointStorage.CHECKPOINT_TASK_OWNED_STATE_DIR, parentDirName);

		// validate the contents
		try (ObjectInputStream in = new ObjectInputStream(stateHandle.openInputStream())) {
			assertEquals(state, in.readObject());
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private void assertParent(Path parent, Path child) {
		Path path = new Path(parent, child.getName());
		assertEquals(path, child);
	}
}
