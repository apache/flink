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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for the {@link FsCheckpointStorage}, which implements the checkpoint storage
 * aspects of the {@link FsStateBackend}.
 */
public class FsCheckpointStorageTest extends AbstractFileCheckpointStorageTestBase {

	private static final int FILE_SIZE_THRESHOLD = 1024;
	private static final int WRITE_BUFFER_SIZE = 4096;

	// ------------------------------------------------------------------------
	//  General Fs-based checkpoint storage tests, inherited
	// ------------------------------------------------------------------------

	@Override
	protected CheckpointStorage createCheckpointStorage(Path checkpointDir) throws Exception {
		return new FsCheckpointStorage(checkpointDir, null, new JobID(), FILE_SIZE_THRESHOLD, WRITE_BUFFER_SIZE);
	}

	@Override
	protected CheckpointStorage createCheckpointStorageWithSavepointDir(Path checkpointDir, Path savepointDir) throws Exception {
		return new FsCheckpointStorage(checkpointDir, savepointDir, new JobID(), FILE_SIZE_THRESHOLD, WRITE_BUFFER_SIZE);
	}

	// ------------------------------------------------------------------------
	//  FsCheckpointStorage-specific tests
	// ------------------------------------------------------------------------

	@Test
	public void testSavepointsInOneDirectoryDefaultLocation() throws Exception {
		final Path defaultSavepointDir = Path.fromLocalFile(tmp.newFolder());

		final FsCheckpointStorage storage = new FsCheckpointStorage(
				Path.fromLocalFile(tmp.newFolder()), defaultSavepointDir, new JobID(), FILE_SIZE_THRESHOLD, WRITE_BUFFER_SIZE);

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
				Path.fromLocalFile(tmp.newFolder()), null, new JobID(), FILE_SIZE_THRESHOLD, WRITE_BUFFER_SIZE);

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
				Path.fromLocalFile(tmp.newFolder()),  null, new JobID(), 10, WRITE_BUFFER_SIZE);

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

	@Test
	public void testDirectoriesForExclusiveAndSharedState() throws Exception {
		final FileSystem fs = LocalFileSystem.getSharedInstance();
		final Path checkpointDir = randomTempPath();
		final Path sharedStateDir = randomTempPath();

		FsCheckpointStorageLocation storageLocation = new FsCheckpointStorageLocation(
				fs,
				checkpointDir,
				sharedStateDir,
				randomTempPath(),
				CheckpointStorageLocationReference.getDefault(),
				FILE_SIZE_THRESHOLD,
				WRITE_BUFFER_SIZE);

		assertNotEquals(storageLocation.getCheckpointDirectory(), storageLocation.getSharedStateDirectory());

		assertEquals(0, fs.listStatus(storageLocation.getCheckpointDirectory()).length);
		assertEquals(0, fs.listStatus(storageLocation.getSharedStateDirectory()).length);

		// create exclusive state

		CheckpointStateOutputStream exclusiveStream =
				storageLocation.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);

		exclusiveStream.write(42);
		exclusiveStream.flush();
		StreamStateHandle exclusiveHandle = exclusiveStream.closeAndGetHandle();

		assertEquals(1, fs.listStatus(storageLocation.getCheckpointDirectory()).length);
		assertEquals(0, fs.listStatus(storageLocation.getSharedStateDirectory()).length);

		// create shared state

		CheckpointStateOutputStream sharedStream =
				storageLocation.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED);

		sharedStream.write(42);
		sharedStream.flush();
		StreamStateHandle sharedHandle = sharedStream.closeAndGetHandle();

		assertEquals(1, fs.listStatus(storageLocation.getCheckpointDirectory()).length);
		assertEquals(1, fs.listStatus(storageLocation.getSharedStateDirectory()).length);

		// drop state

		exclusiveHandle.discardState();
		sharedHandle.discardState();
	}

	/**
	 * This test checks that the expected mkdirs action for checkpoint storage,
	 * only be called when initializeBaseLocations and not called when resolveCheckpointStorageLocation.
	 */
	@Test
	public void testStorageLocationMkdirs() throws Exception {
		FsCheckpointStorage storage = new FsCheckpointStorage(
				randomTempPath(), null, new JobID(), FILE_SIZE_THRESHOLD, WRITE_BUFFER_SIZE);

		File baseDir = new File(storage.getCheckpointsDirectory().getPath());
		assertFalse(baseDir.exists());

		// mkdirs would only be called when initializeBaseLocations
		storage.initializeBaseLocations();
		assertTrue(baseDir.exists());

		// mkdir would not be called when resolveCheckpointStorageLocation
		storage = new FsCheckpointStorage(
				randomTempPath(), null, new JobID(), FILE_SIZE_THRESHOLD, WRITE_BUFFER_SIZE);

		FsCheckpointStorageLocation location = (FsCheckpointStorageLocation)
				storage.resolveCheckpointStorageLocation(177, CheckpointStorageLocationReference.getDefault());

		Path checkpointPath = location.getCheckpointDirectory();
		File checkpointDir = new File(checkpointPath.getPath());
		assertFalse(checkpointDir.exists());
	}

	@Test
	public void testResolveCheckpointStorageLocation() throws Exception {
		final FileSystem checkpointFileSystem = mock(FileSystem.class);
		final FsCheckpointStorage storage = new FsCheckpointStorage(
			new TestingPath("hdfs:///checkpoint/", checkpointFileSystem),
			null,
			new JobID(),
			FILE_SIZE_THRESHOLD,
			WRITE_BUFFER_SIZE);

		final FsCheckpointStorageLocation checkpointStreamFactory =
			(FsCheckpointStorageLocation) storage.resolveCheckpointStorageLocation(1L, CheckpointStorageLocationReference.getDefault());
		assertEquals(checkpointFileSystem, checkpointStreamFactory.getFileSystem());

		final CheckpointStorageLocationReference savepointLocationReference =
			AbstractFsCheckpointStorage.encodePathAsReference(new Path("file:///savepoint/"));

		final FsCheckpointStorageLocation savepointStreamFactory =
			(FsCheckpointStorageLocation) storage.resolveCheckpointStorageLocation(2L, savepointLocationReference);
		final FileSystem fileSystem = savepointStreamFactory.getFileSystem();
		assertTrue(fileSystem instanceof LocalFileSystem);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private void assertParent(Path parent, Path child) {
		Path path = new Path(parent, child.getName());
		assertEquals(path, child);
	}

	private static final class TestingPath extends Path {

		private static final long serialVersionUID = 2560119808844230488L;

		@SuppressWarnings("TransientFieldNotInitialized")
		@Nonnull
		private final transient FileSystem fileSystem;

		TestingPath(String pathString, @Nonnull FileSystem fileSystem) {
			super(pathString);
			this.fileSystem = fileSystem;
		}

		@Override
		public FileSystem getFileSystem() {
			return fileSystem;
		}
	}
}
