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
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;

import org.junit.AssumptionViolatedException;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage.CHECKPOINT_FILE_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

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
		return new FsCheckpointStorage(checkpointDir, null, new JobID(), createCheckpointSubDir, FILE_SIZE_THRESHOLD);
	}

	@Override
	protected CheckpointStorage createCheckpointStorageWithSavepointDir(Path checkpointDir, Path savepointDir) throws Exception {
		return new FsCheckpointStorage(checkpointDir, savepointDir, new JobID(), createCheckpointSubDir, FILE_SIZE_THRESHOLD);
	}

	// ------------------------------------------------------------------------
	//  FsCheckpointStorage-specific tests
	// ------------------------------------------------------------------------

	@Test
	public void testSavepointsInOneDirectoryDefaultLocation() throws Exception {
		final Path defaultSavepointDir = Path.fromLocalFile(tmp.newFolder());

		final FsCheckpointStorage storage = new FsCheckpointStorage(
				Path.fromLocalFile(tmp.newFolder()), defaultSavepointDir, new JobID(), createCheckpointSubDir, FILE_SIZE_THRESHOLD);

		final FsCheckpointStorageLocation savepointLocation = (FsCheckpointStorageLocation)
				storage.initializeLocationForSavepoint(52452L, null);

		// all state types should be in the expected location
		assertParent(defaultSavepointDir, savepointLocation.getCheckpointDirectory());
		assertParent(defaultSavepointDir, savepointLocation.getExclusiveCheckpointDirectory());
		assertParent(defaultSavepointDir, savepointLocation.getSharedStateDirectory());
		assertParent(defaultSavepointDir, savepointLocation.getTaskOwnedStateDirectory());

		// cleanup
		savepointLocation.disposeOnFailure();
	}

	@Test
	public void testSavepointsInOneDirectoryCustomLocation() throws Exception {
		final Path savepointDir = Path.fromLocalFile(tmp.newFolder());

		final FsCheckpointStorage storage = new FsCheckpointStorage(
				Path.fromLocalFile(tmp.newFolder()), null, new JobID(), createCheckpointSubDir, FILE_SIZE_THRESHOLD);

		final FsCheckpointStorageLocation savepointLocation = (FsCheckpointStorageLocation)
				storage.initializeLocationForSavepoint(52452L, savepointDir.toString());

		// all state types should be in the expected location
		assertParent(savepointDir, savepointLocation.getCheckpointDirectory());
		assertParent(savepointDir, savepointLocation.getExclusiveCheckpointDirectory());
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
				Path.fromLocalFile(tmp.newFolder()),  null, new JobID(), createCheckpointSubDir, 10);

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
		final Path exclusiveStateDir = randomTempPath();
		final Path sharedStateDir = randomTempPath();

		FsCheckpointStorageLocation storageLocation = new FsCheckpointStorageLocation(
				fs,
				checkpointDir,
				exclusiveStateDir,
				sharedStateDir,
				randomTempPath(),
				CheckpointStorageLocationReference.getDefault(),
				0);

		assertNotEquals(storageLocation.getCheckpointDirectory(), storageLocation.getSharedStateDirectory());

		assertEquals(0, fs.listStatus(storageLocation.getCheckpointDirectory()).length);
		assertEquals(0, fs.listStatus(storageLocation.getExclusiveCheckpointDirectory()).length);
		assertEquals(0, fs.listStatus(storageLocation.getSharedStateDirectory()).length);

		long checkpointId = 23L;
		// create exclusive state

		CheckpointStateOutputStream exclusiveStream =
				storageLocation.createCheckpointStateOutputStream(checkpointId, CheckpointedStateScope.EXCLUSIVE);

		exclusiveStream.write(42);
		exclusiveStream.flush();
		StreamStateHandle exclusiveHandle = exclusiveStream.closeAndGetHandle();
		assertTrue(exclusiveHandle instanceof FileStateHandle);
		((FileStateHandle) exclusiveHandle).getFilePath().getName().startsWith(CHECKPOINT_FILE_PREFIX);

		assertEquals(0, fs.listStatus(storageLocation.getCheckpointDirectory()).length);
		assertEquals(1, fs.listStatus(storageLocation.getExclusiveCheckpointDirectory()).length);
		assertEquals(0, fs.listStatus(storageLocation.getSharedStateDirectory()).length);

		// create shared state

		CheckpointStateOutputStream sharedStream =
				storageLocation.createCheckpointStateOutputStream(checkpointId, CheckpointedStateScope.SHARED);

		sharedStream.write(42);
		sharedStream.flush();
		StreamStateHandle sharedHandle = sharedStream.closeAndGetHandle();
		assertTrue(sharedHandle instanceof FileStateHandle);
		((FileStateHandle) sharedHandle).getFilePath().getName().startsWith(CHECKPOINT_FILE_PREFIX);

		assertEquals(0, fs.listStatus(storageLocation.getCheckpointDirectory()).length);
		assertEquals(1, fs.listStatus(storageLocation.getExclusiveCheckpointDirectory()).length);
		assertEquals(1, fs.listStatus(storageLocation.getSharedStateDirectory()).length);

		// drop state

		exclusiveHandle.discardState();
		sharedHandle.discardState();
	}

	/**
	 * This test checks that no mkdirs is called by the checkpoint storage location when resolved.
	 */
	@Test
	public void testStorageLocationDoesNotMkdirs() throws Exception {
		FsCheckpointStorage storage = new FsCheckpointStorage(
				randomTempPath(), null, new JobID(), createCheckpointSubDir, FILE_SIZE_THRESHOLD);

		File baseDir =  new File(storage.getCheckpointsDirectory().getPath());
		assertTrue(baseDir.exists());

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
			createCheckpointSubDir,
			FILE_SIZE_THRESHOLD);

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

	@Override
	public void testPersistMultipleMetadataOnlyCheckpoints() throws Exception {
		if (createCheckpointSubDir) {
			super.testPersistMultipleMetadataOnlyCheckpoints();
		} else {
			throw new AssumptionViolatedException("Multiple checkpoints from different jobs with the same checkpoint ID cannot be guaranteed that they do not interfere with each other, " +
				"due to we have not created checkpoint sub-directory.");
		}
	}

	@Test
	public void testCreateStateSubDIrs() throws IOException {
		final FsCheckpointStorage storage = new FsCheckpointStorage(
			randomTempPath(),
			null,
			new JobID(),
			createCheckpointSubDir,
			0);

		try {
			storage.setStateSubDirs(-1);
			fail("expect IllegalArgumentException.");
		} catch (IllegalArgumentException ignored) {}

		// verify default case, no sub-dirs for state directories.
		long checkpointId = 17L;
		CheckpointStreamFactory checkpointStreamFactory = storage.resolveCheckpointStorageLocation(checkpointId, CheckpointStorageLocationReference.getDefault());
		verifySubDirIndex(checkpointStreamFactory, checkpointId, CheckpointedStateScope.EXCLUSIVE, storage.getStateSubDirs());
		verifySubDirIndex(checkpointStreamFactory, checkpointId, CheckpointedStateScope.SHARED, storage.getStateSubDirs());

		// verify case with 1024 sub-dirs for state directories.
		int stateSubDirs = 1024;
		storage.setStateSubDirs(stateSubDirs);
		checkpointId = 18L;
		checkpointStreamFactory = storage.resolveCheckpointStorageLocation(checkpointId, CheckpointStorageLocationReference.getDefault());
		verifySubDirIndex(checkpointStreamFactory, checkpointId, CheckpointedStateScope.EXCLUSIVE, storage.getStateSubDirs());
		verifySubDirIndex(checkpointStreamFactory, checkpointId, CheckpointedStateScope.SHARED, storage.getStateSubDirs());
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private void assertParent(Path parent, Path child) {
		Path path = new Path(parent, child.getName());
		assertEquals(path, child);
	}

	private void verifySubDirIndex(CheckpointStreamFactory checkpointStreamFactory, long checkpointId, CheckpointedStateScope checkpointedStateScope, int stateSubDirs) throws IOException {
		CheckpointStateOutputStream outputStream = checkpointStreamFactory.createCheckpointStateOutputStream(checkpointId, checkpointedStateScope);
		outputStream.write(42);
		outputStream.flush();
		StreamStateHandle exclusiveStateHandle = outputStream.closeAndGetHandle();
		assertTrue(exclusiveStateHandle instanceof FileStateHandle);
		if (stateSubDirs == 0) {
			assertEquals(checkpointedStateScope.name().toLowerCase(), ((FileStateHandle) exclusiveStateHandle).getFilePath().getParent().getName());
		} else {
			int subDirIndex = Integer.parseInt(((FileStateHandle) exclusiveStateHandle).getFilePath().getParent().getName());
			assertTrue(0 <= subDirIndex && subDirIndex < stateSubDirs);
		}
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
		public FileSystem getFileSystem() throws IOException {
			return fileSystem;
		}
	}
}
