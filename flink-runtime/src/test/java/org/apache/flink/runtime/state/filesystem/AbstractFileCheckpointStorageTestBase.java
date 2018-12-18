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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StreamStateHandle;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test base for file-system-based checkoint storage, such as the
 * {@link org.apache.flink.runtime.state.memory.MemoryBackendCheckpointStorage} and the
 * {@link FsCheckpointStorage}.
 */
public abstract class AbstractFileCheckpointStorageTestBase {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	// ------------------------------------------------------------------------
	//  factories for the actual state storage to be tested
	// ------------------------------------------------------------------------

	protected abstract CheckpointStorage createCheckpointStorage(Path checkpointDir) throws Exception;

	protected abstract CheckpointStorage createCheckpointStorageWithSavepointDir(
			Path checkpointDir,
			Path savepointDir) throws Exception;

	// ------------------------------------------------------------------------
	//  pointers
	// ------------------------------------------------------------------------

	@Test
	public void testPointerPathResolution() throws Exception {
		final FileSystem fs = FileSystem.getLocalFileSystem();
		final Path metadataFile = new Path(Path.fromLocalFile(tmp.newFolder()), AbstractFsCheckpointStorage.METADATA_FILE_NAME);

		final String basePointer = metadataFile.getParent().toString();

		final String pointer1 = metadataFile.toString();
		final String pointer2 = metadataFile.getParent().toString();
		final String pointer3 = metadataFile.getParent().toString() + '/';

		// create the storage for some random checkpoint directory
		final CheckpointStorage storage = createCheckpointStorage(randomTempPath());

		final byte[] data = new byte[23686];
		new Random().nextBytes(data);
		try (FSDataOutputStream out = fs.create(metadataFile, WriteMode.NO_OVERWRITE)) {
			out.write(data);
		}

		CompletedCheckpointStorageLocation completed1 = storage.resolveCheckpoint(pointer1);
		CompletedCheckpointStorageLocation completed2 = storage.resolveCheckpoint(pointer2);
		CompletedCheckpointStorageLocation completed3 = storage.resolveCheckpoint(pointer3);

		assertEquals(basePointer, completed1.getExternalPointer());
		assertEquals(basePointer, completed2.getExternalPointer());
		assertEquals(basePointer, completed3.getExternalPointer());

		StreamStateHandle handle1 = completed1.getMetadataHandle();
		StreamStateHandle handle2 = completed2.getMetadataHandle();
		StreamStateHandle handle3 = completed3.getMetadataHandle();

		assertNotNull(handle1);
		assertNotNull(handle2);
		assertNotNull(handle3);

		validateContents(handle1, data);
		validateContents(handle2, data);
		validateContents(handle3, data);
	}

	@Test
	public void testFailingPointerPathResolution() throws Exception {
		// create the storage for some random checkpoint directory
		final CheckpointStorage storage = createCheckpointStorage(randomTempPath());

		// null value
		try {
			storage.resolveCheckpoint(null);
			fail("expected exception");
		} catch (NullPointerException ignored) {}

		// empty string
		try {
			storage.resolveCheckpoint("");
			fail("expected exception");
		} catch (IllegalArgumentException ignored) {}

		// not a file path at all
		try {
			storage.resolveCheckpoint("this-is_not/a#filepath.at.all");
			fail("expected exception");
		} catch (IOException ignored) {}

		// non-existing file
		try {
			storage.resolveCheckpoint(tmp.newFile().toURI().toString() + "_not_existing");
			fail("expected exception");
		} catch (IOException ignored) {}
	}

	// ------------------------------------------------------------------------
	//  checkpoints
	// ------------------------------------------------------------------------

	/**
	 * Validates that multiple checkpoints from different jobs with the same checkpoint ID do not
	 * interfere with each other.
	 */
	@Test
	public void testPersistMultipleMetadataOnlyCheckpoints() throws Exception {
		final FileSystem fs = FileSystem.getLocalFileSystem();
		final Path checkpointDir = new Path(tmp.newFolder().toURI());

		final long checkpointId = 177;

		final CheckpointStorage storage1 = createCheckpointStorage(checkpointDir);
		final CheckpointStorage storage2 = createCheckpointStorage(checkpointDir);

		final CheckpointStorageLocation loc1 = storage1.initializeLocationForCheckpoint(checkpointId);
		final CheckpointStorageLocation loc2 = storage2.initializeLocationForCheckpoint(checkpointId);

		final byte[] data1 = {77, 66, 55, 99, 88};
		final byte[] data2 = {1, 3, 2, 5, 4};

		final CompletedCheckpointStorageLocation completedLocation1;
		try (CheckpointMetadataOutputStream out = loc1.createMetadataOutputStream()) {
			out.write(data1);
			completedLocation1 = out.closeAndFinalizeCheckpoint();
		}
		final String result1 = completedLocation1.getExternalPointer();

		final CompletedCheckpointStorageLocation completedLocation2;
		try (CheckpointMetadataOutputStream out = loc2.createMetadataOutputStream()) {
			out.write(data2);
			completedLocation2 = out.closeAndFinalizeCheckpoint();
		}
		final String result2 = completedLocation2.getExternalPointer();

		// check that this went to a file, but in a nested directory structure

		// one directory per storage
		FileStatus[] files = fs.listStatus(checkpointDir);
		assertEquals(2, files.length);

		// in each per-storage directory, one for the checkpoint
		FileStatus[] job1Files = fs.listStatus(files[0].getPath());
		FileStatus[] job2Files = fs.listStatus(files[1].getPath());
		assertTrue(job1Files.length >= 1);
		assertTrue(job2Files.length >= 1);

		assertTrue(fs.exists(new Path(result1, AbstractFsCheckpointStorage.METADATA_FILE_NAME)));
		assertTrue(fs.exists(new Path(result2, AbstractFsCheckpointStorage.METADATA_FILE_NAME)));

		// check that both storages can resolve each others contents
		validateContents(storage1.resolveCheckpoint(result1).getMetadataHandle(), data1);
		validateContents(storage1.resolveCheckpoint(result2).getMetadataHandle(), data2);
		validateContents(storage2.resolveCheckpoint(result1).getMetadataHandle(), data1);
		validateContents(storage2.resolveCheckpoint(result2).getMetadataHandle(), data2);
	}

	@Test
	public void writeToAlreadyExistingCheckpointFails() throws Exception {
		final byte[] data = {8, 8, 4, 5, 2, 6, 3};
		final long checkpointId = 177;

		final CheckpointStorage storage = createCheckpointStorage(randomTempPath());
		final CheckpointStorageLocation loc = storage.initializeLocationForCheckpoint(checkpointId);

		// write to the metadata file for the checkpoint

		try (CheckpointMetadataOutputStream out = loc.createMetadataOutputStream()) {
			out.write(data);
			out.closeAndFinalizeCheckpoint();
		}

		// create another writer to the metadata file for the checkpoint
		try {
			loc.createMetadataOutputStream();
			fail("this should fail with an exception");
		}
		catch (IOException ignored) {}
	}

	// ------------------------------------------------------------------------
	//  savepoints
	// ------------------------------------------------------------------------

	@Test
	public void testSavepointPathConfiguredAndTarget() throws Exception {
		final Path savepointDir = randomTempPath();
		final Path customDir = randomTempPath();
		testSavepoint(savepointDir, customDir, customDir);
	}

	@Test
	public void testSavepointPathConfiguredNoTarget() throws Exception {
		final Path savepointDir = randomTempPath();
		testSavepoint(savepointDir, null, savepointDir);
	}

	@Test
	public void testNoSavepointPathConfiguredAndTarget() throws Exception {
		final Path customDir = Path.fromLocalFile(tmp.newFolder());
		testSavepoint(null, customDir, customDir);
	}

	@Test
	public void testNoSavepointPathConfiguredNoTarget() throws Exception {
		final CheckpointStorage storage = createCheckpointStorage(randomTempPath());

		try {
			storage.initializeLocationForSavepoint(1337, null);
			fail("this should fail with an exception");
		}
		catch (IllegalArgumentException ignored) {}
	}

	private void testSavepoint(
			@Nullable Path savepointDir,
			@Nullable Path customDir,
			Path expectedParent) throws Exception {

		final CheckpointStorage storage = savepointDir == null ?
				createCheckpointStorage(randomTempPath()) :
				createCheckpointStorageWithSavepointDir(randomTempPath(), savepointDir);

		final String customLocation = customDir == null ? null : customDir.toString();

		final CheckpointStorageLocation savepointLocation =
				storage.initializeLocationForSavepoint(52452L, customLocation);

		final byte[] data = {77, 66, 55, 99, 88};

		final CompletedCheckpointStorageLocation completed;
		try (CheckpointMetadataOutputStream out = savepointLocation.createMetadataOutputStream()) {
			out.write(data);
			completed = out.closeAndFinalizeCheckpoint();
		}

		// we need to do this step to make sure we have a slash (or not) in the same way as the
		// expected path has it
		final Path normalizedWithSlash = Path.fromLocalFile(new File(new Path(completed.getExternalPointer()).getParent().getPath()));

		assertEquals(expectedParent, normalizedWithSlash);
		validateContents(completed.getMetadataHandle(), data);

		// validate that the correct directory was used
		FileStateHandle fileStateHandle = (FileStateHandle) completed.getMetadataHandle();

		// we need to recreate that path in the same way as the "expected path" (via File and URI) to make
		// sure the either both use '/' suffixes, or neither uses them (a bit of an annoying ambiguity)
		Path usedSavepointDir = new Path(new File(fileStateHandle.getFilePath().getParent().getParent().getPath()).toURI());

		assertEquals(expectedParent, usedSavepointDir);
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	public Path randomTempPath() throws IOException {
		return Path.fromLocalFile(tmp.newFolder());
	}

	private static void validateContents(StreamStateHandle handle, byte[] expected) throws IOException {
		try (FSDataInputStream in = handle.openInputStream()) {
			validateContents(in, expected);
		}
	}

	private static void validateContents(InputStream in, byte[] expected) throws IOException {
		final byte[] buffer = new byte[expected.length];

		int pos = 0;
		int remaining = expected.length;
		while (remaining > 0) {
			int read = in.read(buffer, pos, remaining);
			if (read == -1) {
				throw new EOFException();
			}
			pos += read;
			remaining -= read;
		}

		assertArrayEquals(expected, buffer);
	}
}
