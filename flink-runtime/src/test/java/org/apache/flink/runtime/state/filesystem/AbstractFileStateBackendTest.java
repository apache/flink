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
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory.StreamHandleAndPointer;
import org.apache.flink.runtime.state.StreamStateHandle;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Tests for the {@link AbstractFileStateBackend}.
 */
public class AbstractFileStateBackendTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	// ------------------------------------------------------------------------
	//  pointers
	// ------------------------------------------------------------------------

	@Test
	public void testPointerPathResolution() throws Exception {
		final FileSystem fs = FileSystem.getLocalFileSystem();

		final Path checkpointDir = new Path(tmp.newFolder().toURI());
		final Path metadataFile = new Path(checkpointDir, AbstractFileStateBackend.METADATA_FILE_NAME);
		
		final String pointer1 = metadataFile.toString();
		final String pointer2 = metadataFile.getParent().toString();
		final String pointer3 = metadataFile.getParent().toString() + '/';

		final FsStateBackend backend = new FsStateBackend(checkpointDir);

		final byte[] data = new byte[23686];
		new Random().nextBytes(data);
		try (FSDataOutputStream out = fs.create(metadataFile, WriteMode.NO_OVERWRITE)) {
			out.write(data);
		}

		StreamStateHandle handle1 = backend.resolveCheckpointLocation(pointer1);
		StreamStateHandle handle2 = backend.resolveCheckpointLocation(pointer2);
		StreamStateHandle handle3 = backend.resolveCheckpointLocation(pointer3);

		assertNotNull(handle1);
		assertNotNull(handle2);
		assertNotNull(handle3);

		validateContents(handle1, data);
		validateContents(handle2, data);
		validateContents(handle3, data);
	}

	@Test
	public void testFailingPointerPathResolution() throws Exception {
		final Path checkpointDir = new Path(tmp.newFolder().toURI());
		final FsStateBackend backend = new FsStateBackend(checkpointDir);

		// null value
		try {
			backend.resolveCheckpointLocation(null);
			fail("expected exception");
		} catch (NullPointerException ignored) {}

		// empty string
		try {
			backend.resolveCheckpointLocation("");
			fail("expected exception");
		} catch (IllegalArgumentException ignored) {}

		// not a file path at all
		try {
			backend.resolveCheckpointLocation("this-is_not/a#filepath.at.all");
			fail("expected exception");
		} catch (IOException ignored) {}

		// non-existing file
		try {
			backend.resolveCheckpointLocation(tmp.newFile().toURI().toString() + "_not_existing");
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

		final FsStateBackend backend = new FsStateBackend(checkpointDir);

		final JobID jobId1 = new JobID();
		final JobID jobId2 = new JobID();

		final long checkpointId = 177;

		final CheckpointMetadataStreamFactory metadataWriter1 = 
				backend.createCheckpointMetadataStreamFactory(jobId1, checkpointId);
		final CheckpointMetadataStreamFactory metadataWriter2 =
				backend.createCheckpointMetadataStreamFactory(jobId2, checkpointId);

		final byte[] data1 = {77, 66, 55, 99, 88};
		final byte[] data2 = {1, 3, 2, 5, 4};

		final StreamHandleAndPointer handle1;
		final StreamHandleAndPointer handle2;

		try (CheckpointMetadataOutputStream out = metadataWriter1.createCheckpointStateOutputStream()) {
			out.write(data1);
			handle1 = out.closeAndGetPointerHandle();
		}
		try (CheckpointMetadataOutputStream out = metadataWriter2.createCheckpointStateOutputStream()) {
			out.write(data2);
			handle2 = out.closeAndGetPointerHandle();
		}

		// check that this went to a file, but in a nested directory structure

		// one directory per job
		FileStatus[] files = fs.listStatus(checkpointDir);
		assertEquals(2, files.length);

		// in each per-job directory, one for the checkpoint
		FileStatus[] job1Files = fs.listStatus(files[0].getPath());
		FileStatus[] job2Files = fs.listStatus(files[1].getPath());
		assertEquals(1, job1Files.length);
		assertEquals(1, job2Files.length);

		assertTrue(fs.exists(new Path(job1Files[0].getPath(), AbstractFileStateBackend.METADATA_FILE_NAME)));
		assertTrue(fs.exists(new Path(job2Files[0].getPath(), AbstractFileStateBackend.METADATA_FILE_NAME)));

		validateContents(handle1.stateHandle(), data1);
		validateContents(handle2.stateHandle(), data2);
	}

	@Test
	public void writeToAlreadyExistingCheckpointFails() throws Exception {
		final Path checkpointDir = new Path(tmp.newFolder().toURI());
		final FsStateBackend backend = new FsStateBackend(checkpointDir);

		final JobID jobId = new JobID();
		final byte[] data = {8, 8, 4, 5, 2, 6, 3};
		final long checkpointId = 177;

		// write to the metadata file for the checkpoint

		final CheckpointMetadataStreamFactory metadataWriter =
				backend.createCheckpointMetadataStreamFactory(jobId, checkpointId);

		try (CheckpointMetadataOutputStream out = metadataWriter.createCheckpointStateOutputStream()) {
			out.write(data);
			out.closeAndGetPointerHandle();
		}

		// create another writer to the metadata file for the checkpoint

		final CheckpointMetadataStreamFactory metadataWriter2 =
				backend.createCheckpointMetadataStreamFactory(jobId, checkpointId);

		try {
			metadataWriter2.createCheckpointStateOutputStream();
			fail("this should fail with an exception");
		}
		catch (IOException ignored) {}
	}

	// ------------------------------------------------------------------------
	//  savepoints
	// ------------------------------------------------------------------------

	@Test
	public void testSavepointPathConfiguredAndTarget() throws Exception {
		final Path savepointDir = new Path(tmp.newFolder().toURI());
		final Path customDir = new Path(tmp.newFolder().toURI());
		testSavepoint(savepointDir, customDir, customDir);
	}

	@Test
	public void testSavepointPathConfiguredNoTarget() throws Exception {
		final Path savepointDir = new Path(tmp.newFolder().toURI());
		testSavepoint(savepointDir, null, savepointDir);
	}

	@Test
	public void testNoSavepointPathConfiguredAndTarget() throws Exception {
		final Path customDir = new Path(tmp.newFolder().toURI());
		testSavepoint(null, customDir, customDir);
	}

	@Test
	public void testNoSavepointPathConfiguredNoTarget() throws Exception {
		final Path checkpointDir = new Path(tmp.newFolder().toURI());
		final FsStateBackend backend = new FsStateBackend(checkpointDir.toUri(), null);

		try {
			backend.createSavepointMetadataStreamFactory(new JobID(), null);
			fail("this should fail with an exception");
		}
		catch (UnsupportedOperationException ignored) {}
	}

	private void testSavepoint(
			@Nullable Path savepointDir,
			@Nullable Path customDir,
			Path expectedParent) throws Exception {

		final Path checkpointDir = new Path(tmp.newFolder().toURI());
		final FsStateBackend backend = new FsStateBackend(checkpointDir.toUri(), 
				savepointDir == null ? null : savepointDir.toUri());

		final JobID jobId = new JobID();
		final String customLocation = customDir == null ? null : customDir.toString();

		final CheckpointMetadataStreamFactory metadataWriter =
				backend.createSavepointMetadataStreamFactory(jobId, customLocation);

		// should be in the custom location
		assertParent(expectedParent, new Path(metadataWriter.getTargetLocation()));

		final byte[] data = {77, 66, 55, 99, 88};

		final StreamHandleAndPointer handle;
		try (CheckpointMetadataOutputStream out = metadataWriter.createCheckpointStateOutputStream()) {
			out.write(data);
			handle = out.closeAndGetPointerHandle();
		}
		validateContents(handle.stateHandle(), data);
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

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

	private void assertParent(Path parent, Path child) {
		Path path = new Path(parent, child.getName());
		assertEquals(path, child);
	}
}
