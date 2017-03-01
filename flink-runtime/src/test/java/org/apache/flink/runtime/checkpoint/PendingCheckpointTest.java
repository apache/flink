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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint.TaskAcknowledgeResult;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.filesystem.FsCheckpointMetadataStreamFactory;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link PendingCheckpoint}.
 */
public class PendingCheckpointTest {

	private static final Map<ExecutionAttemptID, ExecutionVertex> ACK_TASKS = new HashMap<>();
	private static final ExecutionAttemptID ATTEMPT_ID = new ExecutionAttemptID();

	static {
		ACK_TASKS.put(ATTEMPT_ID, mock(ExecutionVertex.class));
	}

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	// ------------------------------------------------------------------------

	/**
	 * Tests that pending checkpoints cannot be subsumed if they are forced.
	 */
	@Test
	public void testCanBeSubsumed() throws Exception {
		// Forced checkpoints cannot be subsumed
		CheckpointProperties forced = new CheckpointProperties(true, true, true, true, true, true, true);
		PendingCheckpoint pending = createPendingCheckpoint(forced, tmpFolder.newFolder());
		assertFalse(pending.canBeSubsumed());

		try {
			pending.abortSubsumed();
			fail("Did not throw expected Exception");
		} catch (IllegalStateException ignored) {
			// Expected
		}

		// Non-forced checkpoints can be subsumed
		CheckpointProperties subsumed = new CheckpointProperties(false, false, true, true, true, true, true);
		pending = createPendingCheckpoint(subsumed, null);
		assertTrue(pending.canBeSubsumed());
	}

	/**
	 * Tests that the checkpoint properly finalizes when not persisting the metadata.
	 */
	@Test
	public void testFinalizeInternal() throws Exception {
		final CheckpointProperties props = new CheckpointProperties(false, false, true, true, true, true, true);

		final PendingCheckpoint pending = createPendingCheckpoint(props, null);
		final Future<CompletedCheckpoint> future = pending.getCompletionFuture();

		pending.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics());

		final CompletedCheckpoint completed = pending.finalizeCheckpointNonExternalized();

		// pending checkpoints must not be valid any more
		assertTrue(pending.isDiscarded());

		// completed checkpoints must match pending checkpoints
		comparePendingAndCompletedCheckpoint(pending, completed);

		// future must be properly complete
		assertTrue(future.isDone());
		assertTrue(completed == future.getNow(null));
	}

	/**
	 * Checks that finalizing a checkpoint with externalization works properly.
	 * Externalization should work independent of whether the checkpoint properties define the
	 * checkpoint to be externalized or not, because externalization can also be triggered
	 * by high-availability persistence.
	 */
	@Test
	public void testFinalizeExternal() throws Exception {
		final File tmp1 = tmpFolder.newFolder();
		final File tmp2 = tmpFolder.newFolder();

		final CheckpointProperties external = new CheckpointProperties(false, true, true, true, true, true, true);
		final CheckpointProperties nonExternal = new CheckpointProperties(false, false, true, true, true, true, true);

		final PendingCheckpoint pending1 = createPendingCheckpoint(external, tmp1);
		final PendingCheckpoint pending2 = createPendingCheckpoint(nonExternal, tmp2);

		final Future<CompletedCheckpoint> future1 = pending1.getCompletionFuture();
		final Future<CompletedCheckpoint> future2 = pending2.getCompletionFuture();

		pending1.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics());
		pending2.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics());

		// no files existing prior to finalization
		assertEquals(0, countFilesInDirectory(tmp1));
		assertEquals(0, countFilesInDirectory(tmp2));

		// finalize!
		CompletedCheckpoint completed1 = pending1.finalizeCheckpointExternalized();
		CompletedCheckpoint completed2 = pending2.finalizeCheckpointExternalized();

		// pending checkpoints must not be valid any more
		assertTrue(pending1.isDiscarded());
		assertTrue(pending2.isDiscarded());

		// completed checkpoints must match pending checkpoints
		comparePendingAndCompletedCheckpoint(pending1, completed1);
		comparePendingAndCompletedCheckpoint(pending2, completed2);

		// future must be properly complete
		assertTrue(future1.isDone());
		assertTrue(future2.isDone());
		assertTrue(completed1 == future1.getNow(null));
		assertTrue(completed2 == future2.getNow(null));

		// files existing for external metadata
		assertEquals(1, countFilesInDirectory(tmp1));
		assertEquals(1, countFilesInDirectory(tmp2));

		// disposing the completed checkpoints cleans up the state
		completed1.discard();
		completed2.discard();

		assertTrue(tmp1.list() == null || tmp1.list().length == 0);
		assertTrue(tmp2.list() == null || tmp2.list().length == 0);
	}

	@Test
	public void testFinalizeAfterDispose() throws Exception {
		final CheckpointProperties internal = new CheckpointProperties(false, false, true, true, true, true, true);
		final CheckpointProperties external = new CheckpointProperties(false, true, false, false, false, false, false);

		final PendingCheckpoint pendingInternal = createPendingCheckpoint(internal, null);
		final PendingCheckpoint pendingExternal = createPendingCheckpoint(external, tmpFolder.newFolder());
		
		final Future<CompletedCheckpoint> futureInternal = pendingInternal.getCompletionFuture();
		final Future<CompletedCheckpoint> futureExternal = pendingExternal.getCompletionFuture();

		pendingInternal.abortDeclined();
		pendingExternal.abortDeclined();

		assertEquals(TaskAcknowledgeResult.DISCARDED,
				pendingInternal.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics()));
		assertEquals(TaskAcknowledgeResult.DISCARDED,
				pendingExternal.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics()));

		try {
			pendingInternal.finalizeCheckpointExternalized();
			fail("this should fail with an exception");
		}
		catch (IllegalStateException ignored) {
			// expected
		}

		try {
			pendingExternal.finalizeCheckpointExternalized();
			fail("this should fail with an exception");
		}
		catch (IllegalStateException ignored) {
			// expected
		}

		assertTrue(futureInternal.isDone());
		assertTrue(futureExternal.isDone());

		try {
			futureInternal.getNow(null);
			fail("this should fail with an exception");
		} catch (ExecutionException ignored) {}

		try {
			futureInternal.getNow(null);
			fail("this should fail with an exception");
		} catch (ExecutionException ignored) {}
		
	}

	@Test
	public void testFinalizeNotAcknowledged() throws Exception {
		final CheckpointProperties internal = new CheckpointProperties(false, false, true, true, true, true, true);
		final CheckpointProperties external = new CheckpointProperties(false, true, false, false, false, false, false);

		final PendingCheckpoint pendingInternal = createPendingCheckpoint(internal, null);
		final PendingCheckpoint pendingExternal = createPendingCheckpoint(external, tmpFolder.newFolder());

		final Future<CompletedCheckpoint> futureInternal = pendingInternal.getCompletionFuture();
		final Future<CompletedCheckpoint> futureExternal = pendingExternal.getCompletionFuture();

		assertFalse(pendingInternal.isFullyAcknowledged());
		assertFalse(pendingExternal.isFullyAcknowledged());

		try {
			pendingInternal.finalizeCheckpointExternalized();
			fail("this should fail with an exception");
		}
		catch (IllegalStateException ignored) {
			// expected
		}

		try {
			pendingExternal.finalizeCheckpointExternalized();
			fail("this should fail with an exception");
		}
		catch (IllegalStateException ignored) {
			// expected
		}

		assertFalse(futureInternal.isDone());
		assertFalse(futureExternal.isDone());
	}

	/**
	 * Validates that failures during checkpoint writes close the stream (which delete the files
	 * for the file-system based backend).
	 */
	@Test
	public void testFailedWriteClosesStream() throws Exception {
		final File folder = tmpFolder.newFolder();
		assertEquals(0, countFilesInDirectory(folder));

		// the stream that fails on writes
		final FSDataOutputStream mockStream = mock(FSDataOutputStream.class);
		doThrow(new IOException("test exception")).when(mockStream).write(anyInt());
		doThrow(new IOException("test exception")).when(mockStream).write(any(byte[].class), anyInt(), anyInt());

		final Path directory = new Path(folder.toURI());
		final Path metadataFile = new Path(directory, "metadata_file");

		final FileSystem fs = mock(FileSystem.class);
		when(fs.create(metadataFile, WriteMode.NO_OVERWRITE)).thenReturn(mockStream);

		final FsCheckpointMetadataStreamFactory metadataStore = 
				new FsCheckpointMetadataStreamFactory(fs, directory, metadataFile);

		final CheckpointProperties props = new CheckpointProperties(false, false, true, true, true, true, true);
		final Map<ExecutionAttemptID, ExecutionVertex> ackTasks = new HashMap<>(ACK_TASKS);

		final PendingCheckpoint pending = new PendingCheckpoint(new JobID(), 0, 1, ackTasks, 
				props, Executors.directExecutor(), metadataStore);

		assertEquals(TaskAcknowledgeResult.SUCCESS,
				pending.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics()));

		try {
			pending.finalizeCheckpointExternalized();
			fail("this should fail with an exception");
		}
		catch (IOException ignored) {
			// expected
		}

		// the steam must be closed and the partial file must have been deleted
		verify(mockStream).close();
		verify(fs).delete(eq(metadataFile), anyBoolean());
	}

	/**
	 * Tests that the completion future is succeeded on finalize and failed on
	 * abort and failures during finalize.
	 */
	@Test
	public void testCompletionFuture() throws Exception {
		final CheckpointProperties props = new CheckpointProperties(false, true, false, false, false, false, false);
		final File tmp = tmpFolder.newFolder();

		// Abort declined
		PendingCheckpoint pending = createPendingCheckpoint(props, tmp);
		Future<CompletedCheckpoint> future = pending.getCompletionFuture();

		assertFalse(future.isDone());
		pending.abortDeclined();
		assertTrue(future.isDone());

		// Abort expired
		pending = createPendingCheckpoint(props, tmp);
		future = pending.getCompletionFuture();

		assertFalse(future.isDone());
		pending.abortExpired();
		assertTrue(future.isDone());

		// Abort subsumed
		pending = createPendingCheckpoint(props, tmp);
		future = pending.getCompletionFuture();

		assertFalse(future.isDone());
		pending.abortSubsumed();
		assertTrue(future.isDone());

		// Finalize (all ACK'd)
		pending = createPendingCheckpoint(props, tmpFolder.newFolder());
		future = pending.getCompletionFuture();

		assertFalse(future.isDone());
		pending.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics());
		assertTrue(pending.isFullyAcknowledged());
		pending.finalizeCheckpointExternalized();
		assertTrue(future.isDone());

		// Finalize (missing ACKs)
		pending = createPendingCheckpoint(props, tmp);
		future = pending.getCompletionFuture();

		assertFalse(future.isDone());
		try {
			pending.finalizeCheckpointNonExternalized();
			fail("Did not throw expected Exception");
		} catch (IllegalStateException ignored) {
			// Expected
		}
		try {
			pending.finalizeCheckpointExternalized();
			fail("Did not throw expected Exception");
		} catch (IllegalStateException ignored) {
			// Expected
		}
	}

	/**
	 * Tests that abort discards state.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testAbortDiscardsState() throws Exception {
		CheckpointProperties props = new CheckpointProperties(false, true, false, false, false, false, false);
		TaskState state = mock(TaskState.class);

		final File targetDir = tmpFolder.newFolder();

		// Abort declined
		PendingCheckpoint pending = createPendingCheckpoint(props, targetDir);
		setTaskState(pending, state);

		pending.abortDeclined();
		verify(state, times(1)).discardState();

		// Abort error
		Mockito.reset(state);

		pending = createPendingCheckpoint(props, targetDir);
		setTaskState(pending, state);

		pending.abortError(new Exception("Expected Test Exception"));
		verify(state, times(1)).discardState();

		// Abort expired
		Mockito.reset(state);

		pending = createPendingCheckpoint(props, targetDir);
		setTaskState(pending, state);

		pending.abortExpired();
		verify(state, times(1)).discardState();

		// Abort subsumed
		Mockito.reset(state);

		pending = createPendingCheckpoint(props, targetDir);
		setTaskState(pending, state);

		pending.abortSubsumed();
		verify(state, times(1)).discardState();
	}

	/**
	 * Tests that the stats callbacks happen if the callback is registered.
	 */
	@Test
	public void testPendingCheckpointStatsCallbacks() throws Exception {
		{
			// Complete successfully
			PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
			PendingCheckpoint pending = createPendingCheckpoint(CheckpointProperties.forStandardCheckpoint(), null);
			pending.setStatsCallback(callback);

			pending.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics());
			verify(callback, times(1)).reportSubtaskStats(any(JobVertexID.class), any(SubtaskStateStats.class));

			pending.finalizeCheckpointNonExternalized();
			verify(callback, times(1)).reportCompletedCheckpoint(any(String.class));
		}

		{
			// Fail subsumed
			PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
			PendingCheckpoint pending = createPendingCheckpoint(CheckpointProperties.forStandardCheckpoint(), null);
			pending.setStatsCallback(callback);

			pending.abortSubsumed();
			verify(callback, times(1)).reportFailedCheckpoint(anyLong(), any(Exception.class));
		}

		{
			// Fail subsumed
			PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
			PendingCheckpoint pending = createPendingCheckpoint(CheckpointProperties.forStandardCheckpoint(), null);
			pending.setStatsCallback(callback);

			pending.abortDeclined();
			verify(callback, times(1)).reportFailedCheckpoint(anyLong(), any(Exception.class));
		}

		{
			// Fail subsumed
			PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
			PendingCheckpoint pending = createPendingCheckpoint(CheckpointProperties.forStandardCheckpoint(), null);
			pending.setStatsCallback(callback);

			pending.abortError(new Exception("Expected test error"));
			verify(callback, times(1)).reportFailedCheckpoint(anyLong(), any(Exception.class));
		}

		{
			// Fail subsumed
			PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
			PendingCheckpoint pending = createPendingCheckpoint(CheckpointProperties.forStandardCheckpoint(), null);
			pending.setStatsCallback(callback);

			pending.abortExpired();
			verify(callback, times(1)).reportFailedCheckpoint(anyLong(), any(Exception.class));
		}
	}

	// ------------------------------------------------------------------------

	private static void comparePendingAndCompletedCheckpoint(PendingCheckpoint pending, CompletedCheckpoint complete) {
		assertEquals(pending.getCheckpointId(), complete.getCheckpointID());
		assertEquals(pending.getCheckpointTimestamp(), complete.getTimestamp());
		assertEquals(pending.getJobId(), complete.getJobId());
		assertEquals(pending.getProps(), complete.getProperties());
	}

	private static int countFilesInDirectory(File directory) throws IOException {
		String[] paths = directory.list();
		if (paths != null) {
			return paths.length;
		}
		else {
			throw new IOException("failed to list " + directory);
		}
	}

	private static PendingCheckpoint createPendingCheckpoint(CheckpointProperties props, File targetDirectory) {
		return createPendingCheckpoint(props, targetDirectory, Executors.directExecutor());
	}

	private static PendingCheckpoint createPendingCheckpoint(
			CheckpointProperties props,
			File targetDirectory,
			Executor executor) {

		final Map<ExecutionAttemptID, ExecutionVertex> ackTasks = new HashMap<>(ACK_TASKS);
		final FsCheckpointMetadataStreamFactory metadataStore;
		
		if (targetDirectory == null) {
			metadataStore = null;
		}
		else {
			final FileSystem fs = FileSystem.getLocalFileSystem();
			final Path directory = new Path(targetDirectory.toURI());
			final Path metadataFile = new Path(directory, "metadata_file");
			metadataStore = new FsCheckpointMetadataStreamFactory(fs, directory, metadataFile);
		}

		return new PendingCheckpoint(
			new JobID(),
			0,
			1,
			ackTasks,
			props,
			executor,
			metadataStore);
	}

	@SuppressWarnings("unchecked")
	static void setTaskState(PendingCheckpoint pending, TaskState state) throws NoSuchFieldException, IllegalAccessException {
		Field field = PendingCheckpoint.class.getDeclaredField("taskStates");
		field.setAccessible(true);
		Map<JobVertexID, TaskState> taskStates = (Map<JobVertexID, TaskState>) field.get(pending);

		taskStates.put(new JobVertexID(), state);
	}
}
