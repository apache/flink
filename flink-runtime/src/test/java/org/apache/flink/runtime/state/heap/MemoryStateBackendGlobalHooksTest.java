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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV1;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory.StreamHandleAndPointer;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendGlobalHooks.StateDisposeHook;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the global state dispose hook of the memory state backend.
 */
public class MemoryStateBackendGlobalHooksTest {

	private final Random rnd = new Random();

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	// ------------------------------------------------------------------------
	//  Checkpoint Dispose
	// ------------------------------------------------------------------------

	/**
	 * Checks that a checkpoint directory with a _metadata file will be deleted the the
	 * dispose hook.
	 */
	@Test
	public void testDisposeCheckpointWithMetadata() throws Exception {
		final JobID jid = new JobID();
		final long checkpointId1 = rnd.nextLong() & 0xffffffffL;
		final long checkpointId2 = rnd.nextLong() & 0xffffffffL;

		final MemoryStateBackend backend = new MemoryStateBackend(tmp.newFolder().toURI().toString(), null);

		//noinspection ConstantConditions
		final File checkpointRootDir = new File(backend.getCheckpointPath().getPath());

		final CompletedCheckpoint checkpoint1 = writeCheckpoint(backend, jid, checkpointId1);
		final CompletedCheckpoint checkpoint2 = writeCheckpoint(backend, jid, checkpointId2);

		final StateDisposeHook hook1 = backend.createCheckpointDisposeHook(checkpoint1);
		final StateDisposeHook hook2 = backend.createCheckpointDisposeHook(checkpoint2);
		assertNotNull(hook1);
		assertNotNull(hook2);
		checkpoint1.registerDisposeHook(hook1);
		checkpoint2.registerDisposeHook(hook2);

		// dispose the first checkpoint - the checkpoint job dir should still exist
		checkpoint1.discard(JobStatus.FINISHED);

		assertTrue(checkpointRootDir.exists());
		assertEquals(1, checkpointRootDir.list().length);

		// dispose the second checkpoint - this should also clean up the root
		checkpoint2.discard(JobStatus.FINISHED);

		assertTrue(checkpointRootDir.exists());
		assertEquals(0, checkpointRootDir.list().length);
	}

	/**
	 * Checks that a checkpoint directory with a _metadata file will be deleted the the
	 * dispose hook.
	 */
	@Test
	public void testDisposeNonExternalCheckpoint() throws Exception {
		final JobID jid = new JobID();
		final long checkpointId = rnd.nextLong() & 0xffffffffL;

		final MemoryStateBackend backend = new MemoryStateBackend(tmp.newFolder().toURI().toString(), null);

		final CompletedCheckpoint checkpoint = new CompletedCheckpoint(
				jid, checkpointId,
				System.currentTimeMillis(),
				System.currentTimeMillis() + 5,
				Collections.<JobVertexID, TaskState>emptyMap(),
				CheckpointProperties.forStandardCheckpoint(),
				null, null);

		final StateDisposeHook hook = backend.createCheckpointDisposeHook(checkpoint);

		// no hook, since there is no state to drop
		assertNull(hook);

		// regular registration etc must still work
		checkpoint.registerDisposeHook(null);
		checkpoint.discard(JobStatus.FINISHED);
	}

	private static CompletedCheckpoint writeCheckpoint(
			StateBackend backend,
			JobID jid,
			long checkpointId) throws IOException {

		final CheckpointMetadataStreamFactory metadataWriter =
				backend.createCheckpointMetadataStreamFactory(jid, checkpointId);

		try (CheckpointMetadataOutputStream out = metadataWriter.createCheckpointStateOutputStream()) {
			final SavepointV1 metadata = new SavepointV1(checkpointId, Collections.<TaskState>emptyList());
			Checkpoints.storeCheckpointMetadata(metadata, new DataOutputStream(out));

			StreamHandleAndPointer result = out.closeAndGetPointerHandle();

			return new CompletedCheckpoint(
					jid, checkpointId,
					System.currentTimeMillis(),
					System.currentTimeMillis() + 5,
					Collections.<JobVertexID, TaskState>emptyMap(),
					CheckpointProperties.forExternalizedCheckpoint(false),
					result.stateHandle(), result.pointer());
		}
	}
}
