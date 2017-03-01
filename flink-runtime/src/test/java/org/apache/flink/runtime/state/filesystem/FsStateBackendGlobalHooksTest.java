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
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.StateBackendGlobalHooks.StateDisposeHook;
import org.apache.flink.runtime.state.StreamStateHandle;

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
import static org.junit.Assert.assertTrue;

/**
 * Tests for the global state dispose hook of the {@link FsStateBackend}.
 */
public class FsStateBackendGlobalHooksTest {

	private final Random rnd = new Random();

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testDisposeCheckpointWithMetadata() throws Exception {
		testDisposeCheckpoint(true);
	}

	@Test
	public void testDisposeNonExternalCheckpoint() throws Exception {
		testDisposeCheckpoint(false);
	}

	private void testDisposeCheckpoint(boolean withMetadata) throws Exception {
		final JobID jid = new JobID();
		final long checkpointId1 = rnd.nextLong() & 0xffffffffL;
		final long checkpointId2 = rnd.nextLong() & 0xffffffffL;

		final FsStateBackend backend = new FsStateBackend(tmp.newFolder().toURI(), 0);

		//noinspection ConstantConditions
		final File checkpointRootDir = new File(backend.getCheckpointPath().getPath());

		final CompletedCheckpoint checkpoint1 = writeCheckpoint(backend, jid, checkpointId1, withMetadata);
		final CompletedCheckpoint checkpoint2 = writeCheckpoint(backend, jid, checkpointId2, withMetadata);

		final StateDisposeHook hook1 = backend.createCheckpointDisposeHook(checkpoint1);
		final StateDisposeHook hook2 = backend.createCheckpointDisposeHook(checkpoint2);
		assertNotNull(hook1);
		assertNotNull(hook2);
		checkpoint1.registerDisposeHook(hook1);
		checkpoint2.registerDisposeHook(hook2);

		// we have two checkpoint dirs in the checkpoint job dir
		assertTrue(checkpointRootDir.exists());
		assertEquals(1, checkpointRootDir.list().length);
		//noinspection ConstantConditions
		assertEquals(2, checkpointRootDir.listFiles()[0].list().length);

		// dispose the first checkpoint
		checkpoint1.discard(JobStatus.FINISHED);

		// the checkpoint job dir should still exist and there is only one checkpoint dir left
		assertTrue(checkpointRootDir.exists());
		assertEquals(1, checkpointRootDir.list().length);
		//noinspection ConstantConditions
		assertEquals(1, checkpointRootDir.listFiles()[0].list().length);

		// dispose the second checkpoint - this should also clean up the root
		checkpoint2.discard(JobStatus.FINISHED);

		assertTrue(checkpointRootDir.exists());
		assertEquals(0, checkpointRootDir.list().length);
	}


	private CompletedCheckpoint writeCheckpoint(
			FsStateBackend backend,
			JobID jid,
			long checkpointId,
			boolean withMetadata) throws IOException {

		final CheckpointMetadataStreamFactory metadataWriter =
				backend.createCheckpointMetadataStreamFactory(jid, checkpointId);

		// write some junk - files not tracked in the checkpoint (simulate some orphaned state objects)
		for (int i = rnd.nextInt(5) + 1; i > 0; i--) {
			try (CheckpointStateOutputStream stream = backend.createStreamFactory(jid, "op" + i)
					.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis()))
			{
				for (int k = rnd.nextInt(100) + 1; k > 0; k--) {
					stream.write(rnd.nextInt());
				}
				stream.closeAndGetHandle();
			}
		}

		final CheckpointProperties props;
		final StreamStateHandle metadataHandle;
		final String pointer;

		if (withMetadata) {
			// write the checkpoint metadata
			try (CheckpointMetadataOutputStream out = metadataWriter.createCheckpointStateOutputStream()) {
				final SavepointV1 metadata = new SavepointV1(checkpointId, Collections.<TaskState>emptyList());
				Checkpoints.storeCheckpointMetadata(metadata, new DataOutputStream(out));

				StreamHandleAndPointer result = out.closeAndGetPointerHandle();
				props = CheckpointProperties.forExternalizedCheckpoint(false);
				metadataHandle = result.stateHandle();
				pointer = result.pointer();
			}
		}
		else {
			props = CheckpointProperties.forStandardCheckpoint();
			metadataHandle = null;
			pointer = null;
		}

		return new CompletedCheckpoint(
				jid, checkpointId,
				System.currentTimeMillis(),
				System.currentTimeMillis() + 5,
				Collections.<JobVertexID, TaskState>emptyMap(),
				props, metadataHandle, pointer);
	}
}
