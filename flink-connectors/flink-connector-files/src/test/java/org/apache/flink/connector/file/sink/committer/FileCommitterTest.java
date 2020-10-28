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

package org.apache.flink.connector.file.sink.committer;

import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.connector.file.sink.utils.NoOpBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link FileCommitter}.
 */
public class FileCommitterTest {

	@Test
	public void testCommitPendingFile() throws Exception {
		StubBucketWriter stubBucketWriter = new StubBucketWriter();
		FileCommitter fileCommitter = new FileCommitter(stubBucketWriter);

		FileSinkCommittable fileSinkCommittable = new FileSinkCommittable(
				new FileSinkTestUtils.TestPendingFileRecoverable());
		List<FileSinkCommittable> toRetry = fileCommitter.commit(
				Collections.singletonList(fileSinkCommittable));

		assertEquals(1, stubBucketWriter.getRecoveredPendingFiles().size());
		assertEquals(0, stubBucketWriter.getNumCleanUp());
		assertTrue(stubBucketWriter.getRecoveredPendingFiles().get(0).isCommitted());
		assertEquals(0, toRetry.size());
	}

	@Test
	public void testCleanupInProgressFiles() throws Exception {
		StubBucketWriter stubBucketWriter = new StubBucketWriter();
		FileCommitter fileCommitter = new FileCommitter(stubBucketWriter);

		FileSinkCommittable fileSinkCommittable = new FileSinkCommittable(
				new FileSinkTestUtils.TestInProgressFileRecoverable());
		List<FileSinkCommittable> toRetry = fileCommitter.commit(
				Collections.singletonList(fileSinkCommittable));

		assertEquals(0, stubBucketWriter.getRecoveredPendingFiles().size());
		assertEquals(1, stubBucketWriter.getNumCleanUp());
		assertEquals(0, toRetry.size());
	}

	@Test
	public void testCommitMultiple() throws Exception {
		StubBucketWriter stubBucketWriter = new StubBucketWriter();
		FileCommitter fileCommitter = new FileCommitter(stubBucketWriter);

		List<FileSinkCommittable> committables = Arrays.asList(
				new FileSinkCommittable(new FileSinkTestUtils.TestPendingFileRecoverable()),
				new FileSinkCommittable(new FileSinkTestUtils.TestPendingFileRecoverable()),
				new FileSinkCommittable(new FileSinkTestUtils.TestInProgressFileRecoverable()),
				new FileSinkCommittable(new FileSinkTestUtils.TestPendingFileRecoverable()),
				new FileSinkCommittable(new FileSinkTestUtils.TestInProgressFileRecoverable()));
		List<FileSinkCommittable> toRetry = fileCommitter.commit(committables);

		assertEquals(3, stubBucketWriter.getRecoveredPendingFiles().size());
		assertEquals(2, stubBucketWriter.getNumCleanUp());
		stubBucketWriter
				.getRecoveredPendingFiles()
				.forEach(pendingFile -> assertTrue(pendingFile.isCommitted()));
		assertEquals(0, toRetry.size());
	}

	// ------------------------------- Mock Classes --------------------------------

	private static class RecordingPendingFile implements BucketWriter.PendingFile {
		private boolean committed;

		@Override
		public void commit() throws IOException {
			commitAfterRecovery();
		}

		@Override
		public void commitAfterRecovery() throws IOException {
			committed = true;
		}

		public boolean isCommitted() {
			return committed;
		}
	}

	private static class StubBucketWriter extends NoOpBucketWriter {
		private final List<RecordingPendingFile> recoveredPendingFiles = new ArrayList<>();
		private int numCleanUp;

		@Override
		public BucketWriter.PendingFile recoverPendingFile(
				InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable) throws IOException {
			RecordingPendingFile pendingFile = new RecordingPendingFile();
			recoveredPendingFiles.add(pendingFile);
			return pendingFile;
		}

		@Override
		public boolean cleanupInProgressFileRecoverable(
				InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable) throws IOException {
			numCleanUp++;
			return true;
		}

		public List<RecordingPendingFile> getRecoveredPendingFiles() {
			return recoveredPendingFiles;
		}

		public int getNumCleanUp() {
			return numCleanUp;
		}
	}
}
