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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.utils.NoOpCommitter;
import org.apache.flink.streaming.api.functions.sink.filesystem.utils.NoOpRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.utils.NoOpRecoverableFsDataOutputStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.utils.NoOpRecoverableWriter;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Tests for the {@link Bucket} class.
 */
public class BucketTest {

	private final PartFileWriter.PartFileFactory<String, Integer> factory =
			new RowWisePartWriter.Factory<>(new SimpleStringEncoder<>());

	private final RollingPolicy<String, Integer> rollingPolicy = DefaultRollingPolicy.create().build();

	// --------------------------- Checking Restore ---------------------------

	@Test
	public void inProgressFileShouldBeCommittedIfWriterDoesNotSupportResume() throws IOException {
		final StubNonResumableWriter nonResumableWriter = new StubNonResumableWriter();
		final Bucket<String, Integer> bucket = getRestoredBucketWithOnlyInProgressPart(nonResumableWriter);

		Assert.assertThat(nonResumableWriter, hasMethodCallCountersEqualTo(1, 0, 1));
		Assert.assertThat(bucket, hasNullInProgressFile(true));
	}

	@Test
	public void inProgressFileShouldBeRestoredIfWriterSupportsResume() throws IOException {
		final StubResumableWriter resumableWriter = new StubResumableWriter();
		final Bucket<String, Integer> bucket = getRestoredBucketWithOnlyInProgressPart(resumableWriter);

		Assert.assertThat(resumableWriter, hasMethodCallCountersEqualTo(1, 1, 0));
		Assert.assertThat(bucket, hasNullInProgressFile(false));
	}

	@Test
	public void pendingFilesShouldBeRestored() throws IOException {
		final int expectedRecoverForCommitCounter = 10;

		final StubNonResumableWriter writer = new StubNonResumableWriter();
		final Bucket<String, Integer> bucket = getRestoredBucketWithOnlyPendingParts(writer, expectedRecoverForCommitCounter);

		Assert.assertThat(writer, hasMethodCallCountersEqualTo(0, 0, expectedRecoverForCommitCounter));
		Assert.assertThat(bucket, hasNullInProgressFile(true));
	}

	// ---------------------------------- Matchers ----------------------------------

	private static TypeSafeMatcher<Bucket<String, Integer>> hasNullInProgressFile(final boolean isNull) {

		return new TypeSafeMatcher<Bucket<String, Integer>>() {
			@Override
			protected boolean matchesSafely(Bucket<String, Integer> bucket) {
				final PartFileWriter<String, Integer> inProgressPart = bucket.getInProgressPart();
				return isNull == (inProgressPart == null);
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("a Bucket with its inProgressPart being ")
						.appendText(isNull ? " null." : " not null.");
			}
		};
	}

	private static TypeSafeMatcher<BaseStubWriter> hasMethodCallCountersEqualTo(
			final int supportsResumeCalls,
			final int recoverCalls,
			final int recoverForCommitCalls) {

		return new TypeSafeMatcher<BaseStubWriter>() {
			@Override
			protected boolean matchesSafely(BaseStubWriter writer) {
				return writer.getSupportsResumeCallCounter() == supportsResumeCalls &&
						writer.getRecoverCallCounter() == recoverCalls &&
						writer.getRecoverForCommitCallCounter() == recoverForCommitCalls;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("a Writer where:")
						.appendText(" supportsResume was called ").appendValue(supportsResumeCalls).appendText(" times,")
						.appendText(" recover was called ").appendValue(recoverCalls).appendText(" times,")
						.appendText(" and recoverForCommit was called ").appendValue(recoverForCommitCalls).appendText(" times.")
						.appendText("'");
			}
		};
	}

	// ---------------------------------- Utility Methods ----------------------------------

	private Bucket<String, Integer> getRestoredBucketWithOnlyInProgressPart(final BaseStubWriter writer) throws IOException {
		final BucketState<Integer> stateWithOnlyInProgressFile =
				new BucketState<>(5, new Path(), 12345L, new NoOpRecoverable(), new HashMap<>());
		return Bucket.restore(writer, 0, 1L, factory, rollingPolicy, stateWithOnlyInProgressFile);
	}

	private Bucket<String, Integer> getRestoredBucketWithOnlyPendingParts(final BaseStubWriter writer, final int numberOfPendingParts) throws IOException {
		final Map<Long, List<RecoverableWriter.CommitRecoverable>> completePartsPerCheckpoint =
				createPendingPartsPerCheckpoint(numberOfPendingParts);

		final BucketState<Integer> initStateWithOnlyInProgressFile =
				new BucketState<>(5, new Path(), 12345L, null, completePartsPerCheckpoint);
		return Bucket.restore(writer, 0, 1L, factory, rollingPolicy, initStateWithOnlyInProgressFile);
	}

	private Map<Long, List<RecoverableWriter.CommitRecoverable>> createPendingPartsPerCheckpoint(int noOfCheckpoints) {
		final Map<Long, List<RecoverableWriter.CommitRecoverable>> pendingCommittablesPerCheckpoint = new HashMap<>();
		for (int checkpointId = 0; checkpointId < noOfCheckpoints; checkpointId++) {
			final List<RecoverableWriter.CommitRecoverable> pending = new ArrayList<>();
			pending.add(new NoOpRecoverable());
			pendingCommittablesPerCheckpoint.put((long) checkpointId, pending);
		}
		return pendingCommittablesPerCheckpoint;
	}

	// ---------------------------------- Test Classes ----------------------------------

	/**
	 * A test implementation of a {@link RecoverableWriter} that does not support
	 * resuming, i.e. keep on writing to the in-progress file at the point we were
	 * before the failure.
	 */
	private static class StubResumableWriter extends BaseStubWriter {

		StubResumableWriter() {
			super(true);
		}
	}

	/**
	 * A test implementation of a {@link RecoverableWriter} that does not support
	 * resuming, i.e. keep on writing to the in-progress file at the point we were
	 * before the failure.
	 */
	private static class StubNonResumableWriter extends BaseStubWriter {

		StubNonResumableWriter() {
			super(false);
		}
	}

	/**
	 * A test implementation of a {@link RecoverableWriter} that does not support
	 * resuming, i.e. keep on writing to the in-progress file at the point we were
	 * before the failure.
	 */
	private static class BaseStubWriter extends NoOpRecoverableWriter {

		private final boolean supportsResume;

		private int supportsResumeCallCounter = 0;

		private int recoverCallCounter = 0;

		private int recoverForCommitCallCounter = 0;

		private BaseStubWriter(final boolean supportsResume) {
			this.supportsResume = supportsResume;
		}

		int getSupportsResumeCallCounter() {
			return supportsResumeCallCounter;
		}

		int getRecoverCallCounter() {
			return recoverCallCounter;
		}

		int getRecoverForCommitCallCounter() {
			return recoverForCommitCallCounter;
		}

		@Override
		public RecoverableFsDataOutputStream recover(ResumeRecoverable resumable) throws IOException {
			recoverCallCounter++;
			return new NoOpRecoverableFsDataOutputStream();
		}

		@Override
		public RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable resumable) throws IOException {
			checkArgument(resumable instanceof NoOpRecoverable);
			recoverForCommitCallCounter++;
			return new NoOpCommitter();
		}

		@Override
		public boolean supportsResume() {
			supportsResumeCallCounter++;
			return supportsResume;
		}
	}
}
