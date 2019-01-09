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
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * Tests for different {@link RollingPolicy rolling policies}.
 */
public class RollingPolicyTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@Test
	public void testDefaultRollingPolicy() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final RollingPolicy<String, String> originalRollingPolicy =
				DefaultRollingPolicy
						.create()
						.withMaxPartSize(10L)
						.withInactivityInterval(4L)
						.withRolloverInterval(11L)
						.build();

		final MethodCallCountingPolicyWrapper<String, String> rollingPolicy =
				new MethodCallCountingPolicyWrapper<>(originalRollingPolicy);

		final Buckets<String, String> buckets = createBuckets(path, rollingPolicy);

		rollingPolicy.verifyCallCounters(0L, 0L, 0L, 0L, 0L, 0L);

		// these two will fill up the first in-progress file and at the third it will roll ...
		buckets.onElement("test1", new TestUtils.MockSinkContext(1L, 1L, 1L));
		buckets.onElement("test1", new TestUtils.MockSinkContext(2L, 1L, 2L));
		rollingPolicy.verifyCallCounters(0L, 0L, 1L, 0L, 0L, 0L);

		buckets.onElement("test1", new TestUtils.MockSinkContext(3L, 1L, 3L));
		rollingPolicy.verifyCallCounters(0L, 0L, 2L, 1L, 0L, 0L);

		// still no time to roll
		buckets.onProcessingTime(5L);
		rollingPolicy.verifyCallCounters(0L, 0L, 2L, 1L, 1L, 0L);

		// roll due to inactivity
		buckets.onProcessingTime(7L);
		rollingPolicy.verifyCallCounters(0L, 0L, 2L, 1L, 2L, 1L);

		buckets.onElement("test1", new TestUtils.MockSinkContext(3L, 1L, 3L));

		// roll due to rollover interval
		buckets.onProcessingTime(20L);
		rollingPolicy.verifyCallCounters(0L, 0L, 2L, 1L, 3L, 2L);

		// we take a checkpoint but we should not roll.
		buckets.snapshotState(1L, new TestUtils.MockListState<>(), new TestUtils.MockListState<>());
		rollingPolicy.verifyCallCounters(0L, 0L, 2L, 1L, 3L, 2L);
	}

	@Test
	public void testRollOnCheckpointPolicy() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final MethodCallCountingPolicyWrapper<String, String> rollingPolicy =
				new MethodCallCountingPolicyWrapper<>(OnCheckpointRollingPolicy.build());

		final Buckets<String, String> buckets = createBuckets(path, rollingPolicy);

		rollingPolicy.verifyCallCounters(0L, 0L, 0L, 0L, 0L, 0L);

		buckets.onElement("test1", new TestUtils.MockSinkContext(1L, 1L, 2L));
		buckets.onElement("test1", new TestUtils.MockSinkContext(2L, 1L, 2L));
		buckets.onElement("test1", new TestUtils.MockSinkContext(3L, 1L, 3L));

		// ... we have a checkpoint so we roll ...
		buckets.snapshotState(1L, new TestUtils.MockListState<>(), new TestUtils.MockListState<>());
		rollingPolicy.verifyCallCounters(1L, 1L, 2L, 0L, 0L, 0L);

		// ... create a new in-progress file (before we had closed the last one so it was null)...
		buckets.onElement("test1", new TestUtils.MockSinkContext(5L, 1L, 5L));

		// ... we have a checkpoint so we roll ...
		buckets.snapshotState(2L, new TestUtils.MockListState<>(), new TestUtils.MockListState<>());
		rollingPolicy.verifyCallCounters(2L, 2L, 2L, 0L, 0L, 0L);

		buckets.close();
	}

	@Test
	public void testCustomRollingPolicy() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final MethodCallCountingPolicyWrapper<String, String> rollingPolicy = new MethodCallCountingPolicyWrapper<>(
				new RollingPolicy<String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
						return true;
					}

					@Override
					public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, String element) throws IOException {
						// this means that 2 elements will close the part file.
						return partFileState.getSize() > 9L;
					}

					@Override
					public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileState, long currentTime) {
						return currentTime - partFileState.getLastUpdateTime() >= 10L;
					}
				});

		final Buckets<String, String> buckets = createBuckets(path, rollingPolicy);

		rollingPolicy.verifyCallCounters(0L, 0L, 0L, 0L, 0L, 0L);

		// the following 2 elements will close a part file because of size...
		buckets.onElement("test1", new TestUtils.MockSinkContext(1L, 1L, 2L));
		buckets.onElement("test1", new TestUtils.MockSinkContext(2L, 1L, 2L));

		// only one call because we have no open part file in the other incoming elements, so currentPartFile == null so we roll without checking the policy.
		rollingPolicy.verifyCallCounters(0L, 0L, 1L, 0L, 0L, 0L);

		// ... and this one will trigger the roll and open a new part file...
		buckets.onElement("test1", new TestUtils.MockSinkContext(2L, 1L, 2L));
		rollingPolicy.verifyCallCounters(0L, 0L, 2L, 1L, 0L, 0L);

		// ... we have a checkpoint so we roll ...
		buckets.snapshotState(1L, new TestUtils.MockListState<>(), new TestUtils.MockListState<>());
		rollingPolicy.verifyCallCounters(1L, 1L, 2L, 1L, 0L, 0L);

		// ... create a new in-progress file (before we had closed the last one so it was null)...
		buckets.onElement("test1", new TestUtils.MockSinkContext(2L, 1L, 5L));

		// ... last modification time is 5L, so now we DON'T roll but we check ...
		buckets.onProcessingTime(12L);
		rollingPolicy.verifyCallCounters(1L, 1L, 2L, 1L, 1L, 0L);

		// ... last modification time is 5L, so now we roll
		buckets.onProcessingTime(16L);
		rollingPolicy.verifyCallCounters(1L, 1L, 2L, 1L, 2L, 1L);

		buckets.close();
	}

	// ------------------------------- Utility Methods --------------------------------

	private static Buckets<String, String> createBuckets(
			final Path basePath,
			final MethodCallCountingPolicyWrapper<String, String> rollingPolicyToTest
	) throws IOException {

		return new Buckets<>(
				basePath,
				new TestUtils.StringIdentityBucketAssigner(),
				new DefaultBucketFactoryImpl<>(),
				new RowWisePartWriter.Factory<>(new SimpleStringEncoder<>()),
				rollingPolicyToTest,
				0
		);
	}

	/**
	 * A wrapper of a {@link RollingPolicy} which counts how many times each method of the policy was called
	 * and in how many of them it decided to roll.
	 */
	private static class MethodCallCountingPolicyWrapper<IN, BucketID> implements RollingPolicy<IN, BucketID> {

		private static final long serialVersionUID = 1L;

		private final RollingPolicy<IN, BucketID> originalPolicy;

		private long onCheckpointCallCounter;
		private long onCheckpointRollCounter;

		private long onEventCallCounter;
		private long onEventRollCounter;

		private long onProcessingTimeCallCounter;
		private long onProcessingTimeRollCounter;

		MethodCallCountingPolicyWrapper(final RollingPolicy<IN, BucketID> policy) {
			this.originalPolicy = Preconditions.checkNotNull(policy);

			this.onCheckpointCallCounter = 0L;
			this.onCheckpointRollCounter = 0L;

			this.onEventCallCounter = 0L;
			this.onEventRollCounter = 0L;

			this.onProcessingTimeCallCounter = 0L;
			this.onProcessingTimeRollCounter = 0L;
		}

		@Override
		public boolean shouldRollOnCheckpoint(PartFileInfo<BucketID> partFileState) throws IOException {
			final boolean shouldRoll = originalPolicy.shouldRollOnCheckpoint(partFileState);
			this.onCheckpointCallCounter++;
			if (shouldRoll) {
				this.onCheckpointRollCounter++;
			}
			return shouldRoll;
		}

		@Override
		public boolean shouldRollOnEvent(PartFileInfo<BucketID> partFileState, IN element) throws IOException {
			final boolean shouldRoll = originalPolicy.shouldRollOnEvent(partFileState, element);
			this.onEventCallCounter++;
			if (shouldRoll) {
				this.onEventRollCounter++;
			}
			return shouldRoll;
		}

		@Override
		public boolean shouldRollOnProcessingTime(PartFileInfo<BucketID> partFileState, long currentTime) throws IOException {
			final boolean shouldRoll = originalPolicy.shouldRollOnProcessingTime(partFileState, currentTime);
			this.onProcessingTimeCallCounter++;
			if (shouldRoll) {
				this.onProcessingTimeRollCounter++;
			}
			return shouldRoll;
		}

		void verifyCallCounters(
				final long onCheckpointCalls,
				final long onCheckpointRolls,
				final long onEventCalls,
				final long onEventRolls,
				final long onProcessingTimeCalls,
				final long onProcessingTimeRolls
		) {
			Assert.assertEquals(onCheckpointCalls, onCheckpointCallCounter);
			Assert.assertEquals(onCheckpointRolls, onCheckpointRollCounter);
			Assert.assertEquals(onEventCalls, onEventCallCounter);
			Assert.assertEquals(onEventRolls, onEventRollCounter);
			Assert.assertEquals(onProcessingTimeCalls, onProcessingTimeCallCounter);
			Assert.assertEquals(onProcessingTimeRolls, onProcessingTimeRollCounter);
		}
	}
}
