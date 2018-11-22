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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.fs.local.LocalRecoverableWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link Bucket}.
 */
public class BucketTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@Test
	public void shouldNotCleanupResumablesThatArePartOfTheAckedCheckpoint() throws IOException {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final TestRecoverableWriter recoverableWriter = getRecoverableWriter(path);
		final Bucket<String, String> bucketUnderTest =
				createBucket(recoverableWriter, path, 0, 0);

		bucketUnderTest.write("test-element", 0L);

		final BucketState<String> state = bucketUnderTest.onReceptionOfCheckpoint(0L);
		assertThat(state, hasActiveInProgressFile());

		bucketUnderTest.onSuccessfulCompletionOfCheckpoint(0L);
		assertThat(recoverableWriter, hasCalledDiscard(0)); // it did not discard as this is still valid.
	}

	@Test
	public void shouldCleanupOutdatedResumablesOnCheckpointAck() throws IOException {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final TestRecoverableWriter recoverableWriter = getRecoverableWriter(path);
		final Bucket<String, String> bucketUnderTest =
				createBucket(recoverableWriter, path, 0, 0);

		bucketUnderTest.write("test-element", 0L);

		final BucketState<String> state = bucketUnderTest.onReceptionOfCheckpoint(0L);
		assertThat(state, hasActiveInProgressFile());

		bucketUnderTest.onSuccessfulCompletionOfCheckpoint(0L);

		bucketUnderTest.onReceptionOfCheckpoint(1L);
		bucketUnderTest.onReceptionOfCheckpoint(2L);

		bucketUnderTest.onSuccessfulCompletionOfCheckpoint(2L);
		assertThat(recoverableWriter, hasCalledDiscard(2)); // that is for checkpoints 0 and 1
	}

	@Test
	public void shouldCleanupResumableAfterRestoring() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final TestRecoverableWriter recoverableWriter = getRecoverableWriter(path);
		final Bucket<String, String> bucketUnderTest =
				createBucket(recoverableWriter, path, 0, 0);

		bucketUnderTest.write("test-element", 0L);

		final BucketState<String> state = bucketUnderTest.onReceptionOfCheckpoint(0L);
		assertThat(state, hasActiveInProgressFile());

		bucketUnderTest.onSuccessfulCompletionOfCheckpoint(0L);

		final TestRecoverableWriter newRecoverableWriter = getRecoverableWriter(path);
		restoreBucket(newRecoverableWriter, 0, 1, state);

		assertThat(newRecoverableWriter, hasCalledDiscard(1)); // that is for checkpoints 0 and 1
	}

	@Test
	public void shouldNotCallCleanupWithoutInProgressPartFiles() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final TestRecoverableWriter recoverableWriter = getRecoverableWriter(path);
		final Bucket<String, String> bucketUnderTest =
				createBucket(recoverableWriter, path, 0, 0);

		final BucketState<String> state = bucketUnderTest.onReceptionOfCheckpoint(0L);
		assertThat(state, hasNoActiveInProgressFile());

		bucketUnderTest.onReceptionOfCheckpoint(1L);
		bucketUnderTest.onReceptionOfCheckpoint(2L);

		bucketUnderTest.onSuccessfulCompletionOfCheckpoint(2L);
		assertThat(recoverableWriter, hasCalledDiscard(0)); // we have no in-progress file.
	}

	// ------------------------------- Matchers --------------------------------

	private static TypeSafeMatcher<TestRecoverableWriter> hasCalledDiscard(int times) {
		return new TypeSafeMatcher<TestRecoverableWriter>() {
			@Override
			protected boolean matchesSafely(TestRecoverableWriter writer) {
				return writer.getCleanupCallCounter() == times;
			}

			@Override
			public void describeTo(Description description) {
				description
						.appendText("the TestRecoverableWriter to have called discardRecoverableState() ")
						.appendValue(times)
						.appendText(" times.");
			}
		};
	}

	private static TypeSafeMatcher<BucketState<String>> hasActiveInProgressFile() {
		return new TypeSafeMatcher<BucketState<String>>() {
			@Override
			protected boolean matchesSafely(BucketState<String> state) {
				return state.getInProgressResumableFile() != null;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("a BucketState with active in-progress file.");
			}
		};
	}

	private static TypeSafeMatcher<BucketState<String>> hasNoActiveInProgressFile() {
		return new TypeSafeMatcher<BucketState<String>>() {
			@Override
			protected boolean matchesSafely(BucketState<String> state) {
				return state.getInProgressResumableFile() == null;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("a BucketState with no active in-progress file.");
			}
		};
	}

	// ------------------------------- Mock Classes --------------------------------

	private static class TestRecoverableWriter extends LocalRecoverableWriter {

		private int cleanupCallCounter = 0;

		TestRecoverableWriter(LocalFileSystem fs) {
			super(fs);
		}

		int getCleanupCallCounter() {
			return cleanupCallCounter;
		}

		@Override
		public boolean requiresCleanupOfRecoverableState() {
			return true;
		}

		@Override
		public boolean cleanupRecoverableState(ResumeRecoverable resumable) throws IOException {
			cleanupCallCounter++;
			return super.cleanupRecoverableState(resumable);
		}

		@Override
		public String toString() {
			return "TestRecoverableWriter has called discardRecoverableState() " + cleanupCallCounter + " times.";
		}
	}

	// ------------------------------- Utility Methods --------------------------------

	private static final String bucketId = "testing-bucket";

	private static final RollingPolicy<String, String> rollingPolicy = DefaultRollingPolicy.create().build();

	private static final PartFileWriter.PartFileFactory<String, String> partFileFactory =
			new RowWisePartWriter.Factory<>(new SimpleStringEncoder<>());

	private static Bucket<String, String> createBucket(
			final RecoverableWriter writer,
			final Path bucketPath,
			final int subtaskIdx,
			final int initialPartCounter) {

		return Bucket.getNew(
				writer,
				subtaskIdx,
				bucketId,
				bucketPath,
				initialPartCounter,
				partFileFactory,
				rollingPolicy);
	}

	private static Bucket<String, String> restoreBucket(
			final RecoverableWriter writer,
			final int subtaskIndex,
			final long initialPartCounter,
			final BucketState<String> bucketState) throws Exception {

		return Bucket.restore(
				writer,
				subtaskIndex,
				initialPartCounter,
				partFileFactory,
				rollingPolicy,
				bucketState
		);
	}

	private static TestRecoverableWriter getRecoverableWriter(Path path) {
		try {
			final FileSystem fs = FileSystem.get(path.toUri());
			if (!(fs instanceof LocalFileSystem)) {
				fail("Expected Local FS but got a " + fs.getClass().getName() + " for path: " + path);
			}
			return new TestRecoverableWriter((LocalFileSystem) fs);
		} catch (IOException e) {
			fail();
		}
		return null;
	}
}
