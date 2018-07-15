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

import org.apache.flink.api.common.serialization.SimpleStringWriter;
import org.apache.flink.api.common.serialization.Writer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.ResumableWriter;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.Bucketer;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for the {@link StreamingFileSink}.
 */
public class LocalStreamingFileSinkTest extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@Test
	public void testClosingWithoutInput() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness =
				createRescalingTestSink(outDir, 1, 0, 100L, 124L);
		testHarness.setup();
		testHarness.open();

		testHarness.close();
	}

	@Test
	public void testTruncateAfterRecoveryAndOverwrite() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		OperatorSubtaskState snapshot;

		// we set the max bucket size to small so that we can know when it rolls
		try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness = createRescalingTestSink(
				outDir, 1, 0, 100L, 10L)) {

			testHarness.setup();
			testHarness.open();

			// this creates a new bucket "test1" and part-0-0
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 1), 1L));
			checkLocalFs(outDir, 1, 0);

			// we take a checkpoint so that we keep the in-progress file offset.
			snapshot = testHarness.snapshot(1L, 1L);

			// these will close part-0-0 and open part-0-1
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 2), 2L));
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 3), 3L));

			checkLocalFs(outDir, 2, 0);

			Map<File, String> contents = getFileContentByPath(outDir);
			int fileCounter = 0;
			for (Map.Entry<File, String> fileContents : contents.entrySet()) {
				if (fileContents.getKey().getName().contains(".part-0-0.inprogress")) {
					fileCounter++;
					Assert.assertEquals("test1@1\ntest1@2\n", fileContents.getValue());
				} else if (fileContents.getKey().getName().contains(".part-0-1.inprogress")) {
					fileCounter++;
					Assert.assertEquals("test1@3\n", fileContents.getValue());
				}
			}
			Assert.assertEquals(2L, fileCounter);
		}

		try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness = createRescalingTestSink(
				outDir, 1, 0, 100L, 10L)) {

			testHarness.setup();
			testHarness.initializeState(snapshot);
			testHarness.open();

			// the in-progress is the not cleaned up one and the pending is truncated and finalized
			checkLocalFs(outDir, 2, 0);

			// now we go back to the first checkpoint so it should truncate part-0-0 and restart part-0-1
			int fileCounter = 0;
			for (Map.Entry<File, String> fileContents : getFileContentByPath(outDir).entrySet()) {
				if (fileContents.getKey().getName().contains(".part-0-0.inprogress")) {
					// truncated
					fileCounter++;
					Assert.assertEquals("test1@1\n", fileContents.getValue());
				} else if (fileContents.getKey().getName().contains(".part-0-1.inprogress")) {
					// ignored for now as we do not clean up. This will be overwritten.
					fileCounter++;
					Assert.assertEquals("test1@3\n", fileContents.getValue());
				}
			}
			Assert.assertEquals(2L, fileCounter);

			// the first closes part-0-0 and the second will open part-0-1
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 4), 4L));

			fileCounter = 0;
			for (Map.Entry<File, String> fileContents : getFileContentByPath(outDir).entrySet()) {
				if (fileContents.getKey().getName().contains(".part-0-0.inprogress")) {
					fileCounter++;
					Assert.assertEquals("test1@1\ntest1@4\n", fileContents.getValue());
				} else if (fileContents.getKey().getName().contains(".part-0-1.inprogress")) {
					// ignored for now as we do not clean up. This will be overwritten.
					fileCounter++;
					Assert.assertEquals("test1@3\n", fileContents.getValue());
				}
			}
			Assert.assertEquals(2L, fileCounter);

			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 5), 5L));
			checkLocalFs(outDir, 3, 0); // the previous part-0-1 in progress is simply ignored (random extension)

			testHarness.snapshot(2L, 2L);

			// this will close the new part-0-1
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 6), 6L));
			checkLocalFs(outDir, 3, 0);

			fileCounter = 0;
			for (Map.Entry<File, String> fileContents : getFileContentByPath(outDir).entrySet()) {
				if (fileContents.getKey().getName().contains(".part-0-0.inprogress")) {
					fileCounter++;
					Assert.assertEquals("test1@1\ntest1@4\n", fileContents.getValue());
				} else if (fileContents.getKey().getName().contains(".part-0-1.inprogress")) {
					if (fileContents.getValue().equals("test1@5\ntest1@6\n") ||
							fileContents.getValue().equals("test1@3\n")) {
						fileCounter++;
					}
				}
			}
			Assert.assertEquals(3L, fileCounter);

			// this will publish part-0-0
			testHarness.notifyOfCompletedCheckpoint(2L);
			checkLocalFs(outDir, 2, 1);

			fileCounter = 0;
			for (Map.Entry<File, String> fileContents : getFileContentByPath(outDir).entrySet()) {
				if (fileContents.getKey().getName().equals("part-0-0")) {
					fileCounter++;
					Assert.assertEquals("test1@1\ntest1@4\n", fileContents.getValue());
				} else if (fileContents.getKey().getName().contains(".part-0-1.inprogress")) {
					if (fileContents.getValue().equals("test1@5\ntest1@6\n") ||
							fileContents.getValue().equals("test1@3\n")) {
						fileCounter++;
					}
				}
			}
			Assert.assertEquals(3L, fileCounter);
		}
	}

	@Test
	public void testCommitStagedFilesInCorrectOrder() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();

		// we set the max bucket size to small so that we can know when it rolls
		try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness = createRescalingTestSink(
				outDir, 1, 0, 100L, 10L)) {

			testHarness.setup();
			testHarness.open();

			testHarness.setProcessingTime(0L);

			// these 2 create a new bucket "test1", with a .part-0-0.inprogress and also fill it
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 1), 1L));
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 2), 2L));
			checkLocalFs(outDir, 1, 0);

			// this will open .part-0-1.inprogress
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 3), 3L));
			checkLocalFs(outDir, 2, 0);

			// we take a checkpoint so that we keep the in-progress file offset.
			testHarness.snapshot(1L, 1L);

			// this will close .part-0-1.inprogress
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 4), 4L));

			// and open and fill .part-0-2.inprogress
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 5), 5L));
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 6), 6L));
			checkLocalFs(outDir, 3, 0);                    // nothing committed yet

			testHarness.snapshot(2L, 2L);

			// open .part-0-3.inprogress
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 7), 7L));
			checkLocalFs(outDir, 4, 0);

			// this will close the part file (time)
			testHarness.setProcessingTime(101L);

			testHarness.snapshot(3L, 3L);

			testHarness.notifyOfCompletedCheckpoint(1L);							// the pending for checkpoint 1 are committed
			checkLocalFs(outDir, 3, 1);

			int fileCounter = 0;
			for (Map.Entry<File, String> fileContents : getFileContentByPath(outDir).entrySet()) {
				if (fileContents.getKey().getName().equals("part-0-0")) {
					fileCounter++;
					Assert.assertEquals("test1@1\ntest1@2\n", fileContents.getValue());
				} else if (fileContents.getKey().getName().contains(".part-0-1.inprogress")) {
					fileCounter++;
					Assert.assertEquals("test1@3\ntest1@4\n", fileContents.getValue());
				} else if (fileContents.getKey().getName().contains(".part-0-2.inprogress")) {
					fileCounter++;
					Assert.assertEquals("test1@5\ntest1@6\n", fileContents.getValue());
				} else if (fileContents.getKey().getName().contains(".part-0-3.inprogress")) {
					fileCounter++;
					Assert.assertEquals("test1@7\n", fileContents.getValue());
				}
			}
			Assert.assertEquals(4L, fileCounter);

			testHarness.notifyOfCompletedCheckpoint(3L);							// all the pending for checkpoint 2 and 3 are committed
			checkLocalFs(outDir, 0, 4);

			fileCounter = 0;
			for (Map.Entry<File, String> fileContents : getFileContentByPath(outDir).entrySet()) {
				if (fileContents.getKey().getName().equals("part-0-0")) {
					fileCounter++;
					Assert.assertEquals("test1@1\ntest1@2\n", fileContents.getValue());
				} else if (fileContents.getKey().getName().equals("part-0-1")) {
					fileCounter++;
					Assert.assertEquals("test1@3\ntest1@4\n", fileContents.getValue());
				} else if (fileContents.getKey().getName().equals("part-0-2")) {
					fileCounter++;
					Assert.assertEquals("test1@5\ntest1@6\n", fileContents.getValue());
				} else if (fileContents.getKey().getName().equals("part-0-3")) {
					fileCounter++;
					Assert.assertEquals("test1@7\n", fileContents.getValue());
				}
			}
			Assert.assertEquals(4L, fileCounter);
		}
	}

	@Test
	public void testInactivityPeriodWithLateNotify() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();

		// we set a big bucket size so that it does not close by size, but by timers.
		try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness = createRescalingTestSink(
				outDir, 1, 0, 100L, 124L)) {

			testHarness.setup();
			testHarness.open();

			testHarness.setProcessingTime(0L);

			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 1), 1L));
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test2", 1), 1L));
			checkLocalFs(outDir, 2, 0);

			int bucketCounter = 0;
			for (Map.Entry<File, String> fileContents : getFileContentByPath(outDir).entrySet()) {
				if (fileContents.getKey().getParentFile().getName().equals("test1")) {
					bucketCounter++;
				} else if (fileContents.getKey().getParentFile().getName().equals("test2")) {
					bucketCounter++;
				}
			}
			Assert.assertEquals(2L, bucketCounter);					// verifies that we have 2 buckets, "test1" and "test2"

			testHarness.setProcessingTime(101L);                                // put them in pending
			checkLocalFs(outDir, 2, 0);

			testHarness.snapshot(0L, 0L);                // put them in pending for 0
			checkLocalFs(outDir, 2, 0);

			// create another 2 buckets with 1 inprogress file each
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test3", 1), 1L));
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test4", 1), 1L));

			testHarness.setProcessingTime(202L);                                // put them in pending

			testHarness.snapshot(1L, 0L);                // put them in pending for 1
			checkLocalFs(outDir, 4, 0);

			testHarness.notifyOfCompletedCheckpoint(0L);            // put the pending for 0 to the "committed" state
			checkLocalFs(outDir, 2, 2);

			bucketCounter = 0;
			for (Map.Entry<File, String> fileContents : getFileContentByPath(outDir).entrySet()) {
				if (fileContents.getKey().getParentFile().getName().equals("test1")) {
					bucketCounter++;
					Assert.assertEquals("part-0-0", fileContents.getKey().getName());
					Assert.assertEquals("test1@1\n", fileContents.getValue());
				} else if (fileContents.getKey().getParentFile().getName().equals("test2")) {
					bucketCounter++;
					Assert.assertEquals("part-0-0", fileContents.getKey().getName());
					Assert.assertEquals("test2@1\n", fileContents.getValue());
				} else if (fileContents.getKey().getParentFile().getName().equals("test3")) {
					bucketCounter++;
				} else if (fileContents.getKey().getParentFile().getName().equals("test4")) {
					bucketCounter++;
				}
			}
			Assert.assertEquals(4L, bucketCounter);

			testHarness.notifyOfCompletedCheckpoint(1L);            // put the pending for 1 to the "committed" state
			checkLocalFs(outDir, 0, 4);

			bucketCounter = 0;
			for (Map.Entry<File, String> fileContents : getFileContentByPath(outDir).entrySet()) {
				if (fileContents.getKey().getParentFile().getName().equals("test1")) {
					bucketCounter++;
					Assert.assertEquals("test1@1\n", fileContents.getValue());
				} else if (fileContents.getKey().getParentFile().getName().equals("test2")) {
					bucketCounter++;
					Assert.assertEquals("test2@1\n", fileContents.getValue());
				} else if (fileContents.getKey().getParentFile().getName().equals("test3")) {
					bucketCounter++;
					Assert.assertEquals("part-0-0", fileContents.getKey().getName());
					Assert.assertEquals("test3@1\n", fileContents.getValue());
				} else if (fileContents.getKey().getParentFile().getName().equals("test4")) {
					bucketCounter++;
					Assert.assertEquals("part-0-0", fileContents.getKey().getName());
					Assert.assertEquals("test4@1\n", fileContents.getValue());
				}
			}
			Assert.assertEquals(4L, bucketCounter);
		}
	}

	@Test
	public void testClosingOnSnapshot() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();

		try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness =
					createRescalingTestSink(outDir, 1, 0, 100L, 2L)) {

			testHarness.setup();
			testHarness.open();

			testHarness.setProcessingTime(0L);

			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 1), 1L));
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test2", 1), 1L));
			checkLocalFs(outDir, 2, 0);

			// this is to check the inactivity threshold
			testHarness.setProcessingTime(101L);
			checkLocalFs(outDir, 2, 0);

			testHarness.processElement(new StreamRecord<>(Tuple2.of("test3", 1), 1L));
			checkLocalFs(outDir, 3, 0);

			testHarness.snapshot(0L, 1L);
			checkLocalFs(outDir, 3, 0);

			testHarness.notifyOfCompletedCheckpoint(0L);
			checkLocalFs(outDir, 0, 3);

			testHarness.snapshot(1L, 0L);

			testHarness.processElement(new StreamRecord<>(Tuple2.of("test4", 10), 10L));
			checkLocalFs(outDir, 1, 3);
		}

		// at close it is not moved to final.
		checkLocalFs(outDir, 1, 3);
	}

	@Test
	public void testScalingDownAndMergingOfStates() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();

		OperatorSubtaskState mergedSnapshot;

		// we set small file size so that the part file rolls.
		try (
				OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness1 = createRescalingTestSink(
						outDir, 2, 0, 100L, 10L);
				OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness2 = createRescalingTestSink(
						outDir, 2, 1, 100L, 10L)
		) {
			testHarness1.setup();
			testHarness1.open();

			testHarness2.setup();
			testHarness2.open();

			testHarness1.processElement(new StreamRecord<>(Tuple2.of("test1", 0), 0L));
			checkLocalFs(outDir, 1, 0);

			testHarness2.processElement(new StreamRecord<>(Tuple2.of("test1", 1), 1L));
			testHarness2.processElement(new StreamRecord<>(Tuple2.of("test2", 1), 1L));

			// all the files are in-progress
			checkLocalFs(outDir, 3, 0);

			int counter = 0;
			for (Map.Entry<File, String> fileContents : getFileContentByPath(outDir).entrySet()) {
				final String parentFilename = fileContents.getKey().getParentFile().getName();
				final String inProgressFilename = fileContents.getKey().getName();

				if (parentFilename.equals("test1") &&
						(
							inProgressFilename.contains(".part-0-0.inprogress") ||
							inProgressFilename.contains(".part-1-0.inprogress")
						)
				) {
						counter++;
				} else if (parentFilename.equals("test2") && inProgressFilename.contains(".part-1-0.inprogress")) {
					counter++;
				}
			}
			Assert.assertEquals(3L, counter);

			// intentionally we snapshot them in the reverse order so that the states are shuffled
			mergedSnapshot = AbstractStreamOperatorTestHarness.repackageState(
					testHarness1.snapshot(1L, 0L),
					testHarness2.snapshot(1L, 0L)
			);
		}

		try (
				OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness = createRescalingTestSink(
						outDir, 1, 0, 100L, 10L)
		) {
			testHarness.setup();
			testHarness.initializeState(mergedSnapshot);
			testHarness.open();

			// still everything in-progress but the in-progress for prev task 1 should be put in pending now
			checkLocalFs(outDir, 3, 0);

			testHarness.snapshot(2L, 2L);
			testHarness.notifyOfCompletedCheckpoint(2L);

			int counter = 0;
			for (Map.Entry<File, String> fileContents : getFileContentByPath(outDir).entrySet()) {
				final String parentFilename = fileContents.getKey().getParentFile().getName();
				final String filename = fileContents.getKey().getName();

				if (parentFilename.equals("test1")) {
					// the following is because it depends on the order in which the states are consumed in the initialize state.
					if (filename.contains("-0.inprogress") || filename.endsWith("-0")) {
						counter++;
						Assert.assertTrue(fileContents.getValue().equals("test1@1\n") || fileContents.getValue().equals("test1@0\n"));
					}
				} else if (parentFilename.equals("test2") && filename.contains(".part-1-0.inprogress")) {
					counter++;
					Assert.assertEquals("test2@1\n", fileContents.getValue());
				}
			}
			Assert.assertEquals(3L, counter);
		}
	}

	@Test
	public void testMaxCounterUponRecovery() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();

		OperatorSubtaskState mergedSnapshot;

		final TestBucketFactory first = new TestBucketFactory();
		final TestBucketFactory second = new TestBucketFactory();

		try (
				OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness1 = createCustomRescalingTestSink(
						outDir, 2, 0, 100L, 2L, first, new SimpleStringWriter<>());
				OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness2 = createCustomRescalingTestSink(
						outDir, 2, 1, 100L, 2L, second, new SimpleStringWriter<>())
		) {
			testHarness1.setup();
			testHarness1.open();

			testHarness2.setup();
			testHarness2.open();

			// we only put elements in one task.
			testHarness1.processElement(new StreamRecord<>(Tuple2.of("test1", 0), 0L));
			testHarness1.processElement(new StreamRecord<>(Tuple2.of("test1", 0), 0L));
			testHarness1.processElement(new StreamRecord<>(Tuple2.of("test1", 0), 0L));
			checkLocalFs(outDir, 3, 0);

			// intentionally we snapshot them in the reverse order so that the states are shuffled
			mergedSnapshot = AbstractStreamOperatorTestHarness.repackageState(
					testHarness2.snapshot(0L, 0L),
					testHarness1.snapshot(0L, 0L)
			);
		}

		final TestBucketFactory firstRecovered = new TestBucketFactory();
		final TestBucketFactory secondRecovered = new TestBucketFactory();

		try (
				OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness1 = createCustomRescalingTestSink(
						outDir, 2, 0, 100L, 2L, firstRecovered, new SimpleStringWriter<>());
				OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness2 = createCustomRescalingTestSink(
						outDir, 2, 1, 100L, 2L, secondRecovered, new SimpleStringWriter<>())
		) {
			testHarness1.setup();
			testHarness1.initializeState(mergedSnapshot);
			testHarness1.open();

			// we have to send an element so that the factory updates its counter.
			testHarness1.processElement(new StreamRecord<>(Tuple2.of("test4", 0), 0L));

			Assert.assertEquals(3L, firstRecovered.getInitialCounter());
			checkLocalFs(outDir, 1, 3);

			testHarness2.setup();
			testHarness2.initializeState(mergedSnapshot);
			testHarness2.open();

			// we have to send an element so that the factory updates its counter.
			testHarness2.processElement(new StreamRecord<>(Tuple2.of("test2", 0), 0L));

			Assert.assertEquals(3L, secondRecovered.getInitialCounter());
			checkLocalFs(outDir, 2, 3);
		}
	}

	//////////////////////			Helper Methods			//////////////////////

	private OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> createRescalingTestSink(
			File outDir,
			int totalParallelism,
			int taskIdx,
			long inactivityInterval,
			long partMaxSize) throws Exception {

		return createCustomRescalingTestSink(
				outDir,
				totalParallelism,
				taskIdx,
				inactivityInterval,
				partMaxSize,
				new DefaultBucketFactory<>(),
				(Writer<Tuple2<String, Integer>>) (element, stream) -> {
					stream.write((element.f0 + '@' + element.f1).getBytes(StandardCharsets.UTF_8));
					stream.write('\n');
				});
	}

	private OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> createCustomRescalingTestSink(
			File outDir,
			int totalParallelism,
			int taskIdx,
			long inactivityInterval,
			long partMaxSize,
			BucketFactory<Tuple2<String, Integer>> factory,
			Writer<Tuple2<String, Integer>> writer) throws Exception {

		StreamingFileSink<Tuple2<String, Integer>> sink = new StreamingFileSink<>(new Path(outDir.toURI()), factory)
				.setBucketer(new Bucketer<Tuple2<String, Integer>>() {

					private static final long serialVersionUID = -3086487303018372007L;

					@Override
					public String getBucketId(Tuple2<String, Integer> element, SinkFunction.Context context) {
						return element.f0;
					}
				})
				.setWriter(writer)
				.setRollingPolicy(
						new DefaultRollingPolicy()
								.withMaxPartSize(partMaxSize)
								.withRolloverInterval(inactivityInterval)
								.withInactivityInterval(inactivityInterval))
				.setBucketCheckInterval(10L);

		return new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), 10, totalParallelism, taskIdx);
	}

	static class TestBucketFactory extends DefaultBucketFactory<Tuple2<String, Integer>> {

		private static final long serialVersionUID = 2794824980604027930L;

		private long initialCounter = -1L;

		@Override
		public Bucket<Tuple2<String, Integer>> getBucket(
				ResumableWriter fsWriter,
				int subtaskIndex,
				String bucketId,
				Path bucketPath,
				long initialPartCounter,
				Writer<Tuple2<String, Integer>> writer) throws IOException {

			this.initialCounter = initialPartCounter;

			return super.getBucket(
					fsWriter,
					subtaskIndex,
					bucketId,
					bucketPath,
					initialPartCounter,
					writer);
		}

		@Override
		public Bucket<Tuple2<String, Integer>> getBucket(
				ResumableWriter fsWriter,
				int subtaskIndex,
				long initialPartCounter,
				Writer<Tuple2<String, Integer>> writer,
				BucketState bucketState) throws IOException {

			this.initialCounter = initialPartCounter;

			return super.getBucket(
					fsWriter,
					subtaskIndex,
					initialPartCounter,
					writer,
					bucketState);
		}

		public long getInitialCounter() {
			return initialCounter;
		}
	}

	private static void checkLocalFs(File outDir, int expectedInProgress, int expectedCompleted) {
		int inProgress = 0;
		int finished = 0;

		for (File file: FileUtils.listFiles(outDir, null, true)) {
			if (file.getAbsolutePath().endsWith("crc")) {
				continue;
			}

			if (file.toPath().getFileName().toString().startsWith(".")) {
				inProgress++;
			} else {
				finished++;
			}
		}

		Assert.assertEquals(expectedInProgress, inProgress);
		Assert.assertEquals(expectedCompleted, finished);
	}

	private static Map<File, String> getFileContentByPath(File directory) throws IOException {
		Map<File, String> contents = new HashMap<>(4);

		final Collection<File> filesInBucket = FileUtils.listFiles(directory, null, true);
		for (File file : filesInBucket) {
			contents.put(file, FileUtils.readFileToString(file));
		}
		return contents;
	}
}
