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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Tests for different {@link RollingPolicy rolling policies}.
 */
public class RollingPolicyTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@Test
	public void testDefaultRollingPolicy() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();

		final RollingPolicy<Tuple2<String, Integer>, String> rollingPolicy = DefaultRollingPolicy
				.create()
				.withMaxPartSize(10L)
				.withInactivityInterval(4L)
				.withRolloverInterval(11L)
				.build();

		try (
				OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness = TestUtils.createCustomRescalingTestSink(
						outDir,
						1,
						0,
						1L,
						new TestUtils.TupleToStringBucketer(),
						new SimpleStringEncoder<>(),
						rollingPolicy,
						new DefaultBucketFactoryImpl<>())
		) {
			testHarness.setup();
			testHarness.open();

			testHarness.setProcessingTime(0L);

			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 1), 1L));
			TestUtils.checkLocalFs(outDir, 1, 0);

			// roll due to size
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 2), 2L));
			TestUtils.checkLocalFs(outDir, 1, 0);

			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 3), 3L));
			TestUtils.checkLocalFs(outDir, 2, 0);

			// roll due to inactivity
			testHarness.setProcessingTime(7L);

			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 4), 4L));
			TestUtils.checkLocalFs(outDir, 3, 0);

			// roll due to rollover interval
			testHarness.setProcessingTime(20L);

			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 5), 5L));
			TestUtils.checkLocalFs(outDir, 4, 0);

			// we take a checkpoint but we should not roll.
			testHarness.snapshot(1L, 1L);

			TestUtils.checkLocalFs(outDir, 4, 0);

			// acknowledge the checkpoint, so publish the 3 closed files, but not the open one.
			testHarness.notifyOfCompletedCheckpoint(1L);
			TestUtils.checkLocalFs(outDir, 1, 3);
		}
	}

	@Test
	public void testRollOnCheckpointPolicy() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();

		final RollingPolicy<Tuple2<String, Integer>, String> rollingPolicy = OnCheckpointRollingPolicy.build();

		try (
				OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness = TestUtils.createCustomRescalingTestSink(
						outDir,
						1,
						0,
						10L,
						new TestUtils.TupleToStringBucketer(),
						new SimpleStringEncoder<>(),
						rollingPolicy,
						new DefaultBucketFactoryImpl<>())
		) {
			testHarness.setup();
			testHarness.open();

			testHarness.setProcessingTime(0L);

			testHarness.processElement(new StreamRecord<>(Tuple2.of("test2", 1), 1L));

			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 1), 1L));
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 2), 2L));
			TestUtils.checkLocalFs(outDir, 2, 0);

			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 3), 3L));
			TestUtils.checkLocalFs(outDir, 2, 0);

			// we take a checkpoint so we roll.
			testHarness.snapshot(1L, 1L);

			for (File file: FileUtils.listFiles(outDir, null, true)) {
				if (Objects.equals(file.getParentFile().getName(), "test1")) {
					Assert.assertTrue(file.getName().contains(".part-0-1.inprogress."));
				} else if (Objects.equals(file.getParentFile().getName(), "test2")) {
					Assert.assertTrue(file.getName().contains(".part-0-0.inprogress."));
				}
			}

			// this will create a new part file
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 4), 4L));
			TestUtils.checkLocalFs(outDir, 3, 0);

			testHarness.notifyOfCompletedCheckpoint(1L);
			for (File file: FileUtils.listFiles(outDir, null, true)) {
				if (Objects.equals(file.getParentFile().getName(), "test1")) {
					Assert.assertTrue(
							file.getName().contains(".part-0-2.inprogress.") || file.getName().equals("part-0-1")
					);
				} else if (Objects.equals(file.getParentFile().getName(), "test2")) {
					Assert.assertEquals("part-0-0", file.getName());
				}
			}

			// and open and fill .part-0-2.inprogress
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 5), 5L));
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 6), 6L));
			TestUtils.checkLocalFs(outDir, 1, 2);

			// we take a checkpoint so we roll.
			testHarness.snapshot(2L, 2L);

			testHarness.processElement(new StreamRecord<>(Tuple2.of("test2", 7), 7L));
			TestUtils.checkLocalFs(outDir, 2, 2);

			for (File file: FileUtils.listFiles(outDir, null, true)) {
				if (Objects.equals(file.getParentFile().getName(), "test1")) {
					Assert.assertThat(
							file.getName(),
							either(containsString(".part-0-2.inprogress."))
									.or(equalTo("part-0-1"))
					);
				} else if (Objects.equals(file.getParentFile().getName(), "test2")) {
					Assert.assertThat(
							file.getName(),
							either(containsString(".part-0-3.inprogress."))
									.or(equalTo("part-0-0"))
					);
				}
			}

			// we acknowledge the last checkpoint so we should publish all but the latest in-progress file
			testHarness.notifyOfCompletedCheckpoint(2L);

			TestUtils.checkLocalFs(outDir, 1, 3);
			for (File file: FileUtils.listFiles(outDir, null, true)) {
				if (Objects.equals(file.getParentFile().getName(), "test1")) {
					Assert.assertThat(
							file.getName(),
							either(equalTo("part-0-2")).or(equalTo("part-0-1"))
					);
				} else if (Objects.equals(file.getParentFile().getName(), "test2")) {
					Assert.assertThat(
							file.getName(),
							either(containsString(".part-0-3.inprogress."))
									.or(equalTo("part-0-0"))
					);
				}
			}
		}
	}

	@Test
	public void testCustomRollingPolicy() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();

		final RollingPolicy<Tuple2<String, Integer>, String> rollingPolicy = new RollingPolicy<Tuple2<String, Integer>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
				return true;
			}

			@Override
			public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, Tuple2<String, Integer> element) throws IOException {
				// this means that 2 elements will close the part file.
				return partFileState.getSize() > 12L;
			}

			@Override
			public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileState, long currentTime) {
				return false;
			}
		};

		try (
				OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness = TestUtils.createCustomRescalingTestSink(
						outDir,
						1,
						0,
						10L,
						new TestUtils.TupleToStringBucketer(),
						new SimpleStringEncoder<>(),
						rollingPolicy,
						new DefaultBucketFactoryImpl<>())
		) {
			testHarness.setup();
			testHarness.open();

			testHarness.setProcessingTime(0L);

			testHarness.processElement(new StreamRecord<>(Tuple2.of("test2", 1), 1L));

			// the following 2 elements will close a part file ...
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 1), 1L));
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 2), 2L));

			// ... and this one will open a new ...
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 3), 2L));
			TestUtils.checkLocalFs(outDir, 3, 0);

			// ... and all open part files should close here.
			testHarness.snapshot(1L, 1L);

			// this will create and fill out a new part file
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 4), 4L));
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 5), 5L));
			TestUtils.checkLocalFs(outDir, 4, 0);

			// we take a checkpoint so we roll.
			testHarness.snapshot(2L, 2L);

			// we acknowledge the first checkpoint so we should publish all but the latest in-progress file
			testHarness.notifyOfCompletedCheckpoint(1L);
			TestUtils.checkLocalFs(outDir, 1, 3);
		}
	}
}
