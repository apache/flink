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

package org.apache.flink.table.filesystem.stream;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.filesystem.stream.StreamingFileCommitter.CommitMessage;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test for {@link StreamingFileWriter}.
 */
public class StreamingFileWriterTest {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private Path path;

	@Before
	public void before() throws IOException {
		File file = TEMPORARY_FOLDER.newFile();
		file.delete();
		path = new Path(file.toURI());
	}

	@Test
	public void testFailover() throws Exception {
		OperatorSubtaskState state;
		try (OneInputStreamOperatorTestHarness<RowData, CommitMessage> harness = create()) {
			harness.setup();
			harness.initializeEmptyState();
			harness.open();
			harness.processElement(row("1"), 0);
			harness.processElement(row("2"), 0);
			harness.processElement(row("2"), 0);
			state = harness.snapshot(1, 1);
			harness.processElement(row("3"), 0);
			harness.processElement(row("4"), 0);
			harness.notifyOfCompletedCheckpoint(1);
			List<String> partitions = collect(harness);
			Assert.assertEquals(Arrays.asList("1", "2"), partitions);
		}

		// first retry, no partition {1, 2} records
		try (OneInputStreamOperatorTestHarness<RowData, CommitMessage> harness = create()) {
			harness.setup();
			harness.initializeState(state);
			harness.open();
			harness.processElement(row("3"), 0);
			harness.processElement(row("4"), 0);
			state = harness.snapshot(2, 2);
			harness.notifyOfCompletedCheckpoint(2);
			List<String> partitions = collect(harness);
			Assert.assertEquals(Arrays.asList("1", "2", "3", "4"), partitions);
		}

		// second retry, partition {4} repeat
		try (OneInputStreamOperatorTestHarness<RowData, CommitMessage> harness = create()) {
			harness.setup();
			harness.initializeState(state);
			harness.open();
			harness.processElement(row("4"), 0);
			harness.processElement(row("5"), 0);
			state = harness.snapshot(3, 3);
			harness.notifyOfCompletedCheckpoint(3);
			List<String> partitions = collect(harness);
			Assert.assertEquals(Arrays.asList("3", "4", "5"), partitions);
		}

		// third retry, multiple snapshots
		try (OneInputStreamOperatorTestHarness<RowData, CommitMessage> harness = create()) {
			harness.setup();
			harness.initializeState(state);
			harness.open();
			harness.processElement(row("6"), 0);
			harness.processElement(row("7"), 0);
			harness.snapshot(4, 4);
			harness.processElement(row("8"), 0);
			harness.snapshot(5, 5);
			harness.processElement(row("9"), 0);
			harness.snapshot(6, 6);
			harness.notifyOfCompletedCheckpoint(5);
			List<String> partitions = collect(harness);
			// should not contains partition {9}
			Assert.assertEquals(Arrays.asList("4", "5", "6", "7", "8"), partitions);
		}
	}

	private static RowData row(String s) {
		return GenericRowData.of(StringData.fromString(s));
	}

	private static List<String> collect(
			OneInputStreamOperatorTestHarness<RowData, CommitMessage> harness) {
		List<String> parts = new ArrayList<>();
		harness.extractOutputValues().forEach(m -> parts.addAll(m.partitions));
		return parts;
	}

	private OneInputStreamOperatorTestHarness<RowData, CommitMessage> create() throws Exception {
		StreamingFileWriter writer = new StreamingFileWriter(1000, StreamingFileSink.forRowFormat(
				path, (Encoder<RowData>) (element, stream) ->
						stream.write((element.getString(0) + "\n").getBytes(StandardCharsets.UTF_8)))
				.withBucketAssigner(new BucketAssigner<RowData, String>() {
					@Override
					public String getBucketId(RowData element, Context context) {
						return element.getString(0).toString();
					}

					@Override
					public SimpleVersionedSerializer<String> getSerializer() {
						return SimpleVersionedStringSerializer.INSTANCE;
					}
				})
				.withRollingPolicy(OnCheckpointRollingPolicy.build()));
		OneInputStreamOperatorTestHarness<RowData, CommitMessage> harness = new OneInputStreamOperatorTestHarness<>(
				writer, 1, 1, 0);
		harness.getStreamConfig().setTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		return harness;
	}
}
