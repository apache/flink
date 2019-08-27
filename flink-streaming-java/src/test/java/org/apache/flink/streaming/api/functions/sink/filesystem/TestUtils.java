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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Utilities for the {@link StreamingFileSink} tests.
 */
public class TestUtils {

	static final int MAX_PARALLELISM = 10;

	static OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> createRescalingTestSink(
			File outDir,
			int totalParallelism,
			int taskIdx,
			long inactivityInterval,
			long partMaxSize) throws Exception {

		final RollingPolicy<Tuple2<String, Integer>, String> rollingPolicy =
				DefaultRollingPolicy
						.builder()
						.withMaxPartSize(partMaxSize)
						.withRolloverInterval(inactivityInterval)
						.withInactivityInterval(inactivityInterval)
						.build();

		final BucketAssigner<Tuple2<String, Integer>, String> bucketer = new TupleToStringBucketer();

		final Encoder<Tuple2<String, Integer>> encoder = (element, stream) -> {
			stream.write((element.f0 + '@' + element.f1).getBytes(StandardCharsets.UTF_8));
			stream.write('\n');
		};

		return createCustomRescalingTestSink(
				outDir,
				totalParallelism,
				taskIdx,
				10L,
				bucketer,
				encoder,
				rollingPolicy,
				new DefaultBucketFactoryImpl<>());
	}

	static OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> createCustomRescalingTestSink(
			final File outDir,
			final int totalParallelism,
			final int taskIdx,
			final long bucketCheckInterval,
			final BucketAssigner<Tuple2<String, Integer>, String> bucketer,
			final Encoder<Tuple2<String, Integer>> writer,
			final RollingPolicy<Tuple2<String, Integer>, String> rollingPolicy,
			final BucketFactory<Tuple2<String, Integer>, String> bucketFactory) throws Exception {

		StreamingFileSink<Tuple2<String, Integer>> sink = StreamingFileSink
				.forRowFormat(new Path(outDir.toURI()), writer)
				.withBucketAssigner(bucketer)
				.withRollingPolicy(rollingPolicy)
				.withBucketCheckInterval(bucketCheckInterval)
				.withBucketFactory(bucketFactory)
				.build();

		return new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), MAX_PARALLELISM, totalParallelism, taskIdx);
	}

	static OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> createTestSinkWithBulkEncoder(
			final File outDir,
			final int totalParallelism,
			final int taskIdx,
			final long bucketCheckInterval,
			final BucketAssigner<Tuple2<String, Integer>, String> bucketer,
			final BulkWriter.Factory<Tuple2<String, Integer>> writer,
			final BucketFactory<Tuple2<String, Integer>, String> bucketFactory) throws Exception {

		return createTestSinkWithBulkEncoder(
				outDir,
				totalParallelism,
				taskIdx,
				bucketCheckInterval,
				bucketer,
				writer,
				bucketFactory,
				PartFileConfig.DEFAULT_PART_PREFIX,
				PartFileConfig.DEFAULT_PART_SUFFIX);
	}

	static OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> createTestSinkWithBulkEncoder(
			final File outDir,
			final int totalParallelism,
			final int taskIdx,
			final long bucketCheckInterval,
			final BucketAssigner<Tuple2<String, Integer>, String> bucketer,
			final BulkWriter.Factory<Tuple2<String, Integer>> writer,
			final BucketFactory<Tuple2<String, Integer>, String> bucketFactory,
			final String partFilePrefix,
			final String partFileSuffix) throws Exception {

		StreamingFileSink<Tuple2<String, Integer>> sink = StreamingFileSink
			.forBulkFormat(new Path(outDir.toURI()), writer)
			.withBucketAssigner(bucketer)
			.withBucketCheckInterval(bucketCheckInterval)
			.withBucketFactory(bucketFactory)
			.withPartFilePrefix(partFilePrefix)
			.withPartFileSuffix(partFileSuffix)
			.build();

		return new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), MAX_PARALLELISM, totalParallelism, taskIdx);
	}

	static void checkLocalFs(File outDir, int expectedInProgress, int expectedCompleted) {
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

	static Map<File, String> getFileContentByPath(File directory) throws IOException {
		Map<File, String> contents = new HashMap<>(4);

		final Collection<File> filesInBucket = FileUtils.listFiles(directory, null, true);
		for (File file : filesInBucket) {
			contents.put(file, FileUtils.readFileToString(file));
		}
		return contents;
	}

	static class TupleToStringBucketer implements BucketAssigner<Tuple2<String, Integer>, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public String getBucketId(Tuple2<String, Integer> element, Context context) {
			return element.f0;
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}

	/**
	 * A simple {@link BucketAssigner} that accepts {@code String}'s
	 * and returns the element itself as the bucket id.
	 */
	static class StringIdentityBucketAssigner implements BucketAssigner<String, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getBucketId(String element, BucketAssigner.Context context) {
			return element;
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}

	/**
	 * A mock {@link SinkFunction.Context} to be used in the tests.
	 */
	static class MockSinkContext implements SinkFunction.Context {

		@Nullable
		private Long elementTimestamp;

		private long watermark;

		private long processingTime;

		MockSinkContext(
				@Nullable Long elementTimestamp,
				long watermark,
				long processingTime) {
			this.elementTimestamp = elementTimestamp;
			this.watermark = watermark;
			this.processingTime = processingTime;
		}

		@Override
		public long currentProcessingTime() {
			return processingTime;
		}

		@Override
		public long currentWatermark() {
			return watermark;
		}

		@Nullable
		@Override
		public Long timestamp() {
			return elementTimestamp;
		}
	}

	/**
	 * A mock {@link ListState} used for testing the snapshot/restore cycle of the sink.
	 */
	static class MockListState<T> implements ListState<T> {

		private final List<T> backingList;

		MockListState() {
			this.backingList = new ArrayList<>();
		}

		public List<T> getBackingList() {
			return backingList;
		}

		@Override
		public void update(List<T> values) {
			backingList.clear();
			addAll(values);
		}

		@Override
		public void addAll(List<T> values) {
			backingList.addAll(values);
		}

		@Override
		public Iterable<T> get() {
			return new Iterable<T>() {

				@Nonnull
				@Override
				public Iterator<T> iterator() {
					return backingList.iterator();
				}
			};
		}

		@Override
		public void add(T value) {
			backingList.add(value);
		}

		@Override
		public void clear() {
			backingList.clear();
		}
	}
}
