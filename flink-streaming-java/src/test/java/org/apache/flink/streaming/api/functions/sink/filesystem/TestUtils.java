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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rolling.policies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for the {@link StreamingFileSink} tests.
 */
public class TestUtils {

	static OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> createRescalingTestSink(
			File outDir,
			int totalParallelism,
			int taskIdx,
			long inactivityInterval,
			long partMaxSize) throws Exception {

		final RollingPolicy<Tuple2<String, Integer>, String> rollingPolicy =
				DefaultRollingPolicy
						.create()
						.withMaxPartSize(partMaxSize)
						.withRolloverInterval(inactivityInterval)
						.withInactivityInterval(inactivityInterval)
						.build();

		final Bucketer<Tuple2<String, Integer>, String> bucketer = new TupleToStringBucketer();

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
				new DefaultBucketFactory<>());
	}

	static OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> createCustomRescalingTestSink(
			final File outDir,
			final int totalParallelism,
			final int taskIdx,
			final long bucketCheckInterval,
			final Bucketer<Tuple2<String, Integer>, String> bucketer,
			final Encoder<Tuple2<String, Integer>> writer,
			final RollingPolicy<Tuple2<String, Integer>, String> rollingPolicy,
			final BucketFactory<Tuple2<String, Integer>, String> bucketFactory) throws Exception {

		StreamingFileSink<Tuple2<String, Integer>> sink = StreamingFileSink
				.forRowFormat(new Path(outDir.toURI()), writer)
				.withBucketer(bucketer)
				.withRollingPolicy(rollingPolicy)
				.withBucketCheckInterval(bucketCheckInterval)
				.withBucketFactory(bucketFactory)
				.build();

		return new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), 10, totalParallelism, taskIdx);
	}

	static OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> createTestSinkWithBulkEncoder(
			final File outDir,
			final int totalParallelism,
			final int taskIdx,
			final long bucketCheckInterval,
			final Bucketer<Tuple2<String, Integer>, String> bucketer,
			final BulkWriter.Factory<Tuple2<String, Integer>> writer,
			final BucketFactory<Tuple2<String, Integer>, String> bucketFactory) throws Exception {

		StreamingFileSink<Tuple2<String, Integer>> sink = StreamingFileSink
				.forBulkFormat(new Path(outDir.toURI()), writer)
				.withBucketer(bucketer)
				.withBucketCheckInterval(bucketCheckInterval)
				.withBucketFactory(bucketFactory)
				.build();

		return new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), 10, totalParallelism, taskIdx);
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

	static class TupleToStringBucketer implements Bucketer<Tuple2<String, Integer>, String> {

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
}
