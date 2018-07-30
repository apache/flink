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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Tests for {@link Buckets}.
 */
public class BucketsTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@Test
	public void testContextPassingNormalExecution() throws Exception {
		testCorrectPassingOfContext(1L, 2L, 3L);
	}

	@Test
	public void testContextPassingNullTimestamp() throws Exception {
		testCorrectPassingOfContext(null, 2L, 3L);
	}

	private void testCorrectPassingOfContext(Long timestamp, long watermark, long processingTime) throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();

		final Long expectedTimestamp = timestamp;
		final long expectedWatermark = watermark;
		final long expectedProcessingTime = processingTime;

		final Buckets<String, String> buckets = StreamingFileSink
				.<String>forRowFormat(new Path(outDir.toURI()), new SimpleStringEncoder<>())
				.withBucketAssigner(new VarifyingBucketer(expectedTimestamp, expectedWatermark, expectedProcessingTime))
				.createBuckets(2);

		buckets.onElement("TEST", new SinkFunction.Context() {
			@Override
			public long currentProcessingTime() {
				return expectedProcessingTime;
			}

			@Override
			public long currentWatermark() {
				return expectedWatermark;
			}

			@Override
			public Long timestamp() {
				return expectedTimestamp;
			}
		});
	}

	private static class VarifyingBucketer implements BucketAssigner<String, String> {

		private static final long serialVersionUID = 7729086510972377578L;

		private final Long expectedTimestamp;
		private final long expectedWatermark;
		private final long expectedProcessingTime;

		VarifyingBucketer(
				final Long expectedTimestamp,
				final long expectedWatermark,
				final long expectedProcessingTime
		) {
			this.expectedTimestamp = expectedTimestamp;
			this.expectedWatermark = expectedWatermark;
			this.expectedProcessingTime = expectedProcessingTime;
		}

		@Override
		public String getBucketId(String element, Context context) {
			final Long elementTimestamp = context.timestamp();
			final long watermark = context.currentWatermark();
			final long processingTime = context.currentProcessingTime();

			Assert.assertEquals(expectedTimestamp, elementTimestamp);
			Assert.assertEquals(expectedProcessingTime, processingTime);
			Assert.assertEquals(expectedWatermark, watermark);

			return element;
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}
}
