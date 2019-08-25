/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Integration tests for {@link BucketAssigner bucket assigners}.
 */
public class BucketAssignerITCases {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@Test
	public void testAssembleBucketPath() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path basePath = new Path(outDir.toURI());
		final long time = 1000L;

		final RollingPolicy<String, String> rollingPolicy =
			DefaultRollingPolicy
				.create()
				.withMaxPartSize(7L)
				.build();

		final Buckets<String, String> buckets =  new Buckets<>(
			basePath,
			new BasePathBucketAssigner<>(),
			new DefaultBucketFactoryImpl<>(),
			new RowWisePartWriter.Factory<>(new SimpleStringEncoder<>()),
			rollingPolicy,
			0,
			new PartFileConfig()
		);

		Bucket<String, String> bucket =
			buckets.onElement("abc", new TestUtils.MockSinkContext(time, time, time));
		Assert.assertEquals(new Path(basePath.toUri()), bucket.getBucketPath());
	}
}
