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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/**
 * A factory returning {@link Bucket buckets}.
 */
@Internal
class DefaultBucketFactoryImpl<IN, BucketID> implements BucketFactory<IN, BucketID> {

	private static final long serialVersionUID = 1L;

	@Override
	public Bucket<IN, BucketID> getNewBucket(
			final int subtaskIndex,
			final BucketID bucketId,
			final Path bucketPath,
			final long initialPartCounter,
			final BucketWriter<IN, BucketID> partFileWriterFactory,
			final RollingPolicy<IN, BucketID> rollingPolicy,
			final OutputFileConfig outputFileConfig) {

		return Bucket.getNew(
				subtaskIndex,
				bucketId,
				bucketPath,
				initialPartCounter,
				partFileWriterFactory,
				rollingPolicy,
				outputFileConfig);
	}

	@Override
	public Bucket<IN, BucketID> restoreBucket(
			final int subtaskIndex,
			final long initialPartCounter,
			final BucketWriter<IN, BucketID> partFileWriterFactory,
			final RollingPolicy<IN, BucketID> rollingPolicy,
			final BucketState<BucketID> bucketState,
			final OutputFileConfig outputFileConfig) throws IOException {

		return Bucket.restore(
				subtaskIndex,
				initialPartCounter,
				partFileWriterFactory,
				rollingPolicy,
				bucketState,
				outputFileConfig);
	}
}
