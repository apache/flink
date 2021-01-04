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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/** A factory able to create {@link Bucket buckets} for the {@link StreamingFileSink}. */
@Internal
public interface BucketFactory<IN, BucketID> extends Serializable {

    Bucket<IN, BucketID> getNewBucket(
            final int subtaskIndex,
            final BucketID bucketId,
            final Path bucketPath,
            final long initialPartCounter,
            final BucketWriter<IN, BucketID> bucketWriter,
            final RollingPolicy<IN, BucketID> rollingPolicy,
            @Nullable final FileLifeCycleListener<BucketID> fileListener,
            final OutputFileConfig outputFileConfig)
            throws IOException;

    Bucket<IN, BucketID> restoreBucket(
            final int subtaskIndex,
            final long initialPartCounter,
            final BucketWriter<IN, BucketID> bucketWriter,
            final RollingPolicy<IN, BucketID> rollingPolicy,
            final BucketState<BucketID> bucketState,
            @Nullable final FileLifeCycleListener<BucketID> fileListener,
            final OutputFileConfig outputFileConfig)
            throws IOException;
}
