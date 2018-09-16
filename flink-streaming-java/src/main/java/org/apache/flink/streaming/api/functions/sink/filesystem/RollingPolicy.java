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

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;
import java.io.Serializable;

/**
 * The policy based on which a {@link Bucket} in the {@link StreamingFileSink}
 * rolls its currently open part file and opens a new one.
 */
@PublicEvolving
public interface RollingPolicy<IN, BucketID> extends Serializable {

	/**
	 * Determines if the in-progress part file for a bucket should roll on every checkpoint.
	 * @param partFileState the state of the currently open part file of the bucket.
	 * @return {@code True} if the part file should roll, {@link false} otherwise.
	 */
	boolean shouldRollOnCheckpoint(final PartFileInfo<BucketID> partFileState) throws IOException;

	/**
	 * Determines if the in-progress part file for a bucket should roll based on its current state, e.g. its size.
	 * @param element the element being processed.
	 * @param partFileState the state of the currently open part file of the bucket.
	 * @return {@code True} if the part file should roll, {@link false} otherwise.
	 */
	boolean shouldRollOnEvent(final PartFileInfo<BucketID> partFileState, IN element) throws IOException;

	/**
	 * Determines if the in-progress part file for a bucket should roll based on a time condition.
	 * @param partFileState the state of the currently open part file of the bucket.
	 * @param currentTime the current processing time.
	 * @return {@code True} if the part file should roll, {@link false} otherwise.
	 */
	boolean shouldRollOnProcessingTime(final PartFileInfo<BucketID> partFileState, final long currentTime) throws IOException;
}
