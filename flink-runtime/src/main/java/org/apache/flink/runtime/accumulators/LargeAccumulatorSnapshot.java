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

package org.apache.flink.runtime.accumulators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * In case some user-defined accumulators do not fit in an Akka message payload, we store them in the
 * blobCache, and put in the snapshot only the mapping between the name of the accumulator,
 * and its blobKey in the cache. This clase is a subclass of the BaseAccumulatorSnapshot
 * and holds the (potential) references to blobs stored in the BlobCache and containing
 * these oversized accumulators. It is used for the transfer from TaskManagers to the
 * JobManager and from the JobManager to the Client.
 */
public class LargeAccumulatorSnapshot extends BaseAccumulatorSnapshot {

	/**
	 * In case some accumulators do not fit in an Akka message payload, we store them in the blobCache and put
	 * in the snapshot only the mapping between the name of the accumulator, and its blobKey
	 * in the cache. This list holds exactly this mapping.
	 * */
	private final Map<String, List<BlobKey>> largeUserAccumulatorBlobs;

	public LargeAccumulatorSnapshot(
			JobID jobID, ExecutionAttemptID executionAttemptID,
			Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> flinkAccumulators,
			Map<String, List<BlobKey>> oversizedUserAccumulatorBlobKeys) throws IOException {
		super(jobID, executionAttemptID, flinkAccumulators);
		this.largeUserAccumulatorBlobs = oversizedUserAccumulatorBlobKeys;
	}

	/**
	 * Gets the BlobKeys of the oversized accumulators that were too big to be sent through akka, and
	 * had to be stored in the BlobCache.
	 * @return the maping between accumulator and its blobKeys.
	 */
	public Map<String, List<BlobKey>> getLargeAccumulatorBlobKeys() {
		return largeUserAccumulatorBlobs;
	}
}
