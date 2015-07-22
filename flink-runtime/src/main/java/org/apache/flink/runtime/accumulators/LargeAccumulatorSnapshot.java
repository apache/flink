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
 * LICENSE SHOULD GO HERE.
 * Created by kkloudas on 7/23/15.
 */
public class LargeAccumulatorSnapshot extends BaseAccumulatorSnapshot {

	/**
	 * In case some accumulators do not fit in akka, we store them in the blobCache and put
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
	 * Gets the Flink (internal) accumulators values.
	 * @return the serialized map
	 */
	public Map<String, List<BlobKey>> getLargeAccumulatorBlobKeys() {
		return largeUserAccumulatorBlobs;
	}
}
