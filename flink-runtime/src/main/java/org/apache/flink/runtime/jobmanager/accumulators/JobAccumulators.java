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

package org.apache.flink.runtime.jobmanager.accumulators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.runtime.blob.BlobKey;

/**
 * Simple class wrapping a map of accumulators for a single job. Just for better
 * handling.
 */
public class JobAccumulators {

	private final Map<String, Accumulator<?, ?>> accumulators = new HashMap<String, Accumulator<?, ?>>();

	private final Map<String, List<BlobKey>> largeAccumulators = new HashMap<String, List<BlobKey>>();

	public Map<String, Accumulator<?, ?>> getAccumulators() {
		return this.accumulators;
	}

	public Map<String, List<BlobKey>> getLargeAccumulatorRefs() {
		return this.largeAccumulators;
	}

	public void processNew(Map<String, Accumulator<?, ?>> newAccumulators) {
		AccumulatorHelper.mergeInto(this.accumulators, newAccumulators);
	}

	public void processRefs(Map<String, List<BlobKey>> accumulatorBlobRefs) {
		for (Map.Entry<String, List<BlobKey>> otherEntry : accumulatorBlobRefs.entrySet()) {
			List<BlobKey> ownAccumulator = largeAccumulators.get(otherEntry.getKey());
			if (ownAccumulator == null) {
				largeAccumulators.put(otherEntry.getKey(), otherEntry.getValue());
			} else {
				ownAccumulator.addAll(otherEntry.getValue());
			}
		}
	}
}
