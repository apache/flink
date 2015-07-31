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

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.util.SerializedValue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class UserAccumulators implements java.io.Serializable {

	/** Serialized user accumulators which may require the custom user class loader. */
	private final SerializedValue<Map<String, Accumulator<?, ?>>> smallUserAccumulators;

	/**
	 * In case some accumulators do not fit in an Akka message payload, we store them in the blobCache and put
	 * in the snapshot only the mapping between the name of the accumulator, and its blobKey
	 * in the cache. This list holds exactly this mapping.
	 * */
	private final Map<String, List<BlobKey>> largeUserAccumulatorBlobs;

	public UserAccumulators(Map<String, List<BlobKey>> oversizedUserAccumulatorBlobKeys) throws IOException {
		this.smallUserAccumulators = null;
		this.largeUserAccumulatorBlobs = oversizedUserAccumulatorBlobKeys;
	}


	public UserAccumulators(SerializedValue<Map<String, Accumulator<?, ?>>> userAccumulators) throws IOException {
		this.smallUserAccumulators = userAccumulators;
		this.largeUserAccumulatorBlobs = null;
	}

	/**
	 * Gets the user-defined accumulators values that fit in akka payload.
	 * @return the serialized map
	 */
	public Map<String, Accumulator<?, ?>> deserializeSmallUserAccumulators(ClassLoader classLoader) throws IOException, ClassNotFoundException {
		if(largeUserAccumulatorBlobs != null) {
			return Collections.emptyMap();
		}
		return smallUserAccumulators.deserializeValue(classLoader);
	}

	/**
	 * Gets the BlobKeys of the oversized accumulators that were too big to be sent through akka.
	 * These accumulators had to be stored in the BlobCache and their blobKeys are returned here.
	 * @return the maping between accumulator and its blobKeys.
	 */
	public Map<String, List<BlobKey>> getLargeAccumulatorBlobKeys() {
		if(smallUserAccumulators != null) {
			return Collections.emptyMap();
		}
		return largeUserAccumulatorBlobs;
	}
}
