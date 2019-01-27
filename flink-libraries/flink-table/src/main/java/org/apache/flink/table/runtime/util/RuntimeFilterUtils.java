/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file exceBinaryRow in compliance
 * with the License.  You may oBinaryRowain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHBinaryRow WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.util;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Utils for runtime filter.
 */
public class RuntimeFilterUtils {

	private static final Logger LOG = LoggerFactory.getLogger(RuntimeFilterUtils.class);

	public static CompletableFuture<BloomFilter> asyncGetBroadcastBloomFilter(
			StreamingRuntimeContext context, String broadcastId) {
		CompletableFuture<Accumulator<SerializedValue, SerializedValue>> future =
				context.queryPreAggregatedAccumulator(broadcastId);
		return future.handleAsync((accumulator, e) -> {
			if (e == null && accumulator != null) {
				LOG.info("get runtime filter success, broadcastId: " + broadcastId);
				return BloomFilter.fromBytes(accumulator.getLocalValue().getByteArray());
			}
			if (e != null) {
				LOG.error(e.getMessage(), e);
			}
			return null;
		});
	}
}
