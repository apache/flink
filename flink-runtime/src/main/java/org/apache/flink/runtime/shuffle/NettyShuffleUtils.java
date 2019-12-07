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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

import java.util.Collection;

/**
 * Configuration object for the network stack.
 */
public class NettyShuffleUtils {

	public static int getNetworkBuffersPerInputChannel(final Configuration configuration) {
		return configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
	}

	public static int getNetworkBufferSize(final Configuration configuration) {
		return ConfigurationParserUtils.getPageSize(configuration);
	}

	public static int getNumberOfRequiredBuffersForResultPartition(final int numberOfSubpartitions) {
		return numberOfSubpartitions + 1;
	}

	public static int computeRequiredNetworkBuffers(
			final int numBuffersPerChannel,
			final Collection<Integer> numChannelsOfGates,
			final Collection<Integer> numSubpartitionsOfResults) {

		// each input channel will retain N exclusive network buffers, N = buffersPerInputChannel
		final int numTotalChannels = numChannelsOfGates.stream().mapToInt(Integer::intValue).sum();
		final int numBuffersForInputs = numBuffersPerChannel * numTotalChannels;

		final int numBuffersForResults = numSubpartitionsOfResults.stream()
			.map(NettyShuffleUtils::getNumberOfRequiredBuffersForResultPartition)
			.mapToInt(Integer::intValue)
			.sum();

		return numBuffersForInputs + numBuffersForResults;
	}

	/**
	 * Private default constructor to avoid being instantiated.
	 */
	private NettyShuffleUtils() {}
}
