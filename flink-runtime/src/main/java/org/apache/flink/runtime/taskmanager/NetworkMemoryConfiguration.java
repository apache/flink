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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Configurations for {@link org.apache.flink.runtime.io.network.buffer.NetworkBufferPool}. */
public class NetworkMemoryConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkMemoryConfiguration.class);

    private final int numNetworkBuffers;

    private final int networkBufferSize;

    private final Duration requestSegmentsTimeout;

    public NetworkMemoryConfiguration(
            int numNetworkBuffers, int networkBufferSize, Duration requestSegmentsTimeout) {
        this.numNetworkBuffers = numNetworkBuffers;
        this.networkBufferSize = networkBufferSize;
        this.requestSegmentsTimeout = checkNotNull(requestSegmentsTimeout);
    }

    public int getNetworkBufferSize() {
        return networkBufferSize;
    }

    public int getNumNetworkBuffers() {
        return numNetworkBuffers;
    }

    public Duration getRequestSegmentsTimeout() {
        return requestSegmentsTimeout;
    }

    public static NetworkMemoryConfiguration fromConfiguration(
            Configuration configuration, MemorySize networkMemorySize) {
        final Duration requestSegmentsTimeout =
                Duration.ofMillis(
                        configuration.getLong(
                                NettyShuffleEnvironmentOptions
                                        .NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS));
        final int pageSize = ConfigurationParserUtils.getPageSize(configuration);
        final int numberOfNetworkBuffers =
                calculateNumberOfNetworkBuffers(configuration, networkMemorySize, pageSize);
        return new NetworkMemoryConfiguration(
                numberOfNetworkBuffers, pageSize, requestSegmentsTimeout);
    }

    /**
     * Calculates the number of network buffers based on configuration and jvm heap size.
     *
     * @param configuration configuration object
     * @param networkMemorySize the size of memory reserved for shuffle environment
     * @param pageSize size of memory segment
     * @return the number of network buffers
     */
    private static int calculateNumberOfNetworkBuffers(
            Configuration configuration, MemorySize networkMemorySize, int pageSize) {

        logIfIgnoringOldConfigs(configuration);

        // tolerate offcuts between intended and allocated memory due to segmentation (will be
        // available to the user-space memory)
        long numberOfNetworkBuffersLong = networkMemorySize.getBytes() / pageSize;
        if (numberOfNetworkBuffersLong > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "The given number of memory bytes ("
                            + networkMemorySize.getBytes()
                            + ") corresponds to more than MAX_INT pages.");
        }

        return (int) numberOfNetworkBuffersLong;
    }

    @SuppressWarnings("deprecation")
    private static void logIfIgnoringOldConfigs(Configuration configuration) {
        if (configuration.contains(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS)) {
            LOG.info(
                    "Ignoring old (but still present) network buffer configuration via {}.",
                    NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS.key());
        }
    }
}
