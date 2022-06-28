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

package org.apache.flink.runtime.io.network.partition.hybrid;

import java.time.Duration;

/** Configuration for hybrid shuffle mode. */
public class HybridShuffleConfiguration {
    private static final int MAX_BUFFERS_READ_AHEAD = 5;
    private static final Duration DEFAULT_BUFFER_REQUEST_TIMEOUT = Duration.ofMillis(5);

    private final int maxBuffersReadAhead;

    private final Duration bufferRequestTimeout;

    private final int maxRequestedBuffers;

    public HybridShuffleConfiguration(
            int maxBuffersReadAhead, Duration bufferRequestTimeout, int maxRequestedBuffers) {
        this.maxBuffersReadAhead = maxBuffersReadAhead;
        this.bufferRequestTimeout = bufferRequestTimeout;
        this.maxRequestedBuffers = maxRequestedBuffers;
    }

    /**
     * Temporarily adopt a fixed value, and then adjust the default value according to the
     * experiment, and introduce configuration options.
     */
    public static HybridShuffleConfiguration createConfiguration(
            int numSubpartitions, int numBuffersPerRequest, Duration bufferRequestTimeout) {
        final int maxRequestedBuffers = Math.max(2 * numBuffersPerRequest, numSubpartitions);
        return new HybridShuffleConfiguration(
                MAX_BUFFERS_READ_AHEAD, bufferRequestTimeout, maxRequestedBuffers);
    }

    public static HybridShuffleConfiguration createConfiguration(
            int numSubpartitions, int numBuffersPerRequest) {
        return createConfiguration(
                numSubpartitions, numBuffersPerRequest, DEFAULT_BUFFER_REQUEST_TIMEOUT);
    }

    public int getMaxRequestedBuffers() {
        return maxRequestedBuffers;
    }

    /**
     * Determine how many buffers to read ahead at most for each subpartition to prevent other
     * consumers from starving.
     */
    public int getMaxBuffersReadAhead() {
        return maxBuffersReadAhead;
    }

    /**
     * Maximum time to wait when requesting read buffers from the buffer pool before throwing an
     * exception.
     */
    public Duration getBufferRequestTimeout() {
        return bufferRequestTimeout;
    }
}
