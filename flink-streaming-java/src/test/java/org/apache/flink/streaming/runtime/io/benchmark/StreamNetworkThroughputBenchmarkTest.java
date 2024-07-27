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

package org.apache.flink.streaming.runtime.io.benchmark;

import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for various network benchmarks based on {@link StreamNetworkThroughputBenchmark}. */
class StreamNetworkThroughputBenchmarkTest {

    protected StreamNetworkThroughputBenchmark createBenchmark() {
        return new StreamNetworkThroughputBenchmark();
    }

    @Test
    void pointToPointBenchmark() throws Exception {
        StreamNetworkThroughputBenchmark benchmark = createBenchmark();
        benchmark.setUp(1, 1, 100);
        try {
            benchmark.executeBenchmark(1_000);
        } finally {
            benchmark.tearDown();
        }
    }

    @Test
    void largeLocalMode() throws Exception {
        StreamNetworkThroughputBenchmark env = new StreamNetworkThroughputBenchmark();
        env.setUp(4, 10, 100, true);
        env.executeBenchmark(10_000_000);
        env.tearDown();
    }

    @Test
    void largeRemoteMode() throws Exception {
        StreamNetworkThroughputBenchmark env = new StreamNetworkThroughputBenchmark();
        env.setUp(4, 10, 100, false);
        env.executeBenchmark(10_000_000);
        env.tearDown();
    }

    @Test
    void largeRemoteAlwaysFlush() throws Exception {
        StreamNetworkThroughputBenchmark env = new StreamNetworkThroughputBenchmark();
        env.setUp(1, 1, 0, false);
        env.executeBenchmark(1_000_000);
        env.tearDown();
    }

    @Test
    void remoteModeInsufficientBuffersSender() {
        StreamNetworkThroughputBenchmark env = new StreamNetworkThroughputBenchmark();
        int writers = 2;
        int channels = 2;

        assertThatThrownBy(
                        () ->
                                env.setUp(
                                        writers,
                                        channels,
                                        100,
                                        false,
                                        writers * channels - 1,
                                        writers
                                                * channels
                                                * NettyShuffleEnvironmentOptions
                                                        .NETWORK_BUFFERS_PER_CHANNEL
                                                        .defaultValue()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Insufficient number of network buffers");
    }

    @Test
    void remoteModeInsufficientBuffersReceiver() throws Exception {
        StreamNetworkThroughputBenchmark env = new StreamNetworkThroughputBenchmark();
        int writers = 2;
        int channels = 2;

        assertThatThrownBy(
                        () ->
                                env.setUp(
                                        writers,
                                        channels,
                                        100,
                                        false,
                                        writers * channels,
                                        writers
                                                        * channels
                                                        * NettyShuffleEnvironmentOptions
                                                                .NETWORK_BUFFERS_PER_CHANNEL
                                                                .defaultValue()
                                                - 1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Insufficient number of network buffers");
    }

    @Test
    void remoteModeMinimumBuffers() throws Exception {
        StreamNetworkThroughputBenchmark env = new StreamNetworkThroughputBenchmark();
        int writers = 2;
        int channels = 2;

        env.setUp(
                writers,
                channels,
                100,
                false,
                writers * channels + writers,
                writers
                        + writers
                                * channels
                                * NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL
                                        .defaultValue());
        env.executeBenchmark(10_000);
        env.tearDown();
    }

    @Test
    void pointToMultiPointBenchmark() throws Exception {
        StreamNetworkThroughputBenchmark benchmark = createBenchmark();
        benchmark.setUp(1, 100, 100);
        try {
            benchmark.executeBenchmark(1_000);
        } finally {
            benchmark.tearDown();
        }
    }

    @Test
    void multiPointToPointBenchmark() throws Exception {
        StreamNetworkThroughputBenchmark benchmark = createBenchmark();
        benchmark.setUp(4, 1, 100);
        try {
            benchmark.executeBenchmark(1_000);
        } finally {
            benchmark.tearDown();
        }
    }

    @Test
    void multiPointToMultiPointBenchmark() throws Exception {
        StreamNetworkThroughputBenchmark benchmark = createBenchmark();
        benchmark.setUp(4, 100, 100);
        try {
            benchmark.executeBenchmark(1_000);
        } finally {
            benchmark.tearDown();
        }
    }
}
