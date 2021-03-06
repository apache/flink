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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.InetAddress;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Unit test for {@link NettyShuffleEnvironmentConfiguration}. */
public class NettyShuffleEnvironmentConfigurationTest extends TestLogger {

    private static final MemorySize MEM_SIZE_PARAM = new MemorySize(128L * 1024 * 1024);

    @Test
    public void testNetworkBufferNumberCalculation() {
        final Configuration config = new Configuration();
        config.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("1m"));
        final int numNetworkBuffers =
                NettyShuffleEnvironmentConfiguration.fromConfiguration(
                                config, MEM_SIZE_PARAM, false, InetAddress.getLoopbackAddress())
                        .numNetworkBuffers();
        assertThat(numNetworkBuffers, is(128));
    }

    /**
     * Verifies that {@link NettyShuffleEnvironmentConfiguration#fromConfiguration(Configuration,
     * MemorySize, boolean, InetAddress)} returns the correct result for new configurations via
     * {@link NettyShuffleEnvironmentOptions#NETWORK_REQUEST_BACKOFF_INITIAL}, {@link
     * NettyShuffleEnvironmentOptions#NETWORK_REQUEST_BACKOFF_MAX}, {@link
     * NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_PER_CHANNEL}, {@link
     * NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_PER_INCOMING_CHANNEL}, {@link
     * NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_PER_OUTGOING_CHANNEL} and {@link
     * NettyShuffleEnvironmentOptions#NETWORK_EXTRA_BUFFERS_PER_GATE}
     */
    @Test
    public void testNetworkRequestBackoffAndBuffers() {
        int buffersPerChannel = 10;
        for (int perSubpartition = -100; perSubpartition <= 100; perSubpartition += 10) {
            for (int perInputChannel = -100; perInputChannel <= 100; perInputChannel += 10) {
                int expectedBuffersPerSubpartition =
                        perSubpartition < 0 ? buffersPerChannel : perSubpartition;
                int expectedBuffersPerInputChannel =
                        perInputChannel < 0 ? buffersPerChannel : perInputChannel;

                // set some non-default values
                Configuration config = new Configuration();
                config.setInteger(
                        NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL, 100);
                config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 200);
                config.setInteger(
                        NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL,
                        buffersPerChannel);
                config.setInteger(
                        NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_OUTGOING_CHANNEL,
                        perSubpartition);
                config.setInteger(
                        NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_INCOMING_CHANNEL,
                        perInputChannel);
                config.setInteger(
                        NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, 100);

                NettyShuffleEnvironmentConfiguration networkConfig =
                        NettyShuffleEnvironmentConfiguration.fromConfiguration(
                                config, MEM_SIZE_PARAM, true, InetAddress.getLoopbackAddress());

                assertEquals(networkConfig.partitionRequestInitialBackoff(), 100);
                assertEquals(networkConfig.partitionRequestMaxBackoff(), 200);
                assertEquals(
                        networkConfig.networkBuffersPerSubpartition(),
                        expectedBuffersPerSubpartition);
                assertEquals(
                        networkConfig.networkBuffersPerInputChannel(),
                        expectedBuffersPerInputChannel);
                assertEquals(networkConfig.floatingNetworkBuffersPerGate(), 100);
            }
        }
    }
}
