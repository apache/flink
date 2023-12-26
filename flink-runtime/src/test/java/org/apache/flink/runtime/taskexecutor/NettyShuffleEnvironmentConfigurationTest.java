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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.description.Formatter;
import org.apache.flink.configuration.description.HtmlFormatter;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link NettyShuffleEnvironmentConfiguration}. */
class NettyShuffleEnvironmentConfigurationTest {

    private static final MemorySize MEM_SIZE_PARAM = new MemorySize(128L * 1024 * 1024);

    @Test
    void testNetworkBufferNumberCalculation() {
        final Configuration config = new Configuration();
        config.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("1m"));
        final int numNetworkBuffers =
                NettyShuffleEnvironmentConfiguration.fromConfiguration(
                                config, MEM_SIZE_PARAM, false, InetAddress.getLoopbackAddress())
                        .numNetworkBuffers();
        assertThat(numNetworkBuffers).isEqualTo(128);
    }

    /**
     * Verifies that {@link NettyShuffleEnvironmentConfiguration#fromConfiguration(Configuration,
     * MemorySize, boolean, InetAddress)} returns the correct result for new configurations via
     * {@link NettyShuffleEnvironmentOptions#NETWORK_REQUEST_BACKOFF_INITIAL}, {@link
     * NettyShuffleEnvironmentOptions#NETWORK_REQUEST_BACKOFF_MAX}, {@link
     * NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_PER_CHANNEL} and {@link
     * NettyShuffleEnvironmentOptions#NETWORK_EXTRA_BUFFERS_PER_GATE}
     */
    @Test
    void testNetworkRequestBackoffAndBuffers() {

        // set some non-default values
        final Configuration config = new Configuration();
        config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL, 100);
        config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 200);
        config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL, 10);
        config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, 100);

        final NettyShuffleEnvironmentConfiguration networkConfig =
                NettyShuffleEnvironmentConfiguration.fromConfiguration(
                        config, MEM_SIZE_PARAM, true, InetAddress.getLoopbackAddress());

        assertThat(networkConfig.partitionRequestInitialBackoff()).isEqualTo(100);
        assertThat(networkConfig.partitionRequestMaxBackoff()).isEqualTo(200);
        assertThat(networkConfig.networkBuffersPerChannel()).isEqualTo(10);
        assertThat(networkConfig.floatingNetworkBuffersPerGate()).isEqualTo(100);
    }

    /** Verifies the correlation of sort-merge blocking shuffle config options. */
    @Test
    void testSortMergeShuffleConfigOptionsCorrelation() {
        Formatter formatter = new HtmlFormatter();
        ConfigOption<Integer> configOption =
                NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM;
        String description = formatter.format(configOption.description());

        String configKey =
                getConfigKey(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS);
        assertThat(description).contains(configKey);
        configKey = getConfigKey(TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY);
        assertThat(description).contains(configKey);

        assertThat(NettyShuffleEnvironmentOptions.BATCH_SHUFFLE_COMPRESSION_ENABLED.defaultValue())
                .isTrue();
    }

    private static String getConfigKey(ConfigOption<?> configOption) {
        return "'" + configOption.key() + "'";
    }
}
