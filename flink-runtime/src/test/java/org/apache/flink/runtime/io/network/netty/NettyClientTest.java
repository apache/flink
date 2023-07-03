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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.util.NetUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.Epoll;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollChannelOption;

import jdk.net.ExtendedSocketOptions;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests for {@link NettyClient}. */
public class NettyClientTest {
    @Test
    void testSetKeepaliveOptionWithNioConfigurable() throws Exception {
        assumeThat(keepaliveForNioConfigurable()).isTrue();

        final Configuration config = new Configuration();
        config.set(NettyShuffleEnvironmentOptions.TRANSPORT_TYPE, "nio");
        config.set(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_IDLE_SECONDS, 300);
        config.set(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_INTERVAL_SECONDS, 10);
        config.set(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_COUNT, 8);

        try (NetUtils.Port clientPort = NetUtils.getAvailablePort()) {
            final NettyClient client = createNettyClient(config, clientPort);
            Map<String, Object> options =
                    client.getBootstrap().config().options().entrySet().stream()
                            .collect(Collectors.toMap(e -> e.getKey().name(), Map.Entry::getValue));
            assertThat(options)
                    .containsEntry(NettyClient.NIO_TCP_KEEPIDLE_KEY, 300)
                    .containsEntry(NettyClient.NIO_TCP_KEEPINTERVAL_KEY, 10)
                    .containsEntry(NettyClient.NIO_TCP_KEEPCOUNT_KEY, 8);
        }
    }

    /**
     * Test that keepalive options will not take effect when using netty transport type of nio with
     * an older version of JDK 8.
     */
    @Test
    void testSetKeepaliveOptionWithNioNotConfigurable() throws Exception {
        assumeThat(keepaliveForNioConfigurable()).isFalse();

        final Configuration config = new Configuration();
        config.set(NettyShuffleEnvironmentOptions.TRANSPORT_TYPE, "nio");
        config.set(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_IDLE_SECONDS, 300);
        config.set(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_INTERVAL_SECONDS, 10);
        config.set(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_COUNT, 8);

        try (NetUtils.Port clientPort = NetUtils.getAvailablePort()) {
            final NettyClient client = createNettyClient(config, clientPort);
            Map<String, Object> options =
                    client.getBootstrap().config().options().entrySet().stream()
                            .collect(Collectors.toMap(e -> e.getKey().name(), Map.Entry::getValue));
            assertThat(options)
                    .doesNotContainKeys(
                            NettyClient.NIO_TCP_KEEPIDLE_KEY,
                            NettyClient.NIO_TCP_KEEPINTERVAL_KEY,
                            NettyClient.NIO_TCP_KEEPCOUNT_KEY);
        }
    }

    @Test
    void testSetKeepaliveOptionWithEpoll() throws Exception {
        assumeThat(Epoll.isAvailable()).isTrue();

        final Configuration config = new Configuration();
        config.set(NettyShuffleEnvironmentOptions.TRANSPORT_TYPE, "epoll");
        config.set(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_IDLE_SECONDS, 300);
        config.set(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_INTERVAL_SECONDS, 10);
        config.set(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_COUNT, 8);

        try (NetUtils.Port clientPort = NetUtils.getAvailablePort()) {
            final NettyClient client = createNettyClient(config, clientPort);
            Map<ChannelOption<?>, Object> options = client.getBootstrap().config().options();
            assertThat(options)
                    .containsEntry(EpollChannelOption.TCP_KEEPIDLE, 300)
                    .containsEntry(EpollChannelOption.TCP_KEEPINTVL, 10)
                    .containsEntry(EpollChannelOption.TCP_KEEPCNT, 8);
        }
    }

    private static boolean keepaliveForNioConfigurable() {
        try {
            ExtendedSocketOptions.class.getField(NettyClient.NIO_TCP_KEEPIDLE_KEY);
        } catch (NoSuchFieldException e) {
            return false;
        }
        return true;
    }

    private static NettyClient createNettyClient(Configuration config, NetUtils.Port port)
            throws Exception {

        final NettyConfig nettyClientConfig =
                new NettyConfig(
                        InetAddress.getLoopbackAddress(),
                        port.getPort(),
                        NettyTestUtil.DEFAULT_SEGMENT_SIZE,
                        1,
                        config);

        final NettyBufferPool bufferPool = new NettyBufferPool(1);
        final NettyProtocol protocol = new NettyProtocol(null, null);

        return NettyTestUtil.initClient(nettyClientConfig, protocol, bufferPool);
    }
}
