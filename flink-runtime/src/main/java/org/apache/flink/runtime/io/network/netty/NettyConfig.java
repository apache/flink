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
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.util.PortRange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class NettyConfig {

    private static final Logger LOG = LoggerFactory.getLogger(NettyConfig.class);

    enum TransportType {
        NIO,
        EPOLL,
        AUTO
    }

    static final String SERVER_THREAD_GROUP_NAME = "Flink Netty Server";

    static final String CLIENT_THREAD_GROUP_NAME = "Flink Netty Client";

    private final InetAddress serverAddress;

    private final PortRange serverPortRange;

    private final int memorySegmentSize;

    private final int numberOfSlots;

    private final Configuration config; // optional configuration

    public NettyConfig(
            InetAddress serverAddress,
            int serverPort,
            int memorySegmentSize,
            int numberOfSlots,
            Configuration config) {
        this(serverAddress, new PortRange(serverPort), memorySegmentSize, numberOfSlots, config);
    }

    public NettyConfig(
            InetAddress serverAddress,
            PortRange serverPortRange,
            int memorySegmentSize,
            int numberOfSlots,
            Configuration config) {

        this.serverAddress = checkNotNull(serverAddress);
        this.serverPortRange = serverPortRange;

        checkArgument(memorySegmentSize > 0, "Invalid memory segment size.");
        this.memorySegmentSize = memorySegmentSize;

        checkArgument(numberOfSlots > 0, "Number of slots");
        this.numberOfSlots = numberOfSlots;

        this.config = checkNotNull(config);

        LOG.info(this.toString());
    }

    InetAddress getServerAddress() {
        return serverAddress;
    }

    PortRange getServerPortRange() {
        return serverPortRange;
    }

    // ------------------------------------------------------------------------
    // Getters
    // ------------------------------------------------------------------------

    public int getServerConnectBacklog() {
        return config.getInteger(NettyShuffleEnvironmentOptions.CONNECT_BACKLOG);
    }

    public int getNumberOfArenas() {
        // default: number of slots
        final int configValue = config.getInteger(NettyShuffleEnvironmentOptions.NUM_ARENAS);
        return configValue == -1 ? numberOfSlots : configValue;
    }

    public int getServerNumThreads() {
        // default: number of task slots
        final int configValue =
                config.getInteger(NettyShuffleEnvironmentOptions.NUM_THREADS_SERVER);
        return configValue == -1 ? numberOfSlots : configValue;
    }

    public int getClientNumThreads() {
        // default: number of task slots
        final int configValue =
                config.getInteger(NettyShuffleEnvironmentOptions.NUM_THREADS_CLIENT);
        return configValue == -1 ? numberOfSlots : configValue;
    }

    public int getClientConnectTimeoutSeconds() {
        return config.getInteger(NettyShuffleEnvironmentOptions.CLIENT_CONNECT_TIMEOUT_SECONDS);
    }

    public int getNetworkRetries() {
        return config.getInteger(NettyShuffleEnvironmentOptions.NETWORK_RETRIES);
    }

    public int getSendAndReceiveBufferSize() {
        return config.getInteger(NettyShuffleEnvironmentOptions.SEND_RECEIVE_BUFFER_SIZE);
    }

    public TransportType getTransportType() {
        String transport = config.getString(NettyShuffleEnvironmentOptions.TRANSPORT_TYPE);

        switch (transport) {
            case "nio":
                return TransportType.NIO;
            case "epoll":
                return TransportType.EPOLL;
            default:
                return TransportType.AUTO;
        }
    }

    public Optional<Integer> getTcpKeepIdleInSeconds() {
        return config.getOptional(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_IDLE_SECONDS);
    }

    public Optional<Integer> getTcpKeepInternalInSeconds() {
        return config.getOptional(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_INTERVAL_SECONDS);
    }

    public Optional<Integer> getTcpKeepCount() {
        return config.getOptional(NettyShuffleEnvironmentOptions.CLIENT_TCP_KEEP_COUNT);
    }

    @Nullable
    public SSLHandlerFactory createClientSSLEngineFactory() throws Exception {
        return getSSLEnabled() ? SSLUtils.createInternalClientSSLEngineFactory(config) : null;
    }

    @Nullable
    public SSLHandlerFactory createServerSSLEngineFactory() throws Exception {
        return getSSLEnabled() ? SSLUtils.createInternalServerSSLEngineFactory(config) : null;
    }

    public boolean getSSLEnabled() {
        return config.getBoolean(NettyShuffleEnvironmentOptions.DATA_SSL_ENABLED)
                && SecurityOptions.isInternalSSLEnabled(config);
    }

    public Configuration getConfig() {
        return config;
    }

    @Override
    public String toString() {
        String format =
                "NettyConfig ["
                        + "server address: %s, "
                        + "server port range: %s, "
                        + "ssl enabled: %s, "
                        + "memory segment size (bytes): %d, "
                        + "transport type: %s, "
                        + "number of server threads: %d (%s), "
                        + "number of client threads: %d (%s), "
                        + "server connect backlog: %d (%s), "
                        + "client connect timeout (sec): %d, "
                        + "send/receive buffer size (bytes): %d (%s)]";

        String def = "use Netty's default";
        String man = "manual";

        return String.format(
                format,
                serverAddress,
                serverPortRange,
                getSSLEnabled() ? "true" : "false",
                memorySegmentSize,
                getTransportType(),
                getServerNumThreads(),
                getServerNumThreads() == 0 ? def : man,
                getClientNumThreads(),
                getClientNumThreads() == 0 ? def : man,
                getServerConnectBacklog(),
                getServerConnectBacklog() == 0 ? def : man,
                getClientConnectTimeoutSeconds(),
                getSendAndReceiveBufferSize(),
                getSendAndReceiveBufferSize() == 0 ? def : man);
    }
}
