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

package org.apache.flink.networking;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class acts as a network proxy - listening on local port and forwarding all of the network to
 * the remote host/port. It allows to simulate a network failures in the communication.
 */
public class NetworkFailuresProxy implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkFailuresProxy.class);
    private static final String NETWORK_FAILURE_HANDLER_NAME = "network_failure_handler";

    private final Executor executor = Executors.newCachedThreadPool();
    private final ServerBootstrap serverBootstrap;
    private final Channel channel;
    private final AtomicBoolean blocked = new AtomicBoolean();
    // collection of networkFailureHandlers so that we can call {@link
    // NetworkFailureHandler.closeConnections} on them.
    private final Set<NetworkFailureHandler> networkFailureHandlers =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    public NetworkFailuresProxy(int localPort, String remoteHost, int remotePort) {
        // Configure the bootstrap.
        serverBootstrap =
                new ServerBootstrap(new NioServerSocketChannelFactory(executor, executor));

        // Set up the event pipeline factory.
        ClientSocketChannelFactory channelFactory =
                new NioClientSocketChannelFactory(executor, executor);
        serverBootstrap.setOption("child.tcpNoDelay", true);
        serverBootstrap.setOption("child.keepAlive", true);
        serverBootstrap.setPipelineFactory(
                new ChannelPipelineFactory() {
                    public ChannelPipeline getPipeline() throws Exception {
                        ChannelPipeline pipeline = Channels.pipeline();

                        // synchronized for a race between blocking and creating new handlers
                        synchronized (networkFailureHandlers) {
                            NetworkFailureHandler failureHandler =
                                    new NetworkFailureHandler(
                                            blocked,
                                            networkFailureHandler ->
                                                    networkFailureHandlers.remove(
                                                            networkFailureHandler),
                                            channelFactory,
                                            remoteHost,
                                            remotePort);
                            networkFailureHandlers.add(failureHandler);
                            pipeline.addLast(NETWORK_FAILURE_HANDLER_NAME, failureHandler);
                        }
                        return pipeline;
                    }
                });
        channel = serverBootstrap.bind(new InetSocketAddress(localPort));

        LOG.info("Proxying [*:{}] to [{}:{}]", getLocalPort(), remoteHost, remotePort);
    }

    /** @return local port on which {@link NetworkFailuresProxy} is listening. */
    public int getLocalPort() {
        return ((InetSocketAddress) channel.getLocalAddress()).getPort();
    }

    /** Blocks all ongoing traffic, closes all ongoing and closes any new incoming connections. */
    public void blockTraffic() {
        setTrafficBlocked(true);
    }

    /** Resumes normal communication. */
    public void unblockTraffic() {
        setTrafficBlocked(false);
    }

    @Override
    public void close() throws Exception {
        channel.close();
    }

    private void setTrafficBlocked(boolean blocked) {
        this.blocked.set(blocked);
        if (blocked) {
            // synchronized for a race between blocking and creating new handlers
            synchronized (networkFailureHandlers) {
                for (NetworkFailureHandler failureHandler : networkFailureHandlers) {
                    failureHandler.closeConnections();
                }
            }
        }
    }
}
