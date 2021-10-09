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

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * Factory for {@link NettyPartitionRequestClient} instances.
 *
 * <p>Instances of partition requests clients are shared among several {@link RemoteInputChannel}
 * instances.
 */
class PartitionRequestClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestClientFactory.class);

    private final NettyClient nettyClient;

    private final int retryNumber;

    private final ConcurrentMap<ConnectionID, CompletableFuture<NettyPartitionRequestClient>>
            clients = new ConcurrentHashMap<>();

    PartitionRequestClientFactory(NettyClient nettyClient) {
        this(nettyClient, 0);
    }

    PartitionRequestClientFactory(NettyClient nettyClient, int retryNumber) {
        this.nettyClient = nettyClient;
        this.retryNumber = retryNumber;
    }

    /**
     * Atomically establishes a TCP connection to the given remote address and creates a {@link
     * NettyPartitionRequestClient} instance for this connection.
     */
    NettyPartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
            throws IOException, InterruptedException {
        while (true) {
            final CompletableFuture<NettyPartitionRequestClient> newClientFuture =
                    new CompletableFuture<>();

            final CompletableFuture<NettyPartitionRequestClient> clientFuture =
                    clients.putIfAbsent(connectionId, newClientFuture);

            final NettyPartitionRequestClient client;

            if (clientFuture == null) {
                try {
                    client = connectWithRetries(connectionId);
                } catch (Throwable e) {
                    newClientFuture.completeExceptionally(
                            new IOException("Could not create Netty client.", e));
                    clients.remove(connectionId, newClientFuture);
                    throw e;
                }

                newClientFuture.complete(client);
            } else {
                try {
                    client = clientFuture.get();
                } catch (ExecutionException e) {
                    ExceptionUtils.rethrowIOException(ExceptionUtils.stripExecutionException(e));
                    return null;
                }
            }

            // Make sure to increment the reference count before handing a client
            // out to ensure correct bookkeeping for channel closing.
            if (client.incrementReferenceCounter()) {
                return client;
            } else {
                destroyPartitionRequestClient(connectionId, client);
            }
        }
    }

    private NettyPartitionRequestClient connectWithRetries(ConnectionID connectionId)
            throws InterruptedException, RemoteTransportException {
        int tried = 0;
        while (true) {
            try {
                return connect(connectionId);
            } catch (RemoteTransportException e) {
                tried++;
                if (tried > retryNumber) {
                    LOG.warn("Failed to connect to {}. Giving up.", connectionId.getAddress(), e);
                    throw e;
                } else {
                    LOG.warn(
                            "Failed {} times to connect to {}. Retrying.",
                            tried,
                            connectionId.getAddress(),
                            e);
                }
            }
        }
    }

    private NettyPartitionRequestClient connect(ConnectionID connectionId)
            throws RemoteTransportException, InterruptedException {
        try {
            // It's important to use `sync` here because it waits for this future until it is
            // done, and rethrows the cause of the failure if this future failed. `await` only
            // waits for this future to be completed, without throwing the error.
            Channel channel = nettyClient.connect(connectionId.getAddress()).sync().channel();
            NetworkClientHandler clientHandler = channel.pipeline().get(NetworkClientHandler.class);
            return new NettyPartitionRequestClient(channel, clientHandler, connectionId, this);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new RemoteTransportException(
                    "Connecting to remote task manager '"
                            + connectionId.getAddress()
                            + "' has failed. This might indicate that the remote task "
                            + "manager has been lost.",
                    connectionId.getAddress(),
                    e);
        }
    }

    void closeOpenChannelConnections(ConnectionID connectionId) {
        CompletableFuture<NettyPartitionRequestClient> entry = clients.get(connectionId);

        if (entry != null && !entry.isDone()) {
            entry.thenAccept(
                    client -> {
                        if (client.disposeIfNotUsed()) {
                            clients.remove(connectionId, entry);
                        }
                    });
        }
    }

    int getNumberOfActiveClients() {
        return clients.size();
    }

    /** Removes the client for the given {@link ConnectionID}. */
    void destroyPartitionRequestClient(ConnectionID connectionId, PartitionRequestClient client) {
        final CompletableFuture<NettyPartitionRequestClient> future = clients.get(connectionId);
        if (future != null && future.isDone()) {
            future.thenAccept(
                    futureClient -> {
                        if (client.equals(futureClient)) {
                            clients.remove(connectionId, future);
                        }
                    });
        }
    }
}
