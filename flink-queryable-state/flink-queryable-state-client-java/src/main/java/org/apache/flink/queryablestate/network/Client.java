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

package org.apache.flink.queryablestate.network;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.queryablestate.FutureUtils;
import org.apache.flink.queryablestate.network.messages.MessageBody;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.stats.KvStateRequestStats;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedWriteHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The base class for every client in the queryable state module. It is using pure netty to send and
 * receive messages of type {@link MessageBody}.
 *
 * @param <REQ> the type of request the client will send.
 * @param <RESP> the type of response the client expects to receive.
 */
@Internal
public class Client<REQ extends MessageBody, RESP extends MessageBody> {

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    /** The name of the client. Used for logging and stack traces. */
    private final String clientName;

    /** Netty's Bootstrap. */
    private final Bootstrap bootstrap;

    /** The serializer to be used for (de-)serializing messages. */
    private final MessageSerializer<REQ, RESP> messageSerializer;

    /** Statistics tracker. */
    private final KvStateRequestStats stats;

    /** Established connections. */
    private final Map<InetSocketAddress, EstablishedConnection> establishedConnections =
            new ConcurrentHashMap<>();

    /** Pending connections. */
    private final Map<InetSocketAddress, PendingConnection> pendingConnections =
            new ConcurrentHashMap<>();

    /** Atomic shut down future. */
    private final AtomicReference<CompletableFuture<Void>> clientShutdownFuture =
            new AtomicReference<>(null);

    /**
     * Creates a client with the specified number of event loop threads.
     *
     * @param clientName the name of the client.
     * @param numEventLoopThreads number of event loop threads (minimum 1).
     * @param serializer the serializer used to (de-)serialize messages.
     * @param stats the statistics collector.
     */
    public Client(
            final String clientName,
            final int numEventLoopThreads,
            final MessageSerializer<REQ, RESP> serializer,
            final KvStateRequestStats stats) {

        Preconditions.checkArgument(
                numEventLoopThreads >= 1, "Non-positive number of event loop threads.");

        this.clientName = Preconditions.checkNotNull(clientName);
        this.messageSerializer = Preconditions.checkNotNull(serializer);
        this.stats = Preconditions.checkNotNull(stats);

        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("Flink " + clientName + " Event Loop Thread %d")
                        .build();

        final EventLoopGroup nioGroup = new NioEventLoopGroup(numEventLoopThreads, threadFactory);
        final ByteBufAllocator bufferPool = new NettyBufferPool(numEventLoopThreads);

        this.bootstrap =
                new Bootstrap()
                        .group(nioGroup)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.ALLOCATOR, bufferPool)
                        .handler(
                                new ChannelInitializer<SocketChannel>() {
                                    @Override
                                    protected void initChannel(SocketChannel channel)
                                            throws Exception {
                                        channel.pipeline()
                                                .addLast(
                                                        new LengthFieldBasedFrameDecoder(
                                                                Integer.MAX_VALUE, 0, 4, 0, 4))
                                                .addLast(new ChunkedWriteHandler());
                                    }
                                });
    }

    public String getClientName() {
        return clientName;
    }

    public CompletableFuture<RESP> sendRequest(
            final InetSocketAddress serverAddress, final REQ request) {
        if (clientShutdownFuture.get() != null) {
            return FutureUtils.getFailedFuture(
                    new IllegalStateException(clientName + " is already shut down."));
        }

        EstablishedConnection connection = establishedConnections.get(serverAddress);
        if (connection != null) {
            return connection.sendRequest(request);
        } else {
            PendingConnection pendingConnection = pendingConnections.get(serverAddress);
            if (pendingConnection != null) {
                // There was a race, use the existing pending connection.
                return pendingConnection.sendRequest(request);
            } else {
                // We try to connect to the server.
                PendingConnection pending = new PendingConnection(serverAddress, messageSerializer);
                PendingConnection previous = pendingConnections.putIfAbsent(serverAddress, pending);

                if (previous == null) {
                    // OK, we are responsible to connect.
                    bootstrap
                            .connect(serverAddress.getAddress(), serverAddress.getPort())
                            .addListener(pending);
                    return pending.sendRequest(request);
                } else {
                    // There was a race, use the existing pending connection.
                    return previous.sendRequest(request);
                }
            }
        }
    }

    /**
     * Shuts down the client and closes all connections.
     *
     * <p>After a call to this method, all returned futures will be failed.
     *
     * @return A {@link CompletableFuture} that will be completed when the shutdown process is done.
     */
    public CompletableFuture<Void> shutdown() {
        final CompletableFuture<Void> newShutdownFuture = new CompletableFuture<>();
        if (clientShutdownFuture.compareAndSet(null, newShutdownFuture)) {

            final List<CompletableFuture<Void>> connectionFutures = new ArrayList<>();

            for (Map.Entry<InetSocketAddress, EstablishedConnection> conn :
                    establishedConnections.entrySet()) {
                if (establishedConnections.remove(conn.getKey(), conn.getValue())) {
                    connectionFutures.add(conn.getValue().close());
                }
            }

            for (Map.Entry<InetSocketAddress, PendingConnection> conn :
                    pendingConnections.entrySet()) {
                if (pendingConnections.remove(conn.getKey()) != null) {
                    connectionFutures.add(conn.getValue().close());
                }
            }

            CompletableFuture.allOf(
                            connectionFutures.toArray(
                                    new CompletableFuture<?>[connectionFutures.size()]))
                    .whenComplete(
                            (result, throwable) -> {
                                if (throwable != null) {
                                    LOG.warn(
                                            "Problem while shutting down the connections at the {}: {}",
                                            clientName,
                                            throwable);
                                }

                                if (bootstrap != null) {
                                    EventLoopGroup group = bootstrap.group();
                                    if (group != null && !group.isShutdown()) {
                                        group.shutdownGracefully(0L, 0L, TimeUnit.MILLISECONDS)
                                                .addListener(
                                                        finished -> {
                                                            if (finished.isSuccess()) {
                                                                newShutdownFuture.complete(null);
                                                            } else {
                                                                newShutdownFuture
                                                                        .completeExceptionally(
                                                                                finished.cause());
                                                            }
                                                        });
                                    } else {
                                        newShutdownFuture.complete(null);
                                    }
                                } else {
                                    newShutdownFuture.complete(null);
                                }
                            });

            return newShutdownFuture;
        }
        return clientShutdownFuture.get();
    }

    /** A pending connection that is in the process of connecting. */
    private class PendingConnection implements ChannelFutureListener {

        /** Lock to guard the connect call, channel hand in, etc. */
        private final Object connectLock = new Object();

        /** Address of the server we are connecting to. */
        private final InetSocketAddress serverAddress;

        private final MessageSerializer<REQ, RESP> serializer;

        /** Queue of requests while connecting. */
        private final ArrayDeque<PendingRequest> queuedRequests = new ArrayDeque<>();

        /** The established connection after the connect succeeds. */
        private EstablishedConnection established;

        /** Atomic shut down future. */
        private final AtomicReference<CompletableFuture<Void>> connectionShutdownFuture =
                new AtomicReference<>(null);

        /** Failure cause if something goes wrong. */
        private Throwable failureCause;

        /**
         * Creates a pending connection to the given server.
         *
         * @param serverAddress Address of the server to connect to.
         */
        private PendingConnection(
                final InetSocketAddress serverAddress,
                final MessageSerializer<REQ, RESP> serializer) {
            this.serverAddress = serverAddress;
            this.serializer = serializer;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
                handInChannel(future.channel());
            } else {
                close(future.cause());
            }
        }

        /**
         * Returns a future holding the serialized request result.
         *
         * <p>If the channel has been established, forward the call to the established channel,
         * otherwise queue it for when the channel is handed in.
         *
         * @param request the request to be sent.
         * @return Future holding the serialized result
         */
        CompletableFuture<RESP> sendRequest(REQ request) {
            synchronized (connectLock) {
                if (failureCause != null) {
                    return FutureUtils.getFailedFuture(failureCause);
                } else if (connectionShutdownFuture.get() != null) {
                    return FutureUtils.getFailedFuture(new ClosedChannelException());
                } else {
                    if (established != null) {
                        return established.sendRequest(request);
                    } else {
                        // Queue this and handle when connected
                        final PendingRequest pending = new PendingRequest(request);
                        queuedRequests.add(pending);
                        return pending;
                    }
                }
            }
        }

        /**
         * Hands in a channel after a successful connection.
         *
         * @param channel Channel to hand in
         */
        private void handInChannel(Channel channel) {
            synchronized (connectLock) {
                if (connectionShutdownFuture.get() != null || failureCause != null) {
                    // Close the channel and we are done. Any queued requests
                    // are removed on the close/failure call and after that no
                    // new ones can be enqueued.
                    channel.close();
                } else {
                    established = new EstablishedConnection(serverAddress, serializer, channel);

                    while (!queuedRequests.isEmpty()) {
                        final PendingRequest pending = queuedRequests.poll();

                        established
                                .sendRequest(pending.request)
                                .whenComplete(
                                        (response, throwable) -> {
                                            if (throwable != null) {
                                                pending.completeExceptionally(throwable);
                                            } else {
                                                pending.complete(response);
                                            }
                                        });
                    }

                    // Publish the channel for the general public
                    establishedConnections.put(serverAddress, established);
                    pendingConnections.remove(serverAddress);

                    // Check shut down for possible race with shut down. We
                    // don't want any lingering connections after shut down,
                    // which can happen if we don't check this here.
                    if (clientShutdownFuture.get() != null) {
                        if (establishedConnections.remove(serverAddress, established)) {
                            established.close();
                        }
                    }
                }
            }
        }

        /** Close the connecting channel with a ClosedChannelException. */
        private CompletableFuture<Void> close() {
            return close(new ClosedChannelException());
        }

        /**
         * Close the connecting channel with an Exception (can be {@code null}) or forward to the
         * established channel.
         */
        private CompletableFuture<Void> close(Throwable cause) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            if (connectionShutdownFuture.compareAndSet(null, future)) {
                synchronized (connectLock) {
                    if (failureCause == null) {
                        failureCause = cause;
                    }

                    if (established != null) {
                        established
                                .close()
                                .whenComplete(
                                        (result, throwable) -> {
                                            if (throwable != null) {
                                                future.completeExceptionally(throwable);
                                            } else {
                                                future.complete(null);
                                            }
                                        });
                    } else {
                        PendingRequest pending;
                        while ((pending = queuedRequests.poll()) != null) {
                            pending.completeExceptionally(cause);
                        }
                        future.complete(null);
                    }
                }
            }
            return connectionShutdownFuture.get();
        }

        @Override
        public String toString() {
            synchronized (connectLock) {
                return "PendingConnection{"
                        + "serverAddress="
                        + serverAddress
                        + ", queuedRequests="
                        + queuedRequests.size()
                        + ", established="
                        + (established != null)
                        + ", closed="
                        + (connectionShutdownFuture.get() != null)
                        + '}';
            }
        }

        /** A pending request queued while the channel is connecting. */
        private final class PendingRequest extends CompletableFuture<RESP> {

            private final REQ request;

            private PendingRequest(REQ request) {
                this.request = request;
            }
        }
    }

    /**
     * An established connection that wraps the actual channel instance and is registered at the
     * {@link ClientHandler} for callbacks.
     */
    private class EstablishedConnection implements ClientHandlerCallback<RESP> {

        /** Address of the server we are connected to. */
        private final InetSocketAddress serverAddress;

        /** The actual TCP channel. */
        private final Channel channel;

        /** Pending requests keyed by request ID. */
        private final ConcurrentHashMap<Long, TimestampedCompletableFuture> pendingRequests =
                new ConcurrentHashMap<>();

        /** Current request number used to assign unique request IDs. */
        private final AtomicLong requestCount = new AtomicLong();

        /** Atomic shut down future. */
        private final AtomicReference<CompletableFuture<Void>> connectionShutdownFuture =
                new AtomicReference<>(null);

        /**
         * Creates an established connection with the given channel.
         *
         * @param serverAddress Address of the server connected to
         * @param channel The actual TCP channel
         */
        EstablishedConnection(
                final InetSocketAddress serverAddress,
                final MessageSerializer<REQ, RESP> serializer,
                final Channel channel) {

            this.serverAddress = Preconditions.checkNotNull(serverAddress);
            this.channel = Preconditions.checkNotNull(channel);

            // Add the client handler with the callback
            channel.pipeline()
                    .addLast(
                            getClientName() + " Handler",
                            new ClientHandler<>(clientName, serializer, this));

            stats.reportActiveConnection();
        }

        /** Close the channel with a ClosedChannelException. */
        CompletableFuture<Void> close() {
            return close(new ClosedChannelException());
        }

        /**
         * Close the channel with a cause.
         *
         * @param cause The cause to close the channel with.
         * @return Channel close future
         */
        private CompletableFuture<Void> close(final Throwable cause) {
            final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();

            if (connectionShutdownFuture.compareAndSet(null, shutdownFuture)) {
                channel.close()
                        .addListener(
                                finished -> {
                                    stats.reportInactiveConnection();
                                    for (long requestId : pendingRequests.keySet()) {
                                        TimestampedCompletableFuture pending =
                                                pendingRequests.remove(requestId);
                                        if (pending != null
                                                && pending.completeExceptionally(cause)) {
                                            stats.reportFailedRequest();
                                        }
                                    }

                                    // when finishing, if netty successfully closes the channel,
                                    // then the provided exception is used
                                    // as the reason for the closing. If there was something wrong
                                    // at the netty side, then that exception
                                    // is prioritized over the provided one.
                                    if (finished.isSuccess()) {
                                        shutdownFuture.completeExceptionally(cause);
                                    } else {
                                        LOG.warn(
                                                "Something went wrong when trying to close connection due to : ",
                                                cause);
                                        shutdownFuture.completeExceptionally(finished.cause());
                                    }
                                });
            }

            // in case we had a race condition, return the winner of the race.
            return connectionShutdownFuture.get();
        }

        /**
         * Returns a future holding the serialized request result.
         *
         * @param request the request to be sent.
         * @return Future holding the serialized result
         */
        CompletableFuture<RESP> sendRequest(REQ request) {
            TimestampedCompletableFuture requestPromiseTs =
                    new TimestampedCompletableFuture(System.nanoTime());
            try {
                final long requestId = requestCount.getAndIncrement();
                pendingRequests.put(requestId, requestPromiseTs);

                stats.reportRequest();

                ByteBuf buf =
                        MessageSerializer.serializeRequest(channel.alloc(), requestId, request);

                channel.writeAndFlush(buf)
                        .addListener(
                                (ChannelFutureListener)
                                        future -> {
                                            if (!future.isSuccess()) {
                                                // Fail promise if not failed to write
                                                TimestampedCompletableFuture pending =
                                                        pendingRequests.remove(requestId);
                                                if (pending != null
                                                        && pending.completeExceptionally(
                                                                future.cause())) {
                                                    stats.reportFailedRequest();
                                                }
                                            }
                                        });

                // Check for possible race. We don't want any lingering
                // promises after a failure, which can happen if we don't check
                // this here. Note that close is treated as a failure as well.
                CompletableFuture<Void> clShutdownFuture = clientShutdownFuture.get();
                if (clShutdownFuture != null) {
                    TimestampedCompletableFuture pending = pendingRequests.remove(requestId);
                    if (pending != null) {
                        clShutdownFuture.whenComplete(
                                (ignored, throwable) -> {
                                    if (throwable != null
                                            && pending.completeExceptionally(throwable)) {
                                        stats.reportFailedRequest();
                                    } else {
                                        // the shutdown future is always completed exceptionally so
                                        // we should not arrive here.
                                        // but in any case, we complete the pending connection
                                        // request exceptionally.
                                        pending.completeExceptionally(new ClosedChannelException());
                                    }
                                });
                    }
                }
            } catch (Throwable t) {
                requestPromiseTs.completeExceptionally(t);
            }

            return requestPromiseTs;
        }

        @Override
        public void onRequestResult(long requestId, RESP response) {
            TimestampedCompletableFuture pending = pendingRequests.remove(requestId);
            if (pending != null && !pending.isDone()) {
                long durationMillis = (System.nanoTime() - pending.getTimestamp()) / 1_000_000L;
                stats.reportSuccessfulRequest(durationMillis);
                pending.complete(response);
            }
        }

        @Override
        public void onRequestFailure(long requestId, Throwable cause) {
            TimestampedCompletableFuture pending = pendingRequests.remove(requestId);
            if (pending != null && !pending.isDone()) {
                stats.reportFailedRequest();
                pending.completeExceptionally(cause);
            }
        }

        @Override
        public void onFailure(Throwable cause) {
            close(cause)
                    .handle(
                            (cancelled, ignored) ->
                                    establishedConnections.remove(serverAddress, this));
        }

        @Override
        public String toString() {
            return "EstablishedConnection{"
                    + "serverAddress="
                    + serverAddress
                    + ", channel="
                    + channel
                    + ", pendingRequests="
                    + pendingRequests.size()
                    + ", requestCount="
                    + requestCount
                    + '}';
        }

        /** Pair of promise and a timestamp. */
        private class TimestampedCompletableFuture extends CompletableFuture<RESP> {

            private final long timestampInNanos;

            TimestampedCompletableFuture(long timestampInNanos) {
                this.timestampInNanos = timestampInNanos;
            }

            public long getTimestamp() {
                return timestampInNanos;
            }
        }
    }

    @VisibleForTesting
    public boolean isEventGroupShutdown() {
        return bootstrap == null || bootstrap.group().isTerminated();
    }
}
