/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.queryablestate.network;

import org.apache.flink.queryablestate.network.messages.MessageBody;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.stats.KvStateRequestStats;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Connection class used by the {@link Client}.
 *
 * @param <REQ> Request type
 * @param <RESP> Response type
 */
final class ServerConnection<REQ extends MessageBody, RESP extends MessageBody> {
    private static final Logger LOG = LoggerFactory.getLogger(ServerConnection.class);

    private final Object connectionLock;

    @GuardedBy("connectionLock")
    private InternalConnection<REQ, RESP> internalConnection;

    @GuardedBy("connectionLock")
    private boolean running = true;

    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    private ServerConnection(Object lock, InternalConnection<REQ, RESP> internalConnection) {
        this.connectionLock = lock;
        this.internalConnection = internalConnection;
        forwardCloseFuture();
    }

    @GuardedBy("connectionLock")
    private void forwardCloseFuture() {
        final InternalConnection<REQ, RESP> currentConnection = this.internalConnection;
        currentConnection
                .getCloseFuture()
                .whenComplete(
                        (unused, throwable) -> {
                            synchronized (connectionLock) {
                                if (internalConnection == currentConnection) {
                                    if (throwable != null) {
                                        closeFuture.completeExceptionally(throwable);
                                    } else {
                                        closeFuture.complete(null);
                                    }
                                }
                            }
                        });
    }

    CompletableFuture<RESP> sendRequest(REQ request) {
        synchronized (connectionLock) {
            Preconditions.checkState(running, "Connection has already been closed.");
            return internalConnection.sendRequest(request);
        }
    }

    void establishConnection(ChannelFuture future) {
        synchronized (connectionLock) {
            Preconditions.checkState(running, "Connection has already been closed.");
            this.internalConnection = internalConnection.establishConnection(future);
            forwardCloseFuture();
        }
    }

    CompletableFuture<Void> close() {
        synchronized (connectionLock) {
            if (running) {
                running = false;
                internalConnection.close();
            }

            return closeFuture;
        }
    }

    CompletableFuture<Void> getCloseFuture() {
        return closeFuture;
    }

    static <REQ extends MessageBody, RESP extends MessageBody>
            ServerConnection<REQ, RESP> createPendingConnection(
                    final String clientName,
                    final MessageSerializer<REQ, RESP> serializer,
                    final KvStateRequestStats stats) {
        final Object lock = new Object();

        return new ServerConnection<>(
                lock,
                new PendingConnection<>(
                        channel ->
                                new EstablishedConnection<>(
                                        lock, clientName, serializer, channel, stats)));
    }

    interface InternalConnection<REQ, RESP> {
        CompletableFuture<RESP> sendRequest(REQ request);

        InternalConnection<REQ, RESP> establishConnection(ChannelFuture future);

        boolean isEstablished();

        CompletableFuture<Void> getCloseFuture();

        CompletableFuture<Void> close();
    }

    /** A pending connection that is in the process of connecting. */
    private static final class PendingConnection<REQ extends MessageBody, RESP extends MessageBody>
            implements InternalConnection<REQ, RESP> {

        private final Function<Channel, EstablishedConnection<REQ, RESP>> connectionFactory;
        private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        /** Queue of requests while connecting. */
        private final ArrayDeque<PendingConnection.PendingRequest<REQ, RESP>> queuedRequests =
                new ArrayDeque<>();

        /** Failure cause if something goes wrong. */
        @Nullable private Throwable failureCause = null;

        private boolean running = true;

        /** Creates a pending connection to the given server. */
        private PendingConnection(
                Function<Channel, EstablishedConnection<REQ, RESP>> connectionFactory) {
            this.connectionFactory = connectionFactory;
        }

        /**
         * Returns a future holding the serialized request result.
         *
         * <p>Queues the request for when the channel is handed in.
         *
         * @param request the request to be sent.
         * @return Future holding the serialized result
         */
        @Override
        public CompletableFuture<RESP> sendRequest(REQ request) {
            if (failureCause != null) {
                return FutureUtils.completedExceptionally(failureCause);
            } else if (!running) {
                return FutureUtils.completedExceptionally(new ClosedChannelException());
            } else {
                // Queue this and handle when connected
                final PendingConnection.PendingRequest<REQ, RESP> pending =
                        new PendingConnection.PendingRequest<>(request);
                queuedRequests.add(pending);
                return pending;
            }
        }

        @Override
        public InternalConnection<REQ, RESP> establishConnection(ChannelFuture future) {
            if (future.isSuccess()) {
                return createEstablishedConnection(future.channel());
            } else {
                close(future.cause());
                return this;
            }
        }

        @Override
        public boolean isEstablished() {
            return false;
        }

        @Override
        public CompletableFuture<Void> getCloseFuture() {
            return closeFuture;
        }

        /**
         * Creates an established connection from the given channel.
         *
         * @param channel Channel to create an established connection from
         */
        private InternalConnection<REQ, RESP> createEstablishedConnection(Channel channel) {
            if (failureCause != null || !running) {
                // Close the channel and we are done. Any queued requests
                // are removed on the close/failure call and after that no
                // new ones can be enqueued.
                channel.close();
                return this;
            } else {
                final EstablishedConnection<REQ, RESP> establishedConnection =
                        connectionFactory.apply(channel);

                while (!queuedRequests.isEmpty()) {
                    final PendingConnection.PendingRequest<REQ, RESP> pending =
                            queuedRequests.poll();

                    FutureUtils.forward(
                            establishedConnection.sendRequest(pending.getRequest()), pending);
                }

                return establishedConnection;
            }
        }

        /** Close the connecting channel with a ClosedChannelException. */
        @Override
        public CompletableFuture<Void> close() {
            return close(new ClosedChannelException());
        }

        /**
         * Close the connecting channel with an Exception (can be {@code null}) or forward to the
         * established channel.
         */
        private CompletableFuture<Void> close(Throwable cause) {
            if (running) {
                running = false;
                failureCause = cause;

                for (PendingConnection.PendingRequest<REQ, RESP> pendingRequest : queuedRequests) {
                    pendingRequest.completeExceptionally(cause);
                }
                queuedRequests.clear();

                closeFuture.completeExceptionally(cause);
            }

            return closeFuture;
        }

        /** A pending request queued while the channel is connecting. */
        private static final class PendingRequest<REQ extends MessageBody, RESP extends MessageBody>
                extends CompletableFuture<RESP> {

            private final REQ request;

            private PendingRequest(REQ request) {
                this.request = request;
            }

            public REQ getRequest() {
                return request;
            }
        }
    }

    /**
     * An established connection that wraps the actual channel instance and is registered at the
     * {@link ClientHandler} for callbacks.
     */
    private static class EstablishedConnection<REQ extends MessageBody, RESP extends MessageBody>
            implements ClientHandlerCallback<RESP>, InternalConnection<REQ, RESP> {

        private final Object lock;

        /** The actual TCP channel. */
        private final Channel channel;

        private final KvStateRequestStats stats;

        /** Pending requests keyed by request ID. */
        private final ConcurrentHashMap<
                        Long, EstablishedConnection.TimestampedCompletableFuture<RESP>>
                pendingRequests = new ConcurrentHashMap<>();

        private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        /** Current request number used to assign unique request IDs. */
        @GuardedBy("lock")
        private long requestCount = 0;

        @GuardedBy("lock")
        private boolean running = true;

        /**
         * Creates an established connection with the given channel.
         *
         * @param channel The actual TCP channel
         */
        EstablishedConnection(
                final Object lock,
                final String clientName,
                final MessageSerializer<REQ, RESP> serializer,
                final Channel channel,
                final KvStateRequestStats stats) {

            this.lock = lock;
            this.channel = Preconditions.checkNotNull(channel);

            // Add the client handler with the callback
            channel.pipeline()
                    .addLast(
                            clientName + " Handler",
                            new ClientHandler<>(clientName, serializer, this));

            this.stats = stats;
            stats.reportActiveConnection();
        }

        /** Close the channel with a ClosedChannelException. */
        @Override
        public CompletableFuture<Void> close() {
            return close(new ClosedChannelException());
        }

        /**
         * Close the channel with a cause.
         *
         * @param cause The cause to close the channel with.
         * @return Channel close future
         */
        private CompletableFuture<Void> close(final Throwable cause) {
            synchronized (lock) {
                if (running) {
                    running = false;
                    channel.close()
                            .addListener(
                                    finished -> {
                                        stats.reportInactiveConnection();
                                        for (long requestId : pendingRequests.keySet()) {
                                            EstablishedConnection.TimestampedCompletableFuture<RESP>
                                                    pending = pendingRequests.remove(requestId);
                                            if (pending != null
                                                    && pending.completeExceptionally(cause)) {
                                                stats.reportFailedRequest();
                                            }
                                        }

                                        // when finishing, if netty successfully closes the channel,
                                        // then the provided exception is used
                                        // as the reason for the closing. If there was something
                                        // wrong
                                        // at the netty side, then that exception
                                        // is prioritized over the provided one.
                                        if (finished.isSuccess()) {
                                            closeFuture.completeExceptionally(cause);
                                        } else {
                                            LOG.warn(
                                                    "Something went wrong when trying to close connection due to : ",
                                                    cause);
                                            closeFuture.completeExceptionally(finished.cause());
                                        }
                                    });
                }
            }

            return closeFuture;
        }

        /**
         * Returns a future holding the serialized request result.
         *
         * @param request the request to be sent.
         * @return Future holding the serialized result
         */
        @Override
        public CompletableFuture<RESP> sendRequest(REQ request) {
            synchronized (lock) {
                if (running) {
                    EstablishedConnection.TimestampedCompletableFuture<RESP> requestPromiseTs =
                            new EstablishedConnection.TimestampedCompletableFuture<>(
                                    System.nanoTime());
                    try {
                        final long requestId = requestCount++;
                        pendingRequests.put(requestId, requestPromiseTs);

                        stats.reportRequest();

                        ByteBuf buf =
                                MessageSerializer.serializeRequest(
                                        channel.alloc(), requestId, request);

                        channel.writeAndFlush(buf)
                                .addListener(
                                        (ChannelFutureListener)
                                                future -> {
                                                    if (!future.isSuccess()) {
                                                        // Fail promise if not failed to write
                                                        EstablishedConnection
                                                                                .TimestampedCompletableFuture<
                                                                        RESP>
                                                                pending =
                                                                        pendingRequests.remove(
                                                                                requestId);
                                                        if (pending != null
                                                                && pending.completeExceptionally(
                                                                        future.cause())) {
                                                            stats.reportFailedRequest();
                                                        }
                                                    }
                                                });
                    } catch (Throwable t) {
                        requestPromiseTs.completeExceptionally(t);
                    }

                    return requestPromiseTs;
                } else {
                    return FutureUtils.completedExceptionally(new ClosedChannelException());
                }
            }
        }

        @Override
        public InternalConnection<REQ, RESP> establishConnection(ChannelFuture future) {
            throw new IllegalStateException("The connection is already established.");
        }

        @Override
        public boolean isEstablished() {
            return true;
        }

        @Override
        public CompletableFuture<Void> getCloseFuture() {
            return closeFuture;
        }

        @Override
        public void onRequestResult(long requestId, RESP response) {
            EstablishedConnection.TimestampedCompletableFuture<RESP> pending =
                    pendingRequests.remove(requestId);
            if (pending != null && !pending.isDone()) {
                long durationMillis = (System.nanoTime() - pending.getTimestamp()) / 1_000_000L;
                stats.reportSuccessfulRequest(durationMillis);
                pending.complete(response);
            }
        }

        @Override
        public void onRequestFailure(long requestId, Throwable cause) {
            EstablishedConnection.TimestampedCompletableFuture<RESP> pending =
                    pendingRequests.remove(requestId);
            if (pending != null && !pending.isDone()) {
                stats.reportFailedRequest();
                pending.completeExceptionally(cause);
            }
        }

        @Override
        public void onFailure(Throwable cause) {
            close(cause);
        }

        /** Pair of promise and a timestamp. */
        private static final class TimestampedCompletableFuture<RESP extends MessageBody>
                extends CompletableFuture<RESP> {

            private final long timestampInNanos;

            TimestampedCompletableFuture(long timestampInNanos) {
                this.timestampInNanos = timestampInNanos;
            }

            public long getTimestamp() {
                return timestampInNanos;
            }
        }
    }
}
