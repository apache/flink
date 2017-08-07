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

package org.apache.flink.runtime.query.netty;

import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.runtime.query.KvStateServerAddress;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
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

import akka.dispatch.Futures;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import scala.concurrent.Future;
import scala.concurrent.Promise;

/**
 * Netty-based client querying {@link KvStateServer} instances.
 *
 * <p>This client can be used by multiple threads concurrently. Operations are
 * executed asynchronously and return Futures to their result.
 *
 * <p>The incoming pipeline looks as follows:
 * <pre>
 * Socket.read() -> LengthFieldBasedFrameDecoder -> KvStateServerHandler
 * </pre>
 *
 * <p>Received binary messages are expected to contain a frame length field. Netty's
 * {@link LengthFieldBasedFrameDecoder} is used to fully receive the frame before
 * giving it to our {@link KvStateClientHandler}.
 *
 * <p>Connections are established and closed by the client. The server only
 * closes the connection on a fatal failure that cannot be recovered.
 */
public class KvStateClient {

	/** Netty's Bootstrap. */
	private final Bootstrap bootstrap;

	/** Statistics tracker. */
	private final KvStateRequestStats stats;

	/** Established connections. */
	private final ConcurrentHashMap<KvStateServerAddress, EstablishedConnection> establishedConnections =
			new ConcurrentHashMap<>();

	/** Pending connections. */
	private final ConcurrentHashMap<KvStateServerAddress, PendingConnection> pendingConnections =
			new ConcurrentHashMap<>();

	/** Atomic shut down flag. */
	private final AtomicBoolean shutDown = new AtomicBoolean();

	/**
	 * Creates a client with the specified number of event loop threads.
	 *
	 * @param numEventLoopThreads Number of event loop threads (minimum 1).
	 */
	public KvStateClient(int numEventLoopThreads, KvStateRequestStats stats) {
		Preconditions.checkArgument(numEventLoopThreads >= 1, "Non-positive number of event loop threads.");
		NettyBufferPool bufferPool = new NettyBufferPool(numEventLoopThreads);

		ThreadFactory threadFactory = new ThreadFactoryBuilder()
				.setDaemon(true)
				.setNameFormat("Flink KvStateClient Event Loop Thread %d")
				.build();

		NioEventLoopGroup nioGroup = new NioEventLoopGroup(numEventLoopThreads, threadFactory);

		this.bootstrap = new Bootstrap()
				.group(nioGroup)
				.channel(NioSocketChannel.class)
				.option(ChannelOption.ALLOCATOR, bufferPool)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ch.pipeline()
								.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
								// ChunkedWriteHandler respects Channel writability
								.addLast(new ChunkedWriteHandler());
					}
				});

		this.stats = Preconditions.checkNotNull(stats, "Statistics tracker");
	}

	/**
	 * Returns a future holding the serialized request result.
	 *
	 * <p>If the server does not serve a KvState instance with the given ID,
	 * the Future will be failed with a {@link UnknownKvStateID}.
	 *
	 * <p>If the KvState instance does not hold any data for the given key
	 * and namespace, the Future will be failed with a {@link UnknownKeyOrNamespace}.
	 *
	 * <p>All other failures are forwarded to the Future.
	 *
	 * @param serverAddress Address of the server to query
	 * @param kvStateId ID of the KvState instance to query
	 * @param serializedKeyAndNamespace Serialized key and namespace to query KvState instance with
	 * @return Future holding the serialized result
	 */
	public Future<byte[]> getKvState(
			KvStateServerAddress serverAddress,
			KvStateID kvStateId,
			byte[] serializedKeyAndNamespace) {

		if (shutDown.get()) {
			return Futures.failed(new IllegalStateException("Shut down"));
		}

		EstablishedConnection connection = establishedConnections.get(serverAddress);

		if (connection != null) {
			return connection.getKvState(kvStateId, serializedKeyAndNamespace);
		} else {
			PendingConnection pendingConnection = pendingConnections.get(serverAddress);
			if (pendingConnection != null) {
				// There was a race, use the existing pending connection.
				return pendingConnection.getKvState(kvStateId, serializedKeyAndNamespace);
			} else {
				// We try to connect to the server.
				PendingConnection pending = new PendingConnection(serverAddress);
				PendingConnection previous = pendingConnections.putIfAbsent(serverAddress, pending);

				if (previous == null) {
					// OK, we are responsible to connect.
					bootstrap.connect(serverAddress.getHost(), serverAddress.getPort())
							.addListener(pending);

					return pending.getKvState(kvStateId, serializedKeyAndNamespace);
				} else {
					// There was a race, use the existing pending connection.
					return previous.getKvState(kvStateId, serializedKeyAndNamespace);
				}
			}
		}
	}

	/**
	 * Shuts down the client and closes all connections.
	 *
	 * <p>After a call to this method, all returned futures will be failed.
	 */
	public void shutDown() {
		if (shutDown.compareAndSet(false, true)) {
			for (Map.Entry<KvStateServerAddress, EstablishedConnection> conn : establishedConnections.entrySet()) {
				if (establishedConnections.remove(conn.getKey(), conn.getValue())) {
					conn.getValue().close();
				}
			}

			for (Map.Entry<KvStateServerAddress, PendingConnection> conn : pendingConnections.entrySet()) {
				if (pendingConnections.remove(conn.getKey()) != null) {
					conn.getValue().close();
				}
			}

			if (bootstrap != null) {
				EventLoopGroup group = bootstrap.group();
				if (group != null) {
					group.shutdownGracefully(0, 10, TimeUnit.SECONDS);
				}
			}
		}
	}

	/**
	 * Closes the connection to the given server address if it exists.
	 *
	 * <p>If there is a request to the server a new connection will be established.
	 *
	 * @param serverAddress Target address of the connection to close
	 */
	public void closeConnection(KvStateServerAddress serverAddress) {
		PendingConnection pending = pendingConnections.get(serverAddress);
		if (pending != null) {
			pending.close();
		}

		EstablishedConnection established = establishedConnections.remove(serverAddress);
		if (established != null) {
			established.close();
		}
	}

	/**
	 * A pending connection that is in the process of connecting.
	 */
	private class PendingConnection implements ChannelFutureListener {

		/** Lock to guard the connect call, channel hand in, etc. */
		private final Object connectLock = new Object();

		/** Address of the server we are connecting to. */
		private final KvStateServerAddress serverAddress;

		/** Queue of requests while connecting. */
		private final ArrayDeque<PendingRequest> queuedRequests = new ArrayDeque<>();

		/** The established connection after the connect succeeds. */
		private EstablishedConnection established;

		/** Closed flag. */
		private boolean closed;

		/** Failure cause if something goes wrong. */
		private Throwable failureCause;

		/**
		 * Creates a pending connection to the given server.
		 *
		 * @param serverAddress Address of the server to connect to.
		 */
		private PendingConnection(KvStateServerAddress serverAddress) {
			this.serverAddress = serverAddress;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			// Callback from the Bootstrap's connect call.
			if (future.isSuccess()) {
				handInChannel(future.channel());
			} else {
				close(future.cause());
			}
		}

		/**
		 * Returns a future holding the serialized request result.
		 *
		 * <p>If the channel has been established, forward the call to the
		 * established channel, otherwise queue it for when the channel is
		 * handed in.
		 *
		 * @param kvStateId                 ID of the KvState instance to query
		 * @param serializedKeyAndNamespace Serialized key and namespace to query KvState instance
		 *                                  with
		 * @return Future holding the serialized result
		 */
		public Future<byte[]> getKvState(KvStateID kvStateId, byte[] serializedKeyAndNamespace) {
			synchronized (connectLock) {
				if (failureCause != null) {
					return Futures.failed(failureCause);
				} else if (closed) {
					return Futures.failed(new ClosedChannelException());
				} else {
					if (established != null) {
						return established.getKvState(kvStateId, serializedKeyAndNamespace);
					} else {
						// Queue this and handle when connected
						PendingRequest pending = new PendingRequest(kvStateId, serializedKeyAndNamespace);
						queuedRequests.add(pending);
						return pending.promise.future();
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
				if (closed || failureCause != null) {
					// Close the channel and we are done. Any queued requests
					// are removed on the close/failure call and after that no
					// new ones can be enqueued.
					channel.close();
				} else {
					established = new EstablishedConnection(serverAddress, channel);

					PendingRequest pending;
					while ((pending = queuedRequests.poll()) != null) {
						Future<byte[]> resultFuture = established.getKvState(
								pending.kvStateId,
								pending.serializedKeyAndNamespace);

						pending.promise.completeWith(resultFuture);
					}

					// Publish the channel for the general public
					establishedConnections.put(serverAddress, established);
					pendingConnections.remove(serverAddress);

					// Check shut down for possible race with shut down. We
					// don't want any lingering connections after shut down,
					// which can happen if we don't check this here.
					if (shutDown.get()) {
						if (establishedConnections.remove(serverAddress, established)) {
							established.close();
						}
					}
				}
			}
		}

		/**
		 * Close the connecting channel with a ClosedChannelException.
		 */
		private void close() {
			close(new ClosedChannelException());
		}

		/**
		 * Close the connecting channel with an Exception (can be
		 * <code>null</code>) or forward to the established channel.
		 */
		private void close(Throwable cause) {
			synchronized (connectLock) {
				if (!closed) {
					if (failureCause == null) {
						failureCause = cause;
					}

					if (established != null) {
						established.close();
					} else {
						PendingRequest pending;
						while ((pending = queuedRequests.poll()) != null) {
							pending.promise.tryFailure(cause);
						}
					}

					closed = true;
				}
			}
		}

		/**
		 * A pending request queued while the channel is connecting.
		 */
		private final class PendingRequest {

			private final KvStateID kvStateId;
			private final byte[] serializedKeyAndNamespace;
			private final Promise<byte[]> promise;

			private PendingRequest(KvStateID kvStateId, byte[] serializedKeyAndNamespace) {
				this.kvStateId = kvStateId;
				this.serializedKeyAndNamespace = serializedKeyAndNamespace;
				this.promise = Futures.promise();
			}
		}

		@Override
		public String toString() {
			synchronized (connectLock) {
				return "PendingConnection{" +
						"serverAddress=" + serverAddress +
						", queuedRequests=" + queuedRequests.size() +
						", established=" + (established != null) +
						", closed=" + closed +
						'}';
			}
		}
	}

	/**
	 * An established connection that wraps the actual channel instance and is
	 * registered at the {@link KvStateClientHandler} for callbacks.
	 */
	private class EstablishedConnection implements KvStateClientHandlerCallback {

		/** Address of the server we are connected to. */
		private final KvStateServerAddress serverAddress;

		/** The actual TCP channel. */
		private final Channel channel;

		/** Pending requests keyed by request ID. */
		private final ConcurrentHashMap<Long, PromiseAndTimestamp> pendingRequests = new ConcurrentHashMap<>();

		/** Current request number used to assign unique request IDs. */
		private final AtomicLong requestCount = new AtomicLong();

		/** Reference to a failure that was reported by the channel. */
		private final AtomicReference<Throwable> failureCause = new AtomicReference<>();

		/**
		 * Creates an established connection with the given channel.
		 *
		 * @param serverAddress Address of the server connected to
		 * @param channel The actual TCP channel
		 */
		EstablishedConnection(KvStateServerAddress serverAddress, Channel channel) {
			this.serverAddress = Preconditions.checkNotNull(serverAddress, "KvStateServerAddress");
			this.channel = Preconditions.checkNotNull(channel, "Channel");

			// Add the client handler with the callback
			channel.pipeline().addLast("KvStateClientHandler", new KvStateClientHandler(this));

			stats.reportActiveConnection();
		}

		/**
		 * Close the channel with a ClosedChannelException.
		 */
		void close() {
			close(new ClosedChannelException());
		}

		/**
		 * Close the channel with a cause.
		 *
		 * @param cause The cause to close the channel with.
		 * @return Channel close future
		 */
		private boolean close(Throwable cause) {
			if (failureCause.compareAndSet(null, cause)) {
				channel.close();
				stats.reportInactiveConnection();

				for (long requestId : pendingRequests.keySet()) {
					PromiseAndTimestamp pending = pendingRequests.remove(requestId);
					if (pending != null && pending.promise.tryFailure(cause)) {
						stats.reportFailedRequest();
					}
				}

				return true;
			}

			return false;
		}

		/**
		 * Returns a future holding the serialized request result.
		 *
		 * @param kvStateId                 ID of the KvState instance to query
		 * @param serializedKeyAndNamespace Serialized key and namespace to query KvState instance
		 *                                  with
		 * @return Future holding the serialized result
		 */
		Future<byte[]> getKvState(KvStateID kvStateId, byte[] serializedKeyAndNamespace) {
			PromiseAndTimestamp requestPromiseTs = new PromiseAndTimestamp(
					Futures.<byte[]>promise(),
					System.nanoTime());

			try {
				final long requestId = requestCount.getAndIncrement();
				pendingRequests.put(requestId, requestPromiseTs);

				stats.reportRequest();

				ByteBuf buf = KvStateRequestSerializer.serializeKvStateRequest(
						channel.alloc(),
						requestId,
						kvStateId,
						serializedKeyAndNamespace);

				channel.writeAndFlush(buf).addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						if (!future.isSuccess()) {
							// Fail promise if not failed to write
							PromiseAndTimestamp pending = pendingRequests.remove(requestId);
							if (pending != null && pending.promise.tryFailure(future.cause())) {
								stats.reportFailedRequest();
							}
						}
					}
				});

				// Check failure for possible race. We don't want any lingering
				// promises after a failure, which can happen if we don't check
				// this here. Note that close is treated as a failure as well.
				Throwable failure = failureCause.get();
				if (failure != null) {
					// Remove from pending requests to guard against concurrent
					// removal and to make sure that we only count it once as failed.
					PromiseAndTimestamp p = pendingRequests.remove(requestId);
					if (p != null && p.promise.tryFailure(failure)) {
						stats.reportFailedRequest();
					}
				}
			} catch (Throwable t) {
				requestPromiseTs.promise.tryFailure(t);
			}

			return requestPromiseTs.promise.future();
		}

		@Override
		public void onRequestResult(long requestId, byte[] serializedValue) {
			PromiseAndTimestamp pending = pendingRequests.remove(requestId);
			if (pending != null && pending.promise.trySuccess(serializedValue)) {
				long durationMillis = (System.nanoTime() - pending.timestamp) / 1_000_000;
				stats.reportSuccessfulRequest(durationMillis);
			}
		}

		@Override
		public void onRequestFailure(long requestId, Throwable cause) {
			PromiseAndTimestamp pending = pendingRequests.remove(requestId);
			if (pending != null && pending.promise.tryFailure(cause)) {
				stats.reportFailedRequest();
			}
		}

		@Override
		public void onFailure(Throwable cause) {
			if (close(cause)) {
				// Remove from established channels, otherwise future
				// requests will be handled by this failed channel.
				establishedConnections.remove(serverAddress, this);
			}
		}

		@Override
		public String toString() {
			return "EstablishedConnection{" +
					"serverAddress=" + serverAddress +
					", channel=" + channel +
					", pendingRequests=" + pendingRequests.size() +
					", requestCount=" + requestCount +
					", failureCause=" + failureCause +
					'}';
		}

		/**
		 * Pair of promise and a timestamp.
		 */
		private class PromiseAndTimestamp {

			private final Promise<byte[]> promise;
			private final long timestamp;

			public PromiseAndTimestamp(Promise<byte[]> promise, long timestamp) {
				this.promise = promise;
				this.timestamp = timestamp;
			}
		}

	}

}
