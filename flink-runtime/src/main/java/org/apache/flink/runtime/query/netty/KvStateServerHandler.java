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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.netty.message.KvStateRequest;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.query.netty.message.KvStateRequestType;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * This handler dispatches asynchronous tasks, which query {@link KvState}
 * instances and write the result to the channel.
 *
 * <p>The network threads receive the message, deserialize it and dispatch the
 * query task. The actual query is handled in a separate thread as it might
 * otherwise block the network threads (file I/O etc.).
 */
@ChannelHandler.Sharable
class KvStateServerHandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(KvStateServerHandler.class);

	/** KvState registry holding references to the KvState instances. */
	private final KvStateRegistry registry;

	/** Thread pool for query execution. */
	private final ExecutorService queryExecutor;

	/** Exposed server statistics. */
	private final KvStateRequestStats stats;

	/**
	 * Create the handler.
	 *
	 * @param kvStateRegistry Registry to query.
	 * @param queryExecutor   Thread pool for query execution.
	 * @param stats           Exposed server statistics.
	 */
	KvStateServerHandler(
			KvStateRegistry kvStateRegistry,
			ExecutorService queryExecutor,
			KvStateRequestStats stats) {

		this.registry = Objects.requireNonNull(kvStateRegistry, "KvStateRegistry");
		this.queryExecutor = Objects.requireNonNull(queryExecutor, "Query thread pool");
		this.stats = Objects.requireNonNull(stats, "KvStateRequestStats");
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		stats.reportActiveConnection();
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		stats.reportInactiveConnection();
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		KvStateRequest request = null;

		try {
			ByteBuf buf = (ByteBuf) msg;
			KvStateRequestType msgType = KvStateRequestSerializer.deserializeHeader(buf);

			if (msgType == KvStateRequestType.REQUEST) {
				// ------------------------------------------------------------
				// Request
				// ------------------------------------------------------------
				request = KvStateRequestSerializer.deserializeKvStateRequest(buf);

				stats.reportRequest();

				KvState<?, ?, ?, ?, ?> kvState = registry.getKvState(request.getKvStateId());

				if (kvState != null) {
					// Execute actual query async, because it is possibly
					// blocking (e.g. file I/O).
					//
					// A submission failure is not treated as fatal.
					queryExecutor.submit(new AsyncKvStateQueryTask(ctx, request, kvState, stats));
				} else {
					ByteBuf unknown = KvStateRequestSerializer.serializeKvStateRequestFailure(
							ctx.alloc(),
							request.getRequestId(),
							new UnknownKvStateID(request.getKvStateId()));

					ctx.writeAndFlush(unknown);

					stats.reportFailedRequest();
				}
			} else {
				// ------------------------------------------------------------
				// Unexpected
				// ------------------------------------------------------------
				ByteBuf failure = KvStateRequestSerializer.serializeServerFailure(
						ctx.alloc(),
						new IllegalArgumentException("Unexpected message type " + msgType
								+ ". KvStateServerHandler expects "
								+ KvStateRequestType.REQUEST + " messages."));

				ctx.writeAndFlush(failure);
			}
		} catch (Throwable t) {
			String stringifiedCause = ExceptionUtils.stringifyException(t);

			ByteBuf err;
			if (request != null) {
				String errMsg = "Failed to handle incoming request with ID " +
						request.getRequestId() + ". Caused by: " + stringifiedCause;
				err = KvStateRequestSerializer.serializeKvStateRequestFailure(
						ctx.alloc(),
						request.getRequestId(),
						new RuntimeException(errMsg));

				stats.reportFailedRequest();
			} else {
				String errMsg = "Failed to handle incoming message. Caused by: " + stringifiedCause;
				err = KvStateRequestSerializer.serializeServerFailure(
						ctx.alloc(),
						new RuntimeException(errMsg));
			}

			ctx.writeAndFlush(err);
		} finally {
			// IMPORTANT: We have to always recycle the incoming buffer.
			// Otherwise we will leak memory out of Netty's buffer pool.
			//
			// If any operation ever holds on to the buffer, it is the
			// responsibility of that operation to retain the buffer and
			// release it later.
			ReferenceCountUtil.release(msg);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		String stringifiedCause = ExceptionUtils.stringifyException(cause);
		String msg = "Exception in server pipeline. Caused by: " + stringifiedCause;

		ByteBuf err = KvStateRequestSerializer.serializeServerFailure(
				ctx.alloc(),
				new RuntimeException(msg));

		ctx.writeAndFlush(err).addListener(ChannelFutureListener.CLOSE);
	}

	/**
	 * Task to execute the actual query against the {@link KvState} instance.
	 */
	private static class AsyncKvStateQueryTask implements Runnable {

		private final ChannelHandlerContext ctx;

		private final KvStateRequest request;

		private final KvState<?, ?, ?, ?, ?> kvState;

		private final KvStateRequestStats stats;

		private final long creationNanos;

		public AsyncKvStateQueryTask(
				ChannelHandlerContext ctx,
				KvStateRequest request,
				KvState<?, ?, ?, ?, ?> kvState,
				KvStateRequestStats stats) {

			this.ctx = Objects.requireNonNull(ctx, "Channel handler context");
			this.request = Objects.requireNonNull(request, "State query");
			this.kvState = Objects.requireNonNull(kvState, "KvState");
			this.stats = Objects.requireNonNull(stats, "State query stats");
			this.creationNanos = System.nanoTime();
		}

		@Override
		public void run() {
			boolean success = false;

			try {
				if (!ctx.channel().isActive()) {
					return;
				}

				// Query the KvState instance
				byte[] serializedKeyAndNamespace = request.getSerializedKeyAndNamespace();
				byte[] serializedResult = kvState.getSerializedValue(serializedKeyAndNamespace);

				if (serializedResult != null) {
					// We found some data, success!
					ByteBuf buf = KvStateRequestSerializer.serializeKvStateRequestResult(
							ctx.alloc(),
							request.getRequestId(),
							serializedResult);

					int highWatermark = ctx.channel().config().getWriteBufferHighWaterMark();

					ChannelFuture write;
					if (buf.readableBytes() <= highWatermark) {
						write = ctx.writeAndFlush(buf);
					} else {
						write = ctx.writeAndFlush(new ChunkedByteBuf(buf, highWatermark));
					}

					write.addListener(new QueryResultWriteListener());

					success = true;
				} else {
					// No data for the key/namespace. This is considered to be
					// a failure.
					ByteBuf unknownKey = KvStateRequestSerializer.serializeKvStateRequestFailure(
							ctx.alloc(),
							request.getRequestId(),
							new UnknownKeyOrNamespace());

					ctx.writeAndFlush(unknownKey);
				}
			} catch (Throwable t) {
				try {
					String stringifiedCause = ExceptionUtils.stringifyException(t);
					String errMsg = "Failed to query state backend for query " +
							request.getRequestId() + ". Caused by: " + stringifiedCause;

					ByteBuf err = KvStateRequestSerializer.serializeKvStateRequestFailure(
							ctx.alloc(), request.getRequestId(), new RuntimeException(errMsg));

					ctx.writeAndFlush(err);
				} catch (IOException e) {
					LOG.error("Failed to respond with the error after failed to query state backend", e);
				}
			} finally {
				if (!success) {
					stats.reportFailedRequest();
				}
			}
		}

		@Override
		public String toString() {
			return "AsyncKvStateQueryTask{" +
					", request=" + request +
					", creationNanos=" + creationNanos +
					'}';
		}

		/**
		 * Callback after query result has been written.
		 *
		 * <p>Gathers stats and logs errors.
		 */
		private class QueryResultWriteListener implements ChannelFutureListener {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				long durationMillis = (System.nanoTime() - creationNanos) / 1_000_000;

				if (future.isSuccess()) {
					stats.reportSuccessfulRequest(durationMillis);
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Query " + request + " failed after " + durationMillis + " ms", future.cause());
					}

					stats.reportFailedRequest();
				}
			}
		}
	}
}
