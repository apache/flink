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
import org.apache.flink.queryablestate.network.messages.MessageBody;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.messages.MessageType;
import org.apache.flink.queryablestate.network.stats.KvStateRequestStats;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The base class of every handler used by an {@link AbstractServerBase}.
 *
 * @param <REQ> the type of request the server expects to receive.
 * @param <RESP> the type of response the server will send.
 */
@Internal
@ChannelHandler.Sharable
public abstract class AbstractServerHandler<REQ extends MessageBody, RESP extends MessageBody> extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractServerHandler.class);

	private static final long UNKNOWN_REQUEST_ID = -1;

	/** The owning server of this handler. */
	private final AbstractServerBase<REQ, RESP> server;

	/** The serializer used to (de-)serialize messages. */
	private final MessageSerializer<REQ, RESP> serializer;

	/** Thread pool for query execution. */
	protected final ExecutorService queryExecutor;

	/** Exposed server statistics. */
	private final KvStateRequestStats stats;

	/**
	 * Create the handler.
	 *
	 * @param serializer the serializer used to (de-)serialize messages
	 * @param stats statistics collector
	 */
	public AbstractServerHandler(
			final AbstractServerBase<REQ, RESP> server,
			final MessageSerializer<REQ, RESP> serializer,
			final KvStateRequestStats stats) {

		this.server = Preconditions.checkNotNull(server);
		this.serializer = Preconditions.checkNotNull(serializer);
		this.queryExecutor = server.getQueryExecutor();
		this.stats = Preconditions.checkNotNull(stats);
	}

	protected String getServerName() {
		return server.getServerName();
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
		REQ request = null;
		long requestId = UNKNOWN_REQUEST_ID;

		try {
			final ByteBuf buf = (ByteBuf) msg;
			final MessageType msgType = MessageSerializer.deserializeHeader(buf);

			requestId = MessageSerializer.getRequestId(buf);
			LOG.trace("Handling request with ID {}", requestId);

			if (msgType == MessageType.REQUEST) {

				// ------------------------------------------------------------
				// MessageBody
				// ------------------------------------------------------------
				request = serializer.deserializeRequest(buf);
				stats.reportRequest();

				// Execute actual query async, because it is possibly
				// blocking (e.g. file I/O).
				//
				// A submission failure is not treated as fatal.
				queryExecutor.submit(new AsyncRequestTask<>(this, ctx, requestId, request, stats));

			} else {
				// ------------------------------------------------------------
				// Unexpected
				// ------------------------------------------------------------

				final String errMsg = "Unexpected message type " + msgType + ". Expected " + MessageType.REQUEST + ".";
				final ByteBuf failure = MessageSerializer.serializeServerFailure(ctx.alloc(), new IllegalArgumentException(errMsg));

				LOG.debug(errMsg);
				ctx.writeAndFlush(failure);
			}
		} catch (Throwable t) {
			LOG.error("Error while handling request with ID [{}]",
				requestId == UNKNOWN_REQUEST_ID ? "unknown" : requestId,
				t);

			final String stringifiedCause = ExceptionUtils.stringifyException(t);

			String errMsg;
			ByteBuf err;
			if (request != null) {
				errMsg = "Failed request with ID " + requestId + ". Caused by: " + stringifiedCause;
				err = MessageSerializer.serializeRequestFailure(ctx.alloc(), requestId, new RuntimeException(errMsg));
				stats.reportFailedRequest();
			} else {
				errMsg = "Failed incoming message. Caused by: " + stringifiedCause;
				err = MessageSerializer.serializeServerFailure(ctx.alloc(), new RuntimeException(errMsg));
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
		final String msg = "Exception in server pipeline. Caused by: " + ExceptionUtils.stringifyException(cause);
		final ByteBuf err = MessageSerializer.serializeServerFailure(ctx.alloc(), new RuntimeException(msg));

		LOG.debug(msg);
		ctx.writeAndFlush(err).addListener(ChannelFutureListener.CLOSE);
	}

	/**
	 * Handles an incoming request and returns a {@link CompletableFuture} containing the corresponding response.
	 *
	 * <p><b>NOTE:</b> This method is called by multiple threads.
	 *
	 * @param requestId the id of the received request to be handled.
	 * @param request the request to be handled.
	 * @return A future with the response to be forwarded to the client.
	 */
	public abstract CompletableFuture<RESP> handleRequest(final long requestId, final REQ request);

	/**
	 * Shuts down any handler-specific resources, e.g. thread pools etc and returns
	 * a {@link CompletableFuture}.
	 *
	 * <p>If an exception is thrown during the shutdown process, then that exception
	 * will be included in the returned future.
	 *
	 * @return A {@link CompletableFuture} that will be completed when the shutdown
	 * process actually finishes.
	 */
	public abstract CompletableFuture<Void> shutdown();

	/**
	 * Task to execute the actual query against the state instance.
	 */
	private static class AsyncRequestTask<REQ extends MessageBody, RESP extends MessageBody> implements Runnable {

		private final AbstractServerHandler<REQ, RESP> handler;

		private final ChannelHandlerContext ctx;

		private final long requestId;

		private final REQ request;

		private final KvStateRequestStats stats;

		private final long creationNanos;

		AsyncRequestTask(
				final AbstractServerHandler<REQ, RESP> handler,
				final ChannelHandlerContext ctx,
				final long requestId,
				final REQ request,
				final KvStateRequestStats stats) {

			this.handler = Preconditions.checkNotNull(handler);
			this.ctx = Preconditions.checkNotNull(ctx);
			this.requestId = requestId;
			this.request = Preconditions.checkNotNull(request);
			this.stats = Preconditions.checkNotNull(stats);
			this.creationNanos = System.nanoTime();
		}

		@Override
		public void run() {

			if (!ctx.channel().isActive()) {
				return;
			}

			handler.handleRequest(requestId, request).whenComplete((resp, throwable) -> {
				try {
					if (throwable != null) {
						throw throwable instanceof CompletionException
								? throwable.getCause()
								: throwable;
					}

					if (resp == null) {
						throw new BadRequestException(handler.getServerName(), "NULL returned for request with ID " + requestId + ".");
					}

					final ByteBuf serialResp = MessageSerializer.serializeResponse(ctx.alloc(), requestId, resp);

					int highWatermark = ctx.channel().config().getWriteBufferHighWaterMark();

					ChannelFuture write;
					if (serialResp.readableBytes() <= highWatermark) {
						write = ctx.writeAndFlush(serialResp);
					} else {
						write = ctx.writeAndFlush(new ChunkedByteBuf(serialResp, highWatermark));
					}
					write.addListener(new RequestWriteListener());

				} catch (BadRequestException e) {
					LOG.debug("Bad request (request ID = {})", requestId, e);
					try {
						stats.reportFailedRequest();
						final ByteBuf err = MessageSerializer.serializeRequestFailure(ctx.alloc(), requestId, e);
						ctx.writeAndFlush(err);
					} catch (IOException io) {
						LOG.error("Failed to respond with the error after failed request", io);
					}
				} catch (Throwable t) {
					LOG.error("Error while handling request with ID {}", requestId, t);
					try {
						stats.reportFailedRequest();

						final String errMsg = "Failed request " + requestId + "." + System.lineSeparator() + " Caused by: " + ExceptionUtils.stringifyException(t);
						final ByteBuf err = MessageSerializer.serializeRequestFailure(ctx.alloc(), requestId, new RuntimeException(errMsg));
						ctx.writeAndFlush(err);
					} catch (IOException io) {
						LOG.error("Failed to respond with the error after failed request", io);
					}
				}
			});
		}

		@Override
		public String toString() {
			return "AsyncRequestTask{" +
					"requestId=" + requestId +
					", request=" + request +
					'}';
		}

		/**
		 * Callback after query result has been written.
		 *
		 * <p>Gathers stats and logs errors.
		 */
		private class RequestWriteListener implements ChannelFutureListener {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				long durationNanos = System.nanoTime() - creationNanos;
				long durationMillis = TimeUnit.MILLISECONDS.convert(durationNanos, TimeUnit.NANOSECONDS);

				if (future.isSuccess()) {
					LOG.debug("Request {} was successfully answered after {} ms.", request, durationMillis);
					stats.reportSuccessfulRequest(durationMillis);
				} else {
					LOG.debug("Request {} failed after {} ms", request, durationMillis, future.cause());
					stats.reportFailedRequest();
				}
			}
		}
	}
}
