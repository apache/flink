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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.WebSocketSpecification;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Routed;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

/**
 * A channel handler for WebSocket resources.
 *
 * <p>This handler handles handshaking and ongoing messaging with a WebSocket client,
 * based on a {@link WebSocketSpecification} that describes the REST resource location,
 * parameter type, and message inbound/outbound types.  Messages are automatically
 * encoded from (and decoded to) JSON text.
 *
 * <p>Subclasses should override the following methods to extend the respective phases.
 * <ol>
 *     <li>{@code handshakeInitiated} - occurs upon receipt of a handshake request from an HTTP client.  Useful for parameter validation.</li>
 *     <li>{@code handshakeCompleted} - occurs upon successful completion; WebSocket is ready for I/O.</li>
 *     <li>{@code messageReceived} - occurs when a WebSocket message is received on the channel.</li>
 * </ol>
 *
 * <p>The handler supports gateway availability announcements.
 *
 * @param <T> The gateway type.
 * @param <M> The REST parameter type.
 * @param <I> The inbound message type.
 * @param <O> The outbound message type.
 */
public abstract class AbstractWebSocketHandler<T extends RestfulGateway, M extends MessageParameters, I extends RequestBody, O extends ResponseBody> extends ChannelInboundHandlerAdapter {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private final RedirectHandler redirectHandler;

	private final AttributeKey<T> gatewayAttr;

	private final WebSocketSpecification<M, I, O> specification;

	private final ChannelHandler messageCodec;

	private final AttributeKey<M> parametersAttr;

	/**
	 * Creates a new handler.
	 */
	public AbstractWebSocketHandler(
		@Nonnull CompletableFuture<String> localAddressFuture,
		@Nonnull GatewayRetriever<? extends T> leaderRetriever,
		@Nonnull Time timeout,
		@Nonnull WebSocketSpecification<M, I, O> specification) {
		this.redirectHandler = new RedirectHandler<>(localAddressFuture, leaderRetriever, timeout);
		this.gatewayAttr = AttributeKey.valueOf("gateway");
		this.specification = specification;
		this.messageCodec = new JsonWebSocketMessageCodec<>(specification.getClientClass(), specification.getServerClass());
		this.parametersAttr = AttributeKey.valueOf("parameters");
	}

	/**
	 * Sets the gateway associated with the channel.
	 */
	private void setGateway(ChannelHandlerContext ctx, T gateway) {
		ctx.attr(gatewayAttr).set(gateway);
	}

	/**
	 * Returns the gateway associated with the channel.
	 */
	public T getGateway(ChannelHandlerContext ctx) {
		T t = ctx.attr(gatewayAttr).get();
		Preconditions.checkState(t != null, "Gateway is not available.");
		return t;
	}

	/**
	 * Sets the resource parameters associated with the channel.
	 *
	 * <p>The parameters are established by the WebSocket handshake request.
	 */
	private void setMessageParameters(ChannelHandlerContext ctx, M parameters) {
		ctx.attr(parametersAttr).set(parameters);
	}

	/**
	 * Returns the resource parameters associated with the channel.
	 */
	public M getMessageParameters(ChannelHandlerContext ctx) {
		M o = ctx.attr(parametersAttr).get();
		Preconditions.checkState(o != null, "Message parameters are not available.");
		return o;
	}

	@Override
	public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
		if (ctx.pipeline().get(RedirectHandler.class.getName()) == null) {
			ctx.pipeline().addBefore(ctx.name(), RedirectHandler.class.getName(), redirectHandler);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof RedirectHandler.GatewayRetrieved) {
			T gateway = ((RedirectHandler.GatewayRetrieved<T>) evt).getGateway();
			setGateway(ctx, gateway);
			log.debug("Gateway retrieved: {}", gateway);
		}
		else if (evt instanceof WebSocketServerProtocolHandler.ServerHandshakeStateEvent) {
			WebSocketServerProtocolHandler.ServerHandshakeStateEvent handshakeEvent = (WebSocketServerProtocolHandler.ServerHandshakeStateEvent) evt;
			if (handshakeEvent == WebSocketServerProtocolHandler.ServerHandshakeStateEvent.HANDSHAKE_COMPLETE) {
				log.debug("Handshake completed with client IP: {}", ctx.channel().remoteAddress());
				M parameters = getMessageParameters(ctx);
				handshakeCompleted(ctx, parameters);
			}
		}

		super.userEventTriggered(ctx, evt);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void channelRead(ChannelHandlerContext ctx, Object o) throws Exception {
		if (specification.getClientClass().isAssignableFrom(o.getClass())) {
			// process an inbound message
			M parameters = getMessageParameters(ctx);
			try {
				messageReceived(ctx, parameters, (I) o);
			}
			finally {
				ReferenceCountUtil.release(o);
			}
			return;
		}

		if (!(o instanceof Routed)) {
			// a foreign message
			ctx.fireChannelRead(o);
			return;
		}

		// process an inbound HTTP request
		Routed request = (Routed) o;

		// parse the REST request parameters
		M messageParameters = specification.getUnresolvedMessageParameters();
		try {
			messageParameters.resolveParameters(request.pathParams(), request.queryParams());
			if (!messageParameters.isResolved()) {
				throw new IllegalArgumentException("One or more mandatory parameters is missing.");
			}
		}
		catch (IllegalArgumentException e) {
			HandlerUtils.sendErrorResponse(
				ctx,
				request.request(),
				new ErrorResponseBody(String.format("Bad request, could not parse parameters: %s", e.getMessage())),
				HttpResponseStatus.BAD_REQUEST);
			ReferenceCountUtil.release(request);
			return;
		}
		setMessageParameters(ctx, messageParameters);

		// validate the inbound handshake request with the subclass
		CompletableFuture<Void> handshakeReady;
		try {
			handshakeReady = handshakeInitiated(ctx, messageParameters);
		} catch (Exception e) {
			handshakeReady = FutureUtils.completedExceptionally(e);
		}
		handshakeReady.whenCompleteAsync((Void v, Throwable throwable) -> {
			try {
				if (throwable != null) {
					Throwable error = ExceptionUtils.stripCompletionException(throwable);
					if (error instanceof RestHandlerException) {
						final RestHandlerException rhe = (RestHandlerException) error;
						log.error("Exception occurred in REST handler.", error);
						HandlerUtils.sendErrorResponse(ctx, request.request(), new ErrorResponseBody(rhe.getMessage()), rhe.getHttpResponseStatus());
					} else {
						log.error("Implementation error: Unhandled exception.", error);
						HandlerUtils.sendErrorResponse(ctx, request.request(), new ErrorResponseBody("Internal server error."), HttpResponseStatus.INTERNAL_SERVER_ERROR);
					}
				} else {
					upgradeToWebSocket(ctx, request);
				}
			}
			finally {
				ReferenceCountUtil.release(request);
			}
		}, ctx.executor());
	}

	private void upgradeToWebSocket(final ChannelHandlerContext ctx, Routed msg) {

		// store the context of the handler that precedes the current handler,
		// to use that context later to forward the HTTP request to the WebSocket protocol handler
		String before = ctx.pipeline().names().get(ctx.pipeline().names().indexOf(ctx.name()) - 1);
		ChannelHandlerContext beforeCtx = ctx.pipeline().context(before);

		// inject the websocket protocol handler into this channel, to be active
		// until the channel is closed.  note that the handshake may or may not complete synchronously.
		ctx.pipeline().addBefore(ctx.name(), WebSocketServerProtocolHandler.class.getName(),
			new WebSocketServerProtocolHandler(msg.path(), specification.getSubprotocol()));

		// inject the message codec
		ctx.pipeline().addBefore(ctx.name(), messageCodec.getClass().getName(), messageCodec);

		log.debug("Upgraded channel with WS protocol handler and message codec.");

		// forward the message to the installed protocol handler to initiate handshaking
		HttpRequest request = msg.request();
		ReferenceCountUtil.retain(request);
		beforeCtx.fireChannelRead(request);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		log.error("WebSocket channel error; closing the channel.", cause);
		ctx.close();
	}

	/**
	 * Handles a client handshake request to open a WebSocket resource.  Returns a {@link CompletableFuture} to complete handshaking.
	 *
	 * <p>Implementations may decide whether to throw {@link RestHandlerException}s or fail the returned
	 * {@link CompletableFuture} with a {@link RestHandlerException}.
	 *
	 * <p>Failing the future with another exception type or throwing unchecked exceptions is regarded as an
	 * implementation error as it does not allow us to provide a meaningful HTTP status code. In this case a
	 * {@link HttpResponseStatus#INTERNAL_SERVER_ERROR} will be returned.
	 *
	 * @param parameters the REST parameters

	 * @return future indicating completion of handshake pre-processing.
	 * @throws RestHandlerException to produce a pre-formatted HTTP error response.
	 */
	protected CompletableFuture<Void> handshakeInitiated(ChannelHandlerContext ctx, M parameters) throws Exception {
		return CompletableFuture.completedFuture(null);
	}

	/**
	 * Invoked when the current channel has completed the handshaking to establish a WebSocket connection.
	 *
	 * @param ctx the channel handler context
	 * @param parameters the REST parameters
	 * @throws Exception if processing failed.
	 */
	protected void handshakeCompleted(ChannelHandlerContext ctx, M parameters) throws Exception {
	}

	/**
	 * Invoked when the current channel has received a WebSocket message.
	 *
	 * <p>The message object is automatically released after this method is called.
	 *
	 * @param ctx the channel handler context
	 * @param parameters the REST parameters
	 * @param msg the message received
	 * @throws Exception if the message could not be processed.
	 */
	protected abstract void messageReceived(ChannelHandlerContext ctx, M parameters, I msg) throws Exception;
}
