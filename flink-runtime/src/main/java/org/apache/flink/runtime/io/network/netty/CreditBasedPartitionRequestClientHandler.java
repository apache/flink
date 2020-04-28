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

import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.netty.exception.TransportException;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ResumeConsumption;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Channel handler to read the messages of buffer response or error response from the
 * producer, to write and flush the unannounced credits for the producer.
 *
 * <p>It is used in the new network credit-based mode.
 */
class CreditBasedPartitionRequestClientHandler extends ChannelInboundHandlerAdapter implements NetworkClientHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CreditBasedPartitionRequestClientHandler.class);

	/** Channels, which already requested partitions from the producers. */
	private final ConcurrentMap<InputChannelID, RemoteInputChannel> inputChannels = new ConcurrentHashMap<>();

	/** Messages to be sent to the producers (credit announcement or resume consumption request). */
	private final ArrayDeque<ClientOutboundMessage> clientOutboundMessages = new ArrayDeque<>();

	private final AtomicReference<Throwable> channelError = new AtomicReference<>();

	private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

	/**
	 * Set of cancelled partition requests. A request is cancelled iff an input channel is cleared
	 * while data is still coming in for this channel.
	 */
	private final ConcurrentMap<InputChannelID, InputChannelID> cancelled = new ConcurrentHashMap<>();

	/**
	 * The channel handler context is initialized in channel active event by netty thread, the context may also
	 * be accessed by task thread or canceler thread to cancel partition request during releasing resources.
	 */
	private volatile ChannelHandlerContext ctx;

	// ------------------------------------------------------------------------
	// Input channel/receiver registration
	// ------------------------------------------------------------------------

	@Override
	public void addInputChannel(RemoteInputChannel listener) throws IOException {
		checkError();

		inputChannels.putIfAbsent(listener.getInputChannelId(), listener);
	}

	@Override
	public void removeInputChannel(RemoteInputChannel listener) {
		inputChannels.remove(listener.getInputChannelId());
	}

	@Override
	public RemoteInputChannel getInputChannel(InputChannelID inputChannelId) {
		return inputChannels.get(inputChannelId);
	}

	@Override
	public void cancelRequestFor(InputChannelID inputChannelId) {
		if (inputChannelId == null || ctx == null) {
			return;
		}

		if (cancelled.putIfAbsent(inputChannelId, inputChannelId) == null) {
			ctx.writeAndFlush(new NettyMessage.CancelPartitionRequest(inputChannelId));
		}
	}

	@Override
	public void notifyCreditAvailable(final RemoteInputChannel inputChannel) {
		ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(new AddCreditMessage(inputChannel)));
	}

	@Override
	public void resumeConsumption(RemoteInputChannel inputChannel) {
		ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(new ResumeConsumptionMessage(inputChannel)));
	}

	// ------------------------------------------------------------------------
	// Network events
	// ------------------------------------------------------------------------

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		if (this.ctx == null) {
			this.ctx = ctx;
		}

		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// Unexpected close. In normal operation, the client closes the connection after all input
		// channels have been removed. This indicates a problem with the remote task manager.
		if (!inputChannels.isEmpty()) {
			final SocketAddress remoteAddr = ctx.channel().remoteAddress();

			notifyAllChannelsOfErrorAndClose(new RemoteTransportException(
				"Connection unexpectedly closed by remote task manager '" + remoteAddr + "'. "
					+ "This might indicate that the remote task manager was lost.", remoteAddr));
		}

		super.channelInactive(ctx);
	}

	/**
	 * Called on exceptions in the client handler pipeline.
	 *
	 * <p>Remote exceptions are received as regular payload.
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (cause instanceof TransportException) {
			notifyAllChannelsOfErrorAndClose(cause);
		} else {
			final SocketAddress remoteAddr = ctx.channel().remoteAddress();

			final TransportException tex;

			// Improve on the connection reset by peer error message
			if (cause instanceof IOException && cause.getMessage().equals("Connection reset by peer")) {
				tex = new RemoteTransportException("Lost connection to task manager '" + remoteAddr + "'. " +
					"This indicates that the remote task manager was lost.", remoteAddr, cause);
			} else {
				final SocketAddress localAddr = ctx.channel().localAddress();
				tex = new LocalTransportException(
					String.format("%s (connection to '%s')", cause.getMessage(), remoteAddr), localAddr, cause);
			}

			notifyAllChannelsOfErrorAndClose(tex);
		}
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		try {
			decodeMsg(msg);
		} catch (Throwable t) {
			notifyAllChannelsOfErrorAndClose(t);
		}
	}

	/**
	 * Triggered by notifying credit available in the client handler pipeline.
	 *
	 * <p>Enqueues the input channel and will trigger write&flush unannounced credits
	 * for this input channel if it is the first one in the queue.
	 */
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof ClientOutboundMessage) {
			boolean triggerWrite = clientOutboundMessages.isEmpty();

			clientOutboundMessages.add((ClientOutboundMessage) msg);

			if (triggerWrite) {
				writeAndFlushNextMessageIfPossible(ctx.channel());
			}
		} else {
			ctx.fireUserEventTriggered(msg);
		}
	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		writeAndFlushNextMessageIfPossible(ctx.channel());
	}

	private void notifyAllChannelsOfErrorAndClose(Throwable cause) {
		if (channelError.compareAndSet(null, cause)) {
			try {
				for (RemoteInputChannel inputChannel : inputChannels.values()) {
					inputChannel.onError(cause);
				}
			} catch (Throwable t) {
				// We can only swallow the Exception at this point. :(
				LOG.warn("An Exception was thrown during error notification of a remote input channel.", t);
			} finally {
				inputChannels.clear();
				clientOutboundMessages.clear();

				if (ctx != null) {
					ctx.close();
				}
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Checks for an error and rethrows it if one was reported.
	 */
	private void checkError() throws IOException {
		final Throwable t = channelError.get();

		if (t != null) {
			if (t instanceof IOException) {
				throw (IOException) t;
			} else {
				throw new IOException("There has been an error in the channel.", t);
			}
		}
	}

	private void decodeMsg(Object msg) throws Throwable {
		final Class<?> msgClazz = msg.getClass();

		// ---- Buffer --------------------------------------------------------
		if (msgClazz == NettyMessage.BufferResponse.class) {
			NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) msg;

			RemoteInputChannel inputChannel = inputChannels.get(bufferOrEvent.receiverId);
			if (inputChannel == null || inputChannel.isReleased()) {
				bufferOrEvent.releaseBuffer();

				cancelRequestFor(bufferOrEvent.receiverId);

				return;
			}

			decodeBufferOrEvent(inputChannel, bufferOrEvent);

		} else if (msgClazz == NettyMessage.ErrorResponse.class) {
			// ---- Error ---------------------------------------------------------
			NettyMessage.ErrorResponse error = (NettyMessage.ErrorResponse) msg;

			SocketAddress remoteAddr = ctx.channel().remoteAddress();

			if (error.isFatalError()) {
				notifyAllChannelsOfErrorAndClose(new RemoteTransportException(
					"Fatal error at remote task manager '" + remoteAddr + "'.",
					remoteAddr,
					error.cause));
			} else {
				RemoteInputChannel inputChannel = inputChannels.get(error.receiverId);

				if (inputChannel != null) {
					if (error.cause.getClass() == PartitionNotFoundException.class) {
						inputChannel.onFailedPartitionRequest();
					} else {
						inputChannel.onError(new RemoteTransportException(
							"Error at remote task manager '" + remoteAddr + "'.",
							remoteAddr,
							error.cause));
					}
				}
			}
		} else {
			throw new IllegalStateException("Received unknown message from producer: " + msg.getClass());
		}
	}

	private void decodeBufferOrEvent(RemoteInputChannel inputChannel, NettyMessage.BufferResponse bufferOrEvent) throws Throwable {
		if (bufferOrEvent.isBuffer() && bufferOrEvent.bufferSize == 0) {
			inputChannel.onEmptyBuffer(bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
		} else if (bufferOrEvent.getBuffer() != null) {
			inputChannel.onBuffer(bufferOrEvent.getBuffer(), bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
		} else {
			throw new IllegalStateException("The read buffer is null in credit-based input channel.");
		}
	}

	/**
	 * Tries to write&flush unannounced credits for the next input channel in queue.
	 *
	 * <p>This method may be called by the first input channel enqueuing, or the complete
	 * future's callback in previous input channel, or the channel writability changed event.
	 */
	private void writeAndFlushNextMessageIfPossible(Channel channel) {
		if (channelError.get() != null || !channel.isWritable()) {
			return;
		}

		while (true) {
			ClientOutboundMessage outboundMessage = clientOutboundMessages.poll();

			// The input channel may be null because of the write callbacks
			// that are executed after each write.
			if (outboundMessage == null) {
				return;
			}

			//It is no need to notify credit or resume data consumption for the released channel.
			if (!outboundMessage.inputChannel.isReleased()) {
				Object msg = outboundMessage.buildMessage();

				// Write and flush and wait until this is done before
				// trying to continue with the next input channel.
				channel.writeAndFlush(msg).addListener(writeListener);

				return;
			}
		}
	}

	private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			try {
				if (future.isSuccess()) {
					writeAndFlushNextMessageIfPossible(future.channel());
				} else if (future.cause() != null) {
					notifyAllChannelsOfErrorAndClose(future.cause());
				} else {
					notifyAllChannelsOfErrorAndClose(new IllegalStateException("Sending cancelled by user."));
				}
			} catch (Throwable t) {
				notifyAllChannelsOfErrorAndClose(t);
			}
		}
	}

	private static abstract class ClientOutboundMessage {
		protected final RemoteInputChannel inputChannel;

		ClientOutboundMessage(RemoteInputChannel inputChannel) {
			this.inputChannel = inputChannel;
		}

		abstract Object buildMessage();
	}

	private static class AddCreditMessage extends ClientOutboundMessage {

		AddCreditMessage(RemoteInputChannel inputChannel) {
			super(checkNotNull(inputChannel));
		}

		@Override
		public Object buildMessage() {
			return new AddCredit(inputChannel.getAndResetUnannouncedCredit(), inputChannel.getInputChannelId());
		}
	}

	private static class ResumeConsumptionMessage extends ClientOutboundMessage {

		ResumeConsumptionMessage(RemoteInputChannel inputChannel) {
			super(checkNotNull(inputChannel));
		}

		@Override
		Object buildMessage() {
			return new ResumeConsumption(inputChannel.getInputChannelId());
		}
	}
}
