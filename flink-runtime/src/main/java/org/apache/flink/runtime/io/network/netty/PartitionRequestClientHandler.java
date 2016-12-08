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

import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.netty.exception.TransportException;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel.CapacityAvailabilityListener;
import org.apache.flink.runtime.util.event.EventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkState;

class PartitionRequestClientHandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestClientHandler.class);

	private final ConcurrentMap<InputChannelID, RemoteInputChannel> inputChannels = new ConcurrentHashMap<>();

	private final AtomicBoolean channelError = new AtomicBoolean(false);

	private final BufferListenerTask bufferListener = new BufferListenerTask();

	private final ArrayDeque<Object> stagedMessages = new ArrayDeque<Object>();

	private final StagedMessagesHandlerTask stagedMessagesHandler = new StagedMessagesHandlerTask();

	/**
	 * Set of cancelled partition requests. A request is cancelled iff an input channel is cleared
	 * while data is still coming in for this channel.
	 */
	private final ConcurrentMap<InputChannelID, InputChannelID> cancelled = Maps.newConcurrentMap();

	private volatile ChannelHandlerContext ctx;

	// ------------------------------------------------------------------------
	// Input channel/receiver registration
	// ------------------------------------------------------------------------

	void addInputChannel(RemoteInputChannel listener) {
		checkState(!channelError.get(), "There has been an error in the channel.");

		if (!inputChannels.containsKey(listener.getInputChannelId())) {
			inputChannels.put(listener.getInputChannelId(), listener);
		}
	}

	void removeInputChannel(RemoteInputChannel listener) {
		inputChannels.remove(listener.getInputChannelId());
	}

	void cancelRequestFor(InputChannelID inputChannelId) {
		if (inputChannelId == null || ctx == null) {
			return;
		}

		if (cancelled.putIfAbsent(inputChannelId, inputChannelId) == null) {
			ctx.writeAndFlush(new NettyMessage.CancelPartitionRequest(inputChannelId));
		}
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
							+ "This might indicate that the remote task manager was lost.",
					remoteAddr));
		}

		super.channelInactive(ctx);
	}

	/**
	 * Called on exceptions in the client handler pipeline.
	 *
	 * <p> Remote exceptions are received as regular payload.
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

		if (cause instanceof TransportException) {
			notifyAllChannelsOfErrorAndClose(cause);
		}
		else {
			final SocketAddress remoteAddr = ctx.channel().remoteAddress();

			final TransportException tex;

			// Improve on the connection reset by peer error message
			if (cause instanceof IOException
					&& cause.getMessage().equals("Connection reset by peer")) {

				tex = new RemoteTransportException(
						"Lost connection to task manager '" + remoteAddr + "'. This indicates "
								+ "that the remote task manager was lost.", remoteAddr, cause);
			}
			else {
				tex = new LocalTransportException(cause.getMessage(), ctx.channel().localAddress(), cause);
			}

			notifyAllChannelsOfErrorAndClose(tex);
		}
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		try {
			if (!bufferListener.hasStagedBufferOrEvent() && stagedMessages.isEmpty()) {
				decodeMsg(msg);
			}
			else {
				stagedMessages.add(msg);
			}
		}
		catch (Throwable t) {
			notifyAllChannelsOfErrorAndClose(t);
		}
	}

	private void notifyAllChannelsOfErrorAndClose(Throwable cause) {
		if (channelError.compareAndSet(false, true)) {
			try {
				for (RemoteInputChannel inputChannel : inputChannels.values()) {
					inputChannel.onError(cause);
				}
			}
			catch (Throwable t) {
				// We can only swallow the Exception at this point. :(
				LOG.warn("An Exception was thrown during error notification of a "
						+ "remote input channel.", t);
			}
			finally {
				inputChannels.clear();

				if (ctx != null) {
					ctx.close();
				}
			}
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		super.channelReadComplete(ctx);
	}

	private boolean decodeMsg(Object msg) throws Throwable {
		final Class<?> msgClazz = msg.getClass();

		// ---- Buffer --------------------------------------------------------
		if (msgClazz == NettyMessage.BufferResponse.class) {
			NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) msg;

			RemoteInputChannel inputChannel = inputChannels.get(bufferOrEvent.receiverId);
			if (inputChannel == null) {
				bufferOrEvent.releaseBuffer();

				cancelRequestFor(bufferOrEvent.receiverId);

				return true;
			}

			return decodeBufferOrEvent(inputChannel, bufferOrEvent);
		}
		// ---- Error ---------------------------------------------------------
		else if (msgClazz == NettyMessage.ErrorResponse.class) {
			NettyMessage.ErrorResponse error = (NettyMessage.ErrorResponse) msg;

			SocketAddress remoteAddr = ctx.channel().remoteAddress();

			if (error.isFatalError()) {
				notifyAllChannelsOfErrorAndClose(new RemoteTransportException(
						"Fatal error at remote task manager '" + remoteAddr + "'.",
						remoteAddr, error.cause));
			}
			else {
				RemoteInputChannel inputChannel = inputChannels.get(error.receiverId);

				if (inputChannel != null) {
					if (error.cause.getClass() == PartitionNotFoundException.class) {
						inputChannel.onFailedPartitionRequest();
					}
					else {
						inputChannel.onError(new RemoteTransportException(
								"Error at remote task manager '" + remoteAddr + "'.",
										remoteAddr, error.cause));
					}
				}
			}
		}
		else {
			throw new IllegalStateException("Received unknown message from producer: " + msg.getClass());
		}

		return true;
	}

	private boolean decodeBufferOrEvent(RemoteInputChannel inputChannel, NettyMessage.BufferResponse bufferOrEvent) throws Throwable {
		boolean releaseNettyBuffer = true;

		try {
			if (bufferOrEvent.isBuffer()) {
				// ---- Buffer ------------------------------------------------

				// Early return for empty buffers. Otherwise Netty's readBytes() throws an
				// IndexOutOfBoundsException.
				if (bufferOrEvent.getSize() == 0) {
					inputChannel.onEmptyBuffer(bufferOrEvent.sequenceNumber);
					return true;
				}

				BufferProvider bufferProvider = inputChannel.getBufferProvider();

				if (bufferProvider == null) {

					cancelRequestFor(bufferOrEvent.receiverId);

					return false; // receiver has been cancelled/failed
				}

				while (true) {
					Buffer buffer = bufferProvider.requestBuffer();

					if (buffer != null) {
						buffer.setSize(bufferOrEvent.getSize());
						bufferOrEvent.getNettyBuffer().readBytes(buffer.getNioBuffer());

						if (inputChannel.onBuffer(buffer, bufferOrEvent.sequenceNumber, bufferListener)) {
							return true;
						} else {
							// could not queue the buffer - remember it and wait for the availability callback
							bufferListener.stageAllocatedBuffer(inputChannel, buffer, bufferOrEvent.sequenceNumber);
							ctx.channel().config().setAutoRead(false);
							return false;
						}
					}
					else if (bufferListener.registerBufferAvailabilityCallback(bufferProvider, bufferOrEvent)) {
						releaseNettyBuffer = false;

						return false;
					}
					else if (bufferProvider.isDestroyed()) {
						return false;
					}
				}
			}
			else {
				// ---- Event -------------------------------------------------
				// TODO We can just keep the serialized data in the Netty buffer and release it later at the reader
				byte[] byteArray = new byte[bufferOrEvent.getSize()];
				bufferOrEvent.getNettyBuffer().readBytes(byteArray);

				MemorySegment memSeg = MemorySegmentFactory.wrap(byteArray);
				Buffer buffer = new Buffer(memSeg, FreeingBufferRecycler.INSTANCE, false);

				// since this is an event, it can never be rejected under backpressure
				if (!inputChannel.onBuffer(buffer, bufferOrEvent.sequenceNumber, null)) {
					throw new IOException("Error in input channel: Event could not be queued");
				}

				return true;
			}
		}
		finally {
			if (releaseNettyBuffer) {
				bufferOrEvent.releaseBuffer();
			}
		}
	}

	private class AsyncErrorNotificationTask implements Runnable {

		private final Throwable error;

		public AsyncErrorNotificationTask(Throwable error) {
			this.error = error;
		}

		@Override
		public void run() {
			notifyAllChannelsOfErrorAndClose(error);
		}
	}

	/**
	 * A buffer availability listener, which subscribes/unsubscribes the NIO
	 * read event.
	 * <p>
	 * If no buffer is available, the channel read event will be unsubscribed
	 * until one becomes available again.
	 * <p>
	 * After a buffer becomes available again, the buffer is handed over by
	 * the thread calling {@link #onEvent(Buffer)} to the network I/O
	 * thread, which then continues the processing of the staged buffer.
	 */
	private class BufferListenerTask implements EventListener<Buffer>, Runnable, CapacityAvailabilityListener {

		private final AtomicReference<Buffer> availableBuffer = new AtomicReference<Buffer>();

		// a staged response because no buffer was available in the buffer pool
		private NettyMessage.BufferResponse stagedBufferResponse;

		// staged buffer because the input channels individual capacity was exceeded
		private RemoteInputChannel stagedChannel;
		private Buffer stagedBuffer;
		private int stagedSequenceNumber;

		/**
		 * This tries to install a "callback on buffer availability".
		 */
		boolean registerBufferAvailabilityCallback(BufferProvider bufferProvider, NettyMessage.BufferResponse bufferResponse) {
			if (bufferProvider.addListener(this)) {
				if (ctx.channel().config().isAutoRead()) {
					ctx.channel().config().setAutoRead(false);
				}

				stagedBufferResponse = bufferResponse;
				return true;
			}
			else {
				return false;
			}
		}

		void stageAllocatedBuffer(RemoteInputChannel channel, Buffer buffer, int sequenceNumber) {
			this.stagedChannel = channel;
			this.stagedBuffer = buffer;
			this.stagedSequenceNumber = sequenceNumber;
		}

		boolean hasStagedBufferOrEvent() {
			return stagedBufferResponse != null | stagedBuffer != null;
		}

		// Called by the recycling thread (not network I/O thread)
		@Override
		public void onEvent(Buffer buffer) {
			boolean success = false;

			try {
				if (buffer != null) {
					if (availableBuffer.compareAndSet(null, buffer)) {
						ctx.channel().eventLoop().execute(this);

						success = true;
					}
					else {
						throw new IllegalStateException("Received a buffer notification, " +
								" but the previous one has not been handled yet.");
					}
				}
				else {
					// The buffer pool has been destroyed
					stagedBufferResponse = null;

					if (stagedMessages.isEmpty()) {
						ctx.channel().config().setAutoRead(true);
						ctx.channel().read();
					}
					else {
						ctx.channel().eventLoop().execute(stagedMessagesHandler);
					}
				}
			}
			catch (Throwable t) {
				ctx.channel().eventLoop().execute(new AsyncErrorNotificationTask(t));
			}
			finally {
				if (!success) {
					if (buffer != null) {
						buffer.recycle();
					}
				}
			}
		}

		// this is called by the receiving task's thread
		@Override
		public void capacityAvailable() {
			// queue this back in
			ctx.channel().eventLoop().execute(this);
		}

		/**
		 * Continues the decoding of a staged buffers. Called either on availability of capacity,
		 * or after a buffer has become available again.
		 * 
		 * <p>This task is executed by the network I/O thread.
		 */
		@Override
		public void run() {
			try {
				boolean handledStagedMessage = (stagedBuffer == null) ?
						replayStagedNettyMessage() :
						replayCapacityBufferedMessage();


				if (handledStagedMessage) {
					// if we handled our staged message, continue with the next staged messages
					// or resume reading from the network
					if (stagedMessages.isEmpty()) {
						ctx.channel().config().setAutoRead(true);
						ctx.channel().read();
					}
					else {
						ctx.channel().eventLoop().execute(stagedMessagesHandler);
					}
				}
			}
			catch (Throwable t) {
				notifyAllChannelsOfErrorAndClose(t);
			}
		}

		private boolean replayCapacityBufferedMessage() {
			if (stagedChannel.onBuffer(stagedBuffer, stagedSequenceNumber, this)) {
				// managed to add the buffer
				stagedBuffer = null;
				stagedSequenceNumber = -1;
				stagedChannel = null;
				return true;
			}
			else {
				// again did not manage to add the buffer and installed a callback again
				return false;
			}
		}

		private boolean replayStagedNettyMessage() {
			Buffer buffer = null;
			boolean releaseBuffer = true;

			try {
				if ((buffer = availableBuffer.getAndSet(null)) == null) {
					throw new IllegalStateException("Running buffer availability task w/o a buffer.");
				}

				final NettyMessage.BufferResponse stagedBufferResponse = this.stagedBufferResponse;
				this.stagedBufferResponse = null;

				buffer.setSize(stagedBufferResponse.getSize());
				stagedBufferResponse.getNettyBuffer().readBytes(buffer.getNioBuffer());
				stagedBufferResponse.releaseBuffer();

				RemoteInputChannel inputChannel = inputChannels.get(stagedBufferResponse.receiverId);

				if (inputChannel != null) {
					if (inputChannel.onBuffer(buffer, stagedBufferResponse.sequenceNumber, this)) {
						// we handled the staged request
						releaseBuffer = false;
						return true;
					} else {
						// we could not handle the request, it is queued again
						this.stagedBuffer = buffer;
						this.stagedSequenceNumber = stagedBufferResponse.sequenceNumber;
						this.stagedChannel = inputChannel;

						releaseBuffer = false;
						return false;
					}

					
				}
				else {
					cancelRequestFor(stagedBufferResponse.receiverId);
					// we handled the staged request, even if it resulted in a cancellation
					return true;
				}
			}
			finally {
				if (releaseBuffer && buffer != null) {
					buffer.recycle();
				}
			}
		}
	}

	public class StagedMessagesHandlerTask implements Runnable {

		@Override
		public void run() {
			try {
				Object msg;
				while ((msg = stagedMessages.poll()) != null) {
					if (!decodeMsg(msg)) {
						return;
					}
				}

				ctx.channel().config().setAutoRead(true);
				ctx.channel().read();
			}
			catch (Throwable t) {
				notifyAllChannelsOfErrorAndClose(t);
			}
		}
	}
}
