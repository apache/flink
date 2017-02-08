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

import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;

/**
 * A nonEmptyReader of partition queues, which listens for channel writability changed
 * events before writing and flushing {@link Buffer} instances.
 */
class PartitionRequestQueue extends ChannelInboundHandlerAdapter {

	private final Logger LOG = LoggerFactory.getLogger(PartitionRequestQueue.class);

	private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

	private final Queue<SequenceNumberingViewReader> nonEmptyReader = new ArrayDeque<>();

	private final Set<InputChannelID> released = Sets.newHashSet();

	private boolean fatalError;

	private ChannelHandlerContext ctx;

	@Override
	public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
		if (this.ctx == null) {
			this.ctx = ctx;
		}

		super.channelRegistered(ctx);
	}

	void notifyReaderNonEmpty(final SequenceNumberingViewReader reader) {
		// The notification might come from the same thread. For the initial writes this
		// might happen before the reader has set its reference to the view, because
		// creating the queue and the initial notification happen in the same method call.
		// This can be resolved by separating the creation of the view and allowing
		// notifications.

		// TODO This could potentially have a bad performance impact as in the
		// worst case (network consumes faster than the producer) each buffer
		// will trigger a separate event loop task being scheduled.
		ctx.executor().execute(new Runnable() {
			@Override
			public void run() {
				ctx.pipeline().fireUserEventTriggered(reader);
			}
		});
	}

	public void cancel(InputChannelID receiverId) {
		ctx.pipeline().fireUserEventTriggered(receiverId);
	}

	public void close() {
		if (ctx != null) {
			ctx.channel().close();
		}
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		// The user event triggered event loop callback is used for thread-safe
		// hand over of reader queues and cancelled producers.

		if (msg.getClass() == SequenceNumberingViewReader.class) {
			// Queue a non-empty reader for consumption. If the queue
			// is empty, we try trigger the actual write. Otherwise this
			// will be handled by the writeAndFlushIfPossible calls.
			boolean triggerWrite = nonEmptyReader.isEmpty();
			nonEmptyReader.add((SequenceNumberingViewReader) msg);
			if (triggerWrite) {
				writeAndFlushNextMessageIfPossible(ctx.channel());
			}
		} else if (msg.getClass() == InputChannelID.class) {
			// Release partition view that get a cancel request.
			InputChannelID toCancel = (InputChannelID) msg;
			if (released.contains(toCancel)) {
				return;
			}

			// Cancel the request for the input channel
			int size = nonEmptyReader.size();
			for (int i = 0; i < size; i++) {
				SequenceNumberingViewReader reader = nonEmptyReader.poll();
				if (reader.getReceiverId().equals(toCancel)) {
					reader.releaseAllResources();
					markAsReleased(reader.getReceiverId());
				} else {
					nonEmptyReader.add(reader);
				}
			}
		} else {
			ctx.fireUserEventTriggered(msg);
		}
	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		writeAndFlushNextMessageIfPossible(ctx.channel());
	}

	private void writeAndFlushNextMessageIfPossible(final Channel channel) throws IOException {
		if (fatalError) {
			return;
		}

		// The logic here is very similar to the combined input gate and local
		// input channel logic. You can think of this class acting as the input
		// gate and the consumed views as the local input channels.

		BufferAndAvailability next = null;
		try {
			if (channel.isWritable()) {
				while (true) {
					SequenceNumberingViewReader reader = nonEmptyReader.poll();

					// No queue with available data. We allow this here, because
					// of the write callbacks that are executed after each write.
					if (reader == null) {
						return;
					}

					next = reader.getNextBuffer();

					if (next == null) {
						if (reader.isReleased()) {
							markAsReleased(reader.getReceiverId());
							Throwable cause = reader.getFailureCause();

							if (cause != null) {
								ErrorResponse msg = new ErrorResponse(
									new ProducerFailedException(cause),
									reader.getReceiverId());

								ctx.writeAndFlush(msg);
							}
						} else {
							IllegalStateException err = new IllegalStateException(
								"Bug in Netty consumer logic: reader queue got notified by partition " +
									"about available data, but none was available.");
							handleException(ctx.channel(), err);
							return;
						}
					} else {
						// this channel was now removed from the non-empty reader queue
						// we re-add it in case it has more data, because in that case no
						// "non-empty" notification will come for that reader from the queue.
						if (next.moreAvailable()) {
							nonEmptyReader.add(reader);
						}

						BufferResponse msg = new BufferResponse(
							next.buffer(),
							reader.getSequenceNumber(),
							reader.getReceiverId());

						if (isEndOfPartitionEvent(next.buffer())) {
							reader.notifySubpartitionConsumed();
							reader.releaseAllResources();

							markAsReleased(reader.getReceiverId());
						}

						// Write and flush and wait until this is done before
						// trying to continue with the next buffer.
						channel.writeAndFlush(msg).addListener(writeListener);

						return;
					}
				}
			}
		} catch (Throwable t) {
			if (next != null) {
				next.buffer().recycle();
			}

			throw new IOException(t.getMessage(), t);
		}
	}

	private boolean isEndOfPartitionEvent(Buffer buffer) throws IOException {
		return EventSerializer.isEvent(buffer, EndOfPartitionEvent.class,
			getClass().getClassLoader());
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		releaseAllResources();

		ctx.fireChannelInactive();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		handleException(ctx.channel(), cause);
	}

	private void handleException(Channel channel, Throwable cause) throws IOException {
		LOG.debug("Encountered error while consuming partitions", cause);

		fatalError = true;
		releaseAllResources();

		if (channel.isActive()) {
			channel.writeAndFlush(new ErrorResponse(cause)).addListener(ChannelFutureListener.CLOSE);
		}
	}

	private void releaseAllResources() throws IOException {
		SequenceNumberingViewReader reader;
		while ((reader = nonEmptyReader.poll()) != null) {
			reader.releaseAllResources();
			markAsReleased(reader.getReceiverId());
		}
	}

	/**
	 * Marks a receiver as released.
	 */
	private void markAsReleased(InputChannelID receiverId) {
		released.add(receiverId);
	}

	// This listener is called after an element of the current nonEmptyReader has been
	// flushed. If successful, the listener triggers further processing of the
	// queues.
	private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			try {
				if (future.isSuccess()) {
					writeAndFlushNextMessageIfPossible(future.channel());
				} else if (future.cause() != null) {
					handleException(future.channel(), future.cause());
				} else {
					handleException(future.channel(), new IllegalStateException("Sending cancelled by user."));
				}
			} catch (Throwable t) {
				handleException(future.channel(), t);
			}
		}
	}
}
