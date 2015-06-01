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
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.util.event.NotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;

/**
 * A queue of partition queues, which listens for channel writability changed
 * events before writing and flushing {@link Buffer} instances.
 */
class PartitionRequestQueue extends ChannelInboundHandlerAdapter {

	private final Logger LOG = LoggerFactory.getLogger(PartitionRequestQueue.class);

	private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

	private final Queue<SequenceNumberingSubpartitionView> queue = new ArrayDeque<SequenceNumberingSubpartitionView>();

	private final Set<InputChannelID> released = Sets.newHashSet();

	private SequenceNumberingSubpartitionView currentPartitionQueue;

	private boolean fatalError;

	private ChannelHandlerContext ctx;

	@Override
	public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
		if (this.ctx == null) {
			this.ctx = ctx;
		}

		super.channelRegistered(ctx);
	}

	public void enqueue(ResultSubpartitionView partitionQueue, InputChannelID receiverId) throws Exception {
		ctx.pipeline().fireUserEventTriggered(new SequenceNumberingSubpartitionView(partitionQueue, receiverId));
	}

	public void cancel(InputChannelID receiverId) {
		ctx.pipeline().fireUserEventTriggered(receiverId);
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg.getClass() == SequenceNumberingSubpartitionView.class) {
			boolean triggerWrite = queue.isEmpty();

			queue.add((SequenceNumberingSubpartitionView) msg);

			if (triggerWrite) {
				writeAndFlushNextMessageIfPossible(ctx.channel());
			}
		}
		else if (msg.getClass() == InputChannelID.class) {
			InputChannelID toCancel = (InputChannelID) msg;

			if (released.contains(toCancel)) {
				return;
			}

			// Cancel the request for the input channel
			if (currentPartitionQueue != null && currentPartitionQueue.getReceiverId().equals(toCancel)) {
				currentPartitionQueue.releaseAllResources();
				markAsReleased(currentPartitionQueue.receiverId);
				currentPartitionQueue = null;
			}
			else {
				int size = queue.size();

				for (int i = 0; i < size; i++) {
					SequenceNumberingSubpartitionView curr = queue.poll();

					if (curr.getReceiverId().equals(toCancel)) {
						curr.releaseAllResources();
						markAsReleased(curr.receiverId);
					}
					else {
						queue.add(curr);
					}
				}
			}
		}
		else {
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

		Buffer buffer = null;

		try {
			if (channel.isWritable()) {
				while (true) {
					if (currentPartitionQueue == null && (currentPartitionQueue = queue.poll()) == null) {
						return;
					}

					buffer = currentPartitionQueue.getNextBuffer();

					if (buffer == null) {
						if (currentPartitionQueue.registerListener(null)) {
							currentPartitionQueue = null;
						}
						else if (currentPartitionQueue.isReleased()) {
							markAsReleased(currentPartitionQueue.getReceiverId());

							Throwable cause = currentPartitionQueue.getFailureCause();

							if (cause != null) {
								ctx.writeAndFlush(new NettyMessage.ErrorResponse(
										new ProducerFailedException(cause),
										currentPartitionQueue.receiverId));
							}

							currentPartitionQueue = null;
						}
					}
					else {
						BufferResponse resp = new BufferResponse(buffer, currentPartitionQueue.getSequenceNumber(), currentPartitionQueue.getReceiverId());

						if (!buffer.isBuffer() &&
								EventSerializer.fromBuffer(buffer, getClass().getClassLoader()).getClass() == EndOfPartitionEvent.class) {

							currentPartitionQueue.notifySubpartitionConsumed();
							currentPartitionQueue.releaseAllResources();
							markAsReleased(currentPartitionQueue.getReceiverId());

							currentPartitionQueue = null;
						}

						channel.writeAndFlush(resp).addListener(writeListener);

						return;
					}
				}
			}
		}
		catch (Throwable t) {
			if (buffer != null) {
				buffer.recycle();
			}

			throw new IOException(t.getMessage(), t);
		}
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
		fatalError = true;
		releaseAllResources();

		if (channel.isActive()) {
			channel.writeAndFlush(new NettyMessage.ErrorResponse(cause)).addListener(ChannelFutureListener.CLOSE);
		}
	}

	private void releaseAllResources() throws IOException {
		if (currentPartitionQueue != null) {
			currentPartitionQueue.releaseAllResources();
			markAsReleased(currentPartitionQueue.getReceiverId());

			currentPartitionQueue = null;
		}

		while ((currentPartitionQueue = queue.poll()) != null) {
			currentPartitionQueue.releaseAllResources();

			markAsReleased(currentPartitionQueue.getReceiverId());
		}
	}

	/**
	 * Marks a receiver as released.
	 */
	private void markAsReleased(InputChannelID receiverId) {
		released.add(receiverId);
	}

	// This listener is called after an element of the current queue has been
	// flushed. If successful, the listener triggers further processing of the
	// queues.
	private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			try {
				if (future.isSuccess()) {
					writeAndFlushNextMessageIfPossible(future.channel());
				}
				else if (future.cause() != null) {
					handleException(future.channel(), future.cause());
				}
				else {
					handleException(future.channel(), new IllegalStateException("Sending cancelled by user."));
				}
			}
			catch (Throwable t) {
				handleException(future.channel(), t);
			}
		}
	}

	/**
	 * Simple wrapper for the partition queue iterator, which increments a
	 * sequence number for each returned buffer and remembers the receiver ID.
	 */
	private class SequenceNumberingSubpartitionView implements ResultSubpartitionView, NotificationListener {

		private final ResultSubpartitionView queueIterator;

		private final InputChannelID receiverId;

		private int sequenceNumber = -1;

		private SequenceNumberingSubpartitionView(ResultSubpartitionView queueIterator, InputChannelID receiverId) {
			this.queueIterator = checkNotNull(queueIterator);
			this.receiverId = checkNotNull(receiverId);
		}

		private InputChannelID getReceiverId() {
			return receiverId;
		}

		private int getSequenceNumber() {
			return sequenceNumber;
		}

		@Override
		public Buffer getNextBuffer() throws IOException, InterruptedException {
			Buffer buffer = queueIterator.getNextBuffer();

			if (buffer != null) {
				sequenceNumber++;
			}

			return buffer;
		}

		@Override
		public void notifySubpartitionConsumed() throws IOException {
			queueIterator.notifySubpartitionConsumed();
		}

		@Override
		public boolean isReleased() {
			return queueIterator.isReleased();
		}

		@Override
		public Throwable getFailureCause() {
			return queueIterator.getFailureCause();
		}

		@Override
		public boolean registerListener(NotificationListener ignored) throws IOException {
			return queueIterator.registerListener(this);
		}

		@Override
		public void releaseAllResources() throws IOException {
			queueIterator.releaseAllResources();
		}

		/**
		 * Enqueue this iterator again after a notification.
		 */
		@Override
		public void onNotification() {
			ctx.pipeline().fireUserEventTriggered(this);
		}
	}
}
