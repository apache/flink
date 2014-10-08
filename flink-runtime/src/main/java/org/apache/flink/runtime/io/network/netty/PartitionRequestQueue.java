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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.queue.IntermediateResultPartitionQueueIterator;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.util.event.NotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;

/**
 * A queue of partition queues, which listens for channel writability changed
 * events before writing and flushing {@link Buffer} instances.
 */
class PartitionRequestQueue extends ChannelInboundHandlerAdapter {

	private final Logger LOG = LoggerFactory.getLogger(PartitionRequestQueue.class);

	private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

	private final Queue<SequenceNumberingPartitionQueueIterator> queue = new ArrayDeque<SequenceNumberingPartitionQueueIterator>();

	private SequenceNumberingPartitionQueueIterator currentPartitionQueue;

	private boolean fatalError;

	private ChannelHandlerContext ctx;

	private int numTotalSubscribeCalls;

	private AtomicInteger numOutstandingSubscribeCalls = new AtomicInteger();

	private int numConsumedPartitions;

	private AtomicInteger numEnqueueCalls = new AtomicInteger();

	private int numTotalEnqueueOperations;

	private ScheduledFuture<?> logOutputTask;

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		if (LOG.isDebugEnabled()) {
			logOutputTask = ctx.channel().eventLoop().scheduleWithFixedDelay(new DebugOutputTask(), 30, 30, TimeUnit.SECONDS);
		}

		super.channelActive(ctx);
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		if (logOutputTask != null) {
			logOutputTask.cancel(true);
		}

		super.channelUnregistered(ctx);
	}

	@Override
	public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
		if (this.ctx == null) {
			this.ctx = ctx;
		}

		super.channelRegistered(ctx);
	}

	public void enqueue(IntermediateResultPartitionQueueIterator partitionQueue, InputChannelID receiverId) throws Exception {
		numEnqueueCalls.incrementAndGet();
		ctx.pipeline().fireUserEventTriggered(new SequenceNumberingPartitionQueueIterator(partitionQueue, receiverId));
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg.getClass() == SequenceNumberingPartitionQueueIterator.class) {
			boolean triggerWrite = queue.isEmpty();

			numTotalEnqueueOperations++;
			queue.add((SequenceNumberingPartitionQueueIterator) msg);

			if (triggerWrite) {
				writeAndFlushNextMessageIfPossible(ctx.channel());
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
						if (currentPartitionQueue.subscribe(null)) {
							numTotalSubscribeCalls++;
							numOutstandingSubscribeCalls.incrementAndGet();

							currentPartitionQueue = null;
						}
						else if (currentPartitionQueue.isConsumed()) {
							numConsumedPartitions++;

							currentPartitionQueue = null;
						}
					}
					else {
						BufferResponse resp = new BufferResponse(buffer, currentPartitionQueue.getSequenceNumber(), currentPartitionQueue.getReceiverId());

						channel.writeAndFlush(resp).addListener(writeListener);

						return;
					}
				}
			}
		}
		catch (Throwable t) {
			try {
				if (buffer != null) {
					buffer.recycle();
				}
			}
			catch (Throwable ignored) {
				// Make sure that this buffer is recycled in any case
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
			currentPartitionQueue.discard();
			currentPartitionQueue = null;
		}

		while ((currentPartitionQueue = queue.poll()) != null) {
			currentPartitionQueue.discard();
		}
	}

	// This listener is called after an element of the current queue has been
	// flushed. If successful, the listener triggers further processing of the
	// queues.
	private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
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
	}

	/**
	 * Simple wrapper for the partition queue iterator, which increments a
	 * sequence number for each returned buffer and remembers the receiver ID.
	 */
	private class SequenceNumberingPartitionQueueIterator implements IntermediateResultPartitionQueueIterator, NotificationListener {

		private final IntermediateResultPartitionQueueIterator queueIterator;

		private final InputChannelID receiverId;

		private int sequenceNumber = -1;

		private SequenceNumberingPartitionQueueIterator(IntermediateResultPartitionQueueIterator queueIterator, InputChannelID receiverId) {
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
		public Buffer getNextBuffer() throws IOException {
			Buffer buffer = queueIterator.getNextBuffer();

			if (buffer != null) {
				sequenceNumber++;
			}

			return buffer;
		}

		@Override
		public void discard() throws IOException {
			queueIterator.discard();
		}

		@Override
		public boolean subscribe(NotificationListener ignored) throws AlreadySubscribedException {
			return queueIterator.subscribe(this);
		}

		@Override
		public boolean isConsumed() {
			return queueIterator.isConsumed();
		}

		/**
		 * Enqueue this iterator again after a notification.
		 */
		@Override
		public void onNotification() {
			numOutstandingSubscribeCalls.decrementAndGet();
			ctx.pipeline().fireUserEventTriggered(this);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Debug output task executed periodically by the network I/O thread.
	 */
	private class DebugOutputTask implements Runnable {

		@Override
		public void run() {
			StringBuilder str = new StringBuilder();

			str.append("Channel remote address: ");
			str.append(ctx.channel().remoteAddress());
			str.append(". ");

			str.append("Channel active: ");
			str.append(ctx.channel().isActive());
			str.append(". ");

			str.append("Total number of queue operations: ");
			str.append(numTotalEnqueueOperations);
			str.append(". ");

			str.append("Number of enqueue calls: ");
			str.append(numEnqueueCalls.get());
			str.append(". ");

			str.append("Number of consumed partitions: ");
			str.append(numConsumedPartitions);
			str.append(". ");

			str.append("Number of currently queued partitions: ");
			str.append(queue.size());
			str.append(". ");

			str.append("Current partition queue: ");
			str.append(currentPartitionQueue);
			str.append(". ");

			str.append("Total number of subscribe calls: ");
			str.append(numTotalSubscribeCalls);
			str.append(". ");

			str.append("Number of outstanding subscribe calls: ");
			str.append(numOutstandingSubscribeCalls.get());
			str.append(". ");

			str.append("Channel writeable? ");
			str.append(ctx.channel().isWritable());
			str.append(". ");

			LOG.debug(str.toString());
		}
	}
}
