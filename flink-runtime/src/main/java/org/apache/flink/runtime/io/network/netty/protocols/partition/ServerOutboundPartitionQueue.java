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

package org.apache.flink.runtime.io.network.netty.protocols.partition;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.runtime.io.network.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionQueueIterator;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import java.io.IOException;
import java.util.ArrayDeque;

import static org.apache.flink.runtime.io.network.netty.protocols.partition.ServerResponse.BufferOrEventResponse;

public class ServerOutboundPartitionQueue extends ChannelInboundHandlerAdapter {

	private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

	private final ArrayDeque<PartitionQueueAndInputChannelId> queuedMessages = new ArrayDeque<PartitionQueueAndInputChannelId>();

	private ChannelHandlerContext ctx;

	private PartitionQueueAndInputChannelId currentQueue;

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		if (this.ctx == null) {
			this.ctx = ctx;
		}
		super.channelRegistered(ctx);
	}

	public void enqueue(IntermediateResultPartitionQueueIterator partitionQueue, InputChannelID inputChannelId) throws Exception {
		ctx.pipeline().fireUserEventTriggered(new PartitionQueueAndInputChannelId(partitionQueue, inputChannelId));
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg.getClass() == PartitionQueueAndInputChannelId.class) {
			boolean triggerWrite = queuedMessages.isEmpty();

			queuedMessages.addLast((PartitionQueueAndInputChannelId) msg);

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

	private void writeAndFlushNextMessageIfPossible(Channel channel) throws IOException {
		if (channel.isWritable()) {
			do {
				if (currentQueue == null) {
					if (queuedMessages.isEmpty()) {
						return;
					}

					currentQueue = queuedMessages.poll();
				}

				if (currentQueue.partitionQueue.hasNext()) {
					BufferOrEvent boe = currentQueue.partitionQueue.getNextBufferOrEvent();
					if (boe != null) {
						// TODO re-add sequence numbers
						int sequenceNumber = 0;

						BufferOrEventResponse resp = new BufferOrEventResponse(sequenceNumber, currentQueue.inputChannelId, boe);

						channel.writeAndFlush(resp).addListener(writeListener);
					}
					else {
						queuedMessages.add(currentQueue);
						currentQueue = null;
					}
				}
				else {
					currentQueue = null;
				}
			} while (true);
		}
	}

	private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (future.isSuccess()) {
				writeAndFlushNextMessageIfPossible(future.channel());
			}
			else if (future.cause() != null) {
				throw new Exception(future.cause());
			}
			else {
				throw new Exception("Sending cancelled.");
			}
		}
	}

	private static class PartitionQueueAndInputChannelId {

		final IntermediateResultPartitionQueueIterator partitionQueue;

		final InputChannelID inputChannelId;

		private PartitionQueueAndInputChannelId(IntermediateResultPartitionQueueIterator partitionQueue, InputChannelID inputChannelId) {
			this.partitionQueue = partitionQueue;
			this.inputChannelId = inputChannelId;
		}
	}
}
