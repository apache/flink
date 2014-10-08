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

import com.google.common.base.Optional;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.queue.IntermediateResultPartitionQueueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;

class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestServerHandler.class);

	private final IntermediateResultPartitionProvider partitionProvider;

	private final TaskEventDispatcher taskEventDispatcher;

	private final PartitionRequestQueue outboundQueue;

	PartitionRequestServerHandler(IntermediateResultPartitionProvider partitionProvider, TaskEventDispatcher taskEventDispatcher, PartitionRequestQueue outboundQueue) {

		this.partitionProvider = partitionProvider;
		this.taskEventDispatcher = taskEventDispatcher;
		this.outboundQueue = outboundQueue;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
		try {
			Class<?> msgClazz = msg.getClass();

			// ----------------------------------------------------------------
			// Intermediate result partition requests
			// ----------------------------------------------------------------
			if (msgClazz == PartitionRequest.class) {
				PartitionRequest request = (PartitionRequest) msg;

				IntermediateResultPartitionQueueIterator queueIterator =
						partitionProvider.getIntermediateResultPartitionIterator(
								request.producerExecutionId,
								request.partitionId,
								request.queueIndex,
								Optional.<BufferProvider>absent());

				if (queueIterator != null) {
					outboundQueue.enqueue(queueIterator, request.receiverId);
				}
				else {
					respondWithError(ctx, new IllegalArgumentException("Partition not found"), request.receiverId);
				}
			}
			// ----------------------------------------------------------------
			// Task events
			// ----------------------------------------------------------------
			else if (msgClazz == TaskEventRequest.class) {
				TaskEventRequest request = (TaskEventRequest) msg;

				if (!taskEventDispatcher.publish(request.executionId, request.partitionId, request.event)) {
					respondWithError(ctx, new IllegalArgumentException("Task event receiver not found."), request.receiverId);
				}
			}
			else {
				LOG.warn("Received unexpected client request: {}", msg);
			}
		}
		catch (Throwable t) {
			respondWithError(ctx, t);
		}
	}

	private void respondWithError(ChannelHandlerContext ctx, Throwable error) {
		ctx.writeAndFlush(new NettyMessage.ErrorResponse(error));
	}

	private void respondWithError(ChannelHandlerContext ctx, Throwable error, InputChannelID sourceId) {
		ctx.writeAndFlush(new NettyMessage.ErrorResponse(error, sourceId));
	}
}
