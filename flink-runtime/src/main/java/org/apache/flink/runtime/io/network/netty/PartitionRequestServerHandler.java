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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CancelPartitionRequest;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;

/**
 * Channel handler to initiate data transfers and dispatch backwards flowing task events.
 */
class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestServerHandler.class);

	private final ResultPartitionProvider partitionProvider;

	private final TaskEventDispatcher taskEventDispatcher;

	private final PartitionRequestQueue outboundQueue;

	private final NetworkBufferPool networkBufferPool;

	private BufferPool bufferPool;

	PartitionRequestServerHandler(
			ResultPartitionProvider partitionProvider,
			TaskEventDispatcher taskEventDispatcher,
			PartitionRequestQueue outboundQueue,
			NetworkBufferPool networkBufferPool) {

		this.partitionProvider = partitionProvider;
		this.taskEventDispatcher = taskEventDispatcher;
		this.outboundQueue = outboundQueue;
		this.networkBufferPool = networkBufferPool;
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);

		bufferPool = networkBufferPool.createBufferPool(1, false);
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		super.channelUnregistered(ctx);

		if (bufferPool != null) {
			bufferPool.lazyDestroy();
		}
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

				LOG.debug("Read channel on {}: {}.", ctx.channel().localAddress(), request);

				try {
					ResultSubpartitionView subpartition =
							partitionProvider.createSubpartitionView(
									request.partitionId,
									request.queueIndex,
									bufferPool);

					outboundQueue.enqueue(subpartition, request.receiverId);
				}
				catch (PartitionNotFoundException notFound) {
					respondWithError(ctx, notFound, request.receiverId);
				}
			}
			// ----------------------------------------------------------------
			// Task events
			// ----------------------------------------------------------------
			else if (msgClazz == TaskEventRequest.class) {
				TaskEventRequest request = (TaskEventRequest) msg;

				if (!taskEventDispatcher.publish(request.partitionId, request.event)) {
					respondWithError(ctx, new IllegalArgumentException("Task event receiver not found."), request.receiverId);
				}
			}
			else if (msgClazz == CancelPartitionRequest.class) {
				CancelPartitionRequest request = (CancelPartitionRequest) msg;

				outboundQueue.cancel(request.receiverId);
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
		LOG.debug("Responding with error: {}.", error.getClass());

		ctx.writeAndFlush(new NettyMessage.ErrorResponse(error, sourceId));
	}
}
