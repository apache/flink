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
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.flink.runtime.io.network.netty.protocols.partition.ClientRequest.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.protocols.partition.ClientRequest.TaskEventRequest;

public class PartitionRequestClient {

	private final Channel channel;

	private final ClientPartitionRequestHandler handler;

	public PartitionRequestClient(Channel channel, ClientPartitionRequestHandler handler) {
		this.channel = checkNotNull(channel);
		this.handler = checkNotNull(handler);
	}

	public void requestIntermediateResultPartition(
			final IntermediateResultPartitionID partitionId,
			final int requestedQueueIndex,
			final RemoteInputChannel inputChannel) {

		handler.addListener(inputChannel);
		channel.writeAndFlush(new PartitionRequest(partitionId, requestedQueueIndex, inputChannel.getId())).addListener(
				new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						if (!future.isSuccess()) {
							handler.removeListener(inputChannel);
							inputChannel.onError(future.cause());
						}
					}
				}
		);
	}

	public void sendTaskEvent(
			IntermediateResultPartitionID partitionId,
			TaskEvent event,
			final RemoteInputChannel inputChannel) {

		channel.writeAndFlush(new TaskEventRequest(partitionId, event, inputChannel.getId())).addListener(
				new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						if (!future.isSuccess()) {
							inputChannel.onError(future.cause());
						}
					}
				});
	}
}
