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

import com.google.common.base.Optional;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionQueueIterator;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.util.SerializationUtil;

import static org.apache.flink.runtime.io.network.netty.protocols.partition.ClientRequest.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.protocols.partition.ClientRequest.TaskEventRequest;

public class PartitionRequestServerHandler extends SimpleChannelInboundHandler<ClientRequest> {

	TaskEventDispatcher taskEventDispatcher;

	IntermediateResultPartitionProvider partitionProvider;

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, ClientRequest msg) throws Exception {

		if (msg.getClass() == PartitionRequest.class) {
			PartitionRequest request = (PartitionRequest) msg;

			IntermediateResultPartitionQueueIterator queueIterator =
					partitionProvider.getIntermediateResultPartitionIterator(
							request.partitionId,
							request.requestedQueueIndex,
							Optional.<BufferProvider>absent());

			if (queueIterator != null) {
				// enqueue iterator
			}
			else {
				// respond with error message
			}
		}
		else if (msg.getClass() == TaskEventRequest.class) {
			TaskEventRequest request = (TaskEventRequest) msg;

			IntermediateResultPartitionID id = request.partitionId;
			TaskEvent event = (TaskEvent) SerializationUtil.fromSerializedEvent(request.serializedEvent, getClass().getClassLoader());

			if (!taskEventDispatcher.publish(id, event)) {
				// respond with error message
			}
		}
	}
}
