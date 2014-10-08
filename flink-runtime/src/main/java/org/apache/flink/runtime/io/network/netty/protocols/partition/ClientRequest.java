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

import io.netty.buffer.ByteBuf;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.util.SerializationUtil;

import java.nio.ByteBuffer;

interface ClientRequest<T> extends NettyMessage<T> {

	static class PartitionRequest implements ClientRequest<PartitionRequest> {

		final IntermediateResultPartitionID partitionId;

		final int requestedQueueIndex;

		final InputChannelID channelId;

		PartitionRequest(IntermediateResultPartitionID partitionId, int requestedQueueIndex, InputChannelID channelId) {
			this.partitionId = partitionId;
			this.requestedQueueIndex = requestedQueueIndex;
			this.channelId = channelId;
		}

		@Override
		public byte getId() {
			return 0;
		}

		@Override
		public int getLength() {
			return 16 + 4 + 16;
		}

		@Override
		public void writeTo(ByteBuf outboundBuffer) {
			partitionId.writeTo(outboundBuffer);
			outboundBuffer.writeInt(requestedQueueIndex);
			channelId.writeTo(outboundBuffer);
		}

		public static PartitionRequest readFrom(ByteBuf inboundBuffer) {
			IntermediateResultPartitionID partitionId = IntermediateResultPartitionID.fromByteBuffer(inboundBuffer);
			int requestedQueueIndex = inboundBuffer.readInt();
			InputChannelID channelId = InputChannelID.fromByteBuf(inboundBuffer);

			return new PartitionRequest(partitionId, requestedQueueIndex, channelId);
		}
	}

	static class TaskEventRequest implements ClientRequest<TaskEventRequest> {

		final IntermediateResultPartitionID partitionId;

		final InputChannelID channelId;

		final TaskEvent event;

		ByteBuffer serializedEvent;

		TaskEventRequest(IntermediateResultPartitionID partitionId, TaskEvent event, InputChannelID channelId) {
			this.event = event;
			this.partitionId = partitionId;
			this.channelId = channelId;
		}

		@Override
		public byte getId() {
			return 1;
		}

		@Override
		public int getLength() {
			if (serializedEvent == null) {
				serializedEvent = SerializationUtil.toSerializedEvent(event);
			}

			return 16 + 4 + serializedEvent.remaining() + 16;
		}

		@Override
		public void writeTo(ByteBuf outboundBuffer) {
			if (serializedEvent == null) {
				serializedEvent = SerializationUtil.toSerializedEvent(event);
			}

			partitionId.writeTo(outboundBuffer);
			outboundBuffer.writeInt(serializedEvent.remaining());
			outboundBuffer.writeBytes(serializedEvent);
			channelId.writeTo(outboundBuffer);
		}

//		public static TaskEventRequest readFrom(ByteBuf inboundBuffer) {
//			IntermediateResultPartitionID partitionId = IntermediateResultPartitionID.fromByteBuffer(inboundBuffer);
//
//			int eventLength = inboundBuffer.readInt();
//			ByteBuffer serializedEvent = ByteBuffer.allocate(eventLength);
//			inboundBuffer.readBytes(serializedEvent);
//			TaskEvent event = (TaskEvent) Envelope.fromSerializedEvent(serializedEvent, getClass().getClassLoader());
//
//			InputChannelID channelId = InputChannelID.fromByteBuf(inboundBuffer);
//
//			return new TaskEventRequest(partitionId, event, channelId);
//		}
	}
}
