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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An allocator used for requesting buffers in the receiver side of netty handlers.
 */
public class NetworkBufferAllocator {
	private final CreditBasedPartitionRequestClientHandler partitionRequestClientHandler;

	NetworkBufferAllocator(CreditBasedPartitionRequestClientHandler partitionRequestClientHandler) {
		this.partitionRequestClientHandler = checkNotNull(partitionRequestClientHandler);
	}

	/**
	 * Allocates a pooled network buffer for the specific input channel.
	 *
	 * @param receiverId The input channel id to request pooled buffer with.
	 * @param size The requested buffer size.
	 * @return The pooled network buffer.
	 */
	public Buffer allocatePooledNetworkBuffer(InputChannelID receiverId, int size) {
		Buffer buffer = null;

		RemoteInputChannel inputChannel = partitionRequestClientHandler.getInputChannel(receiverId);
		if (inputChannel != null) {
			buffer = inputChannel.requestBuffer();
		}

		return buffer;
	}

	/**
	 * Allocates an un-pooled network buffer with the specific size.
	 *
	 * @param size The requested buffer size.
	 * @return The un-pooled network buffer.
	 */
	public Buffer allocateUnPooledNetworkBuffer(int size) {
		byte[] byteArray = new byte[size];
		MemorySegment memSeg = MemorySegmentFactory.wrap(byteArray);

		return new NetworkBuffer(memSeg, FreeingBufferRecycler.INSTANCE, false);
	}
}
