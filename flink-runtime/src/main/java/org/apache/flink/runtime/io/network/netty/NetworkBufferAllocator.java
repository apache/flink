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
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An allocator used for requesting buffers in the client side netty handlers.
 */
class NetworkBufferAllocator {
	private final NetworkClientHandler networkClientHandler;

	NetworkBufferAllocator(NetworkClientHandler networkClientHandler) {
		this.networkClientHandler = checkNotNull(networkClientHandler);
	}

	/**
	 * Allocates a pooled network buffer for the specific input channel.
	 *
	 * @param receiverId The id of the requested input channel.
	 * @return The pooled network buffer.
	 */
	@Nullable
	Buffer allocatePooledNetworkBuffer(InputChannelID receiverId) {
		Buffer buffer = null;

		RemoteInputChannel inputChannel = networkClientHandler.getInputChannel(receiverId);

		// If the input channel has been released, we cannot allocate buffer and the received message
		// will be discarded.
		if (inputChannel != null) {
			buffer = inputChannel.requestBuffer();
		}

		return buffer;
	}

	/**
	 * Allocates an un-pooled network buffer with the specific size.
	 *
	 * @param size The requested buffer size.
	 * @param dataType The data type this buffer represents.
	 * @return The un-pooled network buffer.
	 */
	Buffer allocateUnPooledNetworkBuffer(int size, Buffer.DataType dataType) {
		byte[] byteArray = new byte[size];
		MemorySegment memSeg = MemorySegmentFactory.wrap(byteArray);

		return new NetworkBuffer(memSeg, FreeingBufferRecycler.INSTANCE, dataType);
	}
}
