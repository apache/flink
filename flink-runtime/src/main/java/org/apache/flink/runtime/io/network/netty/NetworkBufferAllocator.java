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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import javax.annotation.Nullable;

/**
 * A buffer allocator for requesting buffers in the network pipeline.
 */
public interface NetworkBufferAllocator {

	/**
	 * Allocates a pooled network buffer for the specific input channel.
	 *
	 * @param receiverId The input channel id to request pooled buffer with.
	 * @param size The requested buffer size.
	 * @return The pooled network buffer.
	 */
	@Nullable
	Buffer allocatePooledNetworkBuffer(InputChannelID receiverId, int size);

	/**
	 * Allocates an un-pooled network buffer with the specific size.
	 *
	 * @param size The requested buffer size.
	 * @return The un-pooled network buffer.
	 */
	Buffer allocateUnPooledNetworkBuffer(int size);
}
