/**
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

import io.netty.channel.ChannelHandler;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.NettyMessageEncoder;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.NettyMessageEncoder.createFrameLengthDecoder;

class PartitionRequestProtocol implements NettyProtocol {

	private final NettyMessageEncoder messageEncoder = new NettyMessageEncoder();

	private final NettyMessage.NettyMessageDecoder messageDecoder = new NettyMessage.NettyMessageDecoder();

	private final ResultPartitionProvider partitionProvider;
	private final TaskEventDispatcher taskEventDispatcher;
	private final NetworkBufferPool networkbufferPool;

	PartitionRequestProtocol(ResultPartitionProvider partitionProvider, TaskEventDispatcher taskEventDispatcher, NetworkBufferPool networkbufferPool) {
		this.partitionProvider = partitionProvider;
		this.taskEventDispatcher = taskEventDispatcher;
		this.networkbufferPool = networkbufferPool;
	}

	// +-------------------------------------------------------------------+
	// |                        SERVER CHANNEL PIPELINE                    |
	// |                                                                   |
	// |    +----------+----------+ (3) write  +----------------------+    |
	// |    | Queue of queues     +----------->| Message encoder      |    |
	// |    +----------+----------+            +-----------+----------+    |
	// |              /|\                                 \|/              |
	// |               | (2) enqueue                       |               |
	// |    +----------+----------+                        |               |
	// |    | Request handler     |                        |               |
	// |    +----------+----------+                        |               |
	// |              /|\                                  |               |
	// |               |                                   |               |
	// |    +----------+----------+                        |               |
	// |    | Message decoder     |                        |               |
	// |    +----------+----------+                        |               |
	// |              /|\                                  |               |
	// |               |                                   |               |
	// |    +----------+----------+                        |               |
	// |    | Frame decoder       |                        |               |
	// |    +----------+----------+                        |               |
	// |              /|\                                  |               |
	// +---------------+-----------------------------------+---------------+
	// |               | (1) client request               \|/
	// +---------------+-----------------------------------+---------------+
	// |               |                                   |               |
	// |       [ Socket.read() ]                    [ Socket.write() ]     |
	// |                                                                   |
	// |  Netty Internal I/O Threads (Transport Implementation)            |
	// +-------------------------------------------------------------------+

	@Override
	public ChannelHandler[] getServerChannelHandlers() {
		PartitionRequestQueue queueOfPartitionQueues = new PartitionRequestQueue();
		PartitionRequestServerHandler serverHandler = new PartitionRequestServerHandler(
				partitionProvider, taskEventDispatcher, queueOfPartitionQueues, networkbufferPool);

		return new ChannelHandler[] {
				messageEncoder,
				createFrameLengthDecoder(),
				messageDecoder,
				serverHandler,
				queueOfPartitionQueues
		};
	}

	//     +-----------+----------+            +----------------------+
	//     | Remote input channel |            | request client       |
	//     +-----------+----------+            +-----------+----------+
	//                 |                                   | (1) write
	// +---------------+-----------------------------------+---------------+
	// |               |     CLIENT CHANNEL PIPELINE       |               |
	// |               |                                  \|/              |
	// |    +----------+----------+            +----------------------+    |
	// |    | Request handler     +            | Message encoder      |    |
	// |    +----------+----------+            +-----------+----------+    |
	// |              /|\                                 \|/              |
	// |               |                                   |               |
	// |    +----------+----------+                        |               |
	// |    | Message decoder     |                        |               |
	// |    +----------+----------+                        |               |
	// |              /|\                                  |               |
	// |               |                                   |               |
	// |    +----------+----------+                        |               |
	// |    | Frame decoder       |                        |               |
	// |    +----------+----------+                        |               |
	// |              /|\                                  |               |
	// +---------------+-----------------------------------+---------------+
	// |               | (3) server response              \|/ (2) client request
	// +---------------+-----------------------------------+---------------+
	// |               |                                   |               |
	// |       [ Socket.read() ]                    [ Socket.write() ]     |
	// |                                                                   |
	// |  Netty Internal I/O Threads (Transport Implementation)            |
	// +-------------------------------------------------------------------+

	@Override
	public ChannelHandler[] getClientChannelHandlers() {
		return new ChannelHandler[] {
				messageEncoder,
				createFrameLengthDecoder(),
				messageDecoder,
				new PartitionRequestClientHandler()};
	}
}
