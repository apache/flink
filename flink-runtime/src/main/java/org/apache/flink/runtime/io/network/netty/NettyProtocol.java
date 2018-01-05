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

import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.NettyMessageEncoder.createFrameLengthDecoder;

/**
 * Defines the server and client channel handlers, i.e. the protocol, used by netty.
 */
public class NettyProtocol {

	private final NettyMessage.NettyMessageEncoder
		messageEncoder = new NettyMessage.NettyMessageEncoder();

	private final NettyMessage.NettyMessageDecoder messageDecoder = new NettyMessage.NettyMessageDecoder();

	private final ResultPartitionProvider partitionProvider;
	private final TaskEventDispatcher taskEventDispatcher;

	NettyProtocol(ResultPartitionProvider partitionProvider, TaskEventDispatcher taskEventDispatcher) {
		this.partitionProvider = partitionProvider;
		this.taskEventDispatcher = taskEventDispatcher;
	}

	/**
	 * Returns the server channel handlers.
	 *
	 * <pre>
	 * +-------------------------------------------------------------------+
	 * |                        SERVER CHANNEL PIPELINE                    |
	 * |                                                                   |
	 * |    +----------+----------+ (3) write  +----------------------+    |
	 * |    | Queue of queues     +----------->| Message encoder      |    |
	 * |    +----------+----------+            +-----------+----------+    |
	 * |              /|\                                 \|/              |
	 * |               | (2) enqueue                       |               |
	 * |    +----------+----------+                        |               |
	 * |    | Request handler     |                        |               |
	 * |    +----------+----------+                        |               |
	 * |              /|\                                  |               |
	 * |               |                                   |               |
	 * |    +----------+----------+                        |               |
	 * |    | Message decoder     |                        |               |
	 * |    +----------+----------+                        |               |
	 * |              /|\                                  |               |
	 * |               |                                   |               |
	 * |    +----------+----------+                        |               |
	 * |    | Frame decoder       |                        |               |
	 * |    +----------+----------+                        |               |
	 * |              /|\                                  |               |
	 * +---------------+-----------------------------------+---------------+
	 * |               | (1) client request               \|/
	 * +---------------+-----------------------------------+---------------+
	 * |               |                                   |               |
	 * |       [ Socket.read() ]                    [ Socket.write() ]     |
	 * |                                                                   |
	 * |  Netty Internal I/O Threads (Transport Implementation)            |
	 * +-------------------------------------------------------------------+
	 * </pre>
	 *
	 * @return channel handlers
	 */
	public ChannelHandler[] getServerChannelHandlers() {
		PartitionRequestQueue queueOfPartitionQueues = new PartitionRequestQueue();
		PartitionRequestServerHandler serverHandler = new PartitionRequestServerHandler(
			partitionProvider, taskEventDispatcher, queueOfPartitionQueues);

		return new ChannelHandler[] {
			messageEncoder,
			createFrameLengthDecoder(),
			messageDecoder,
			serverHandler,
			queueOfPartitionQueues
		};
	}

	/**
	 * Returns the client channel handlers.
	 *
	 * <pre>
	 *     +-----------+----------+            +----------------------+
	 *     | Remote input channel |            | request client       |
	 *     +-----------+----------+            +-----------+----------+
	 *                 |                                   | (1) write
	 * +---------------+-----------------------------------+---------------+
	 * |               |     CLIENT CHANNEL PIPELINE       |               |
	 * |               |                                  \|/              |
	 * |    +----------+----------+            +----------------------+    |
	 * |    | Request handler     +            | Message encoder      |    |
	 * |    +----------+----------+            +-----------+----------+    |
	 * |              /|\                                 \|/              |
	 * |               |                                   |               |
	 * |    +----------+----------+                        |               |
	 * |    | Message decoder     |                        |               |
	 * |    +----------+----------+                        |               |
	 * |              /|\                                  |               |
	 * |               |                                   |               |
	 * |    +----------+----------+                        |               |
	 * |    | Frame decoder       |                        |               |
	 * |    +----------+----------+                        |               |
	 * |              /|\                                  |               |
	 * +---------------+-----------------------------------+---------------+
	 * |               | (3) server response              \|/ (2) client request
	 * +---------------+-----------------------------------+---------------+
	 * |               |                                   |               |
	 * |       [ Socket.read() ]                    [ Socket.write() ]     |
	 * |                                                                   |
	 * |  Netty Internal I/O Threads (Transport Implementation)            |
	 * +-------------------------------------------------------------------+
	 * </pre>
	 *
	 * @return channel handlers
	 */
	public ChannelHandler[] getClientChannelHandlers() {
		return new ChannelHandler[] {
			messageEncoder,
			createFrameLengthDecoder(),
			messageDecoder,
			new PartitionRequestClientHandler()};
	}

}
