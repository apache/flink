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

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.flink.runtime.io.network.netty.NettyProtocol;

import static org.apache.flink.runtime.io.network.netty.protocols.partition.ServerResponse.ServerResponseDecoder;

public class PartitionRequestProtocol implements NettyProtocol {

	private final NettyMessageEncoder encoder = new NettyMessageEncoder();

	private final ServerResponseDecoder serverResponseDecoder = new ServerResponseDecoder();

	@Override
	public void setServerChannelPipeline(ChannelPipeline pipeline) {
	}

	@Override
	public void setClientChannelPipeline(ChannelPipeline channelPipeline) {
		channelPipeline
				.addLast("Client Encoder", encoder)
				.addLast("Frame Decoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, -4, 4))
				.addLast("Server Response Decoder", serverResponseDecoder)
				.addLast("Client Handler", new ClientPartitionRequestHandler());

	}

}
