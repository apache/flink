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

package org.apache.flink.runtime.io.network.netty.protocols.envelope;

import io.netty.channel.ChannelPipeline;
import org.apache.flink.runtime.io.network.ChannelManager;
import org.apache.flink.runtime.io.network.Envelope;
import org.apache.flink.runtime.io.network.netty.NettyProtocol;
import org.apache.flink.runtime.io.network.netty.protocols.envelope.InboundEnvelopeDecoder;
import org.apache.flink.runtime.io.network.netty.protocols.envelope.InboundEnvelopeDispatcher;
import org.apache.flink.runtime.io.network.netty.protocols.envelope.OutboundEnvelopeEncoder;
import org.apache.flink.runtime.io.network.netty.protocols.envelope.OutboundQueue;

public class NettyEnvelopeProtocol implements NettyProtocol {

	private final ChannelManager channelManager;

	// Sharable client envelope encoding handler
	private final OutboundEnvelopeEncoder clientEnvelopeEncoder = new OutboundEnvelopeEncoder();

	public NettyEnvelopeProtocol(ChannelManager channelManager) {
		this.channelManager = channelManager;
	}

	@Override
	public void setServerChannelPipeline(ChannelPipeline pipeline) {
		pipeline.addLast("Envelope Decoder", new InboundEnvelopeDecoder(channelManager))
				.addLast("Envelope Dispatcher", new InboundEnvelopeDispatcher(channelManager));
	}

	@Override
	public void setClientChannelPipeline(ChannelPipeline pipeline) {
		pipeline.addLast("Envelope Queue", new OutboundQueue<Envelope>(Envelope.class));
		pipeline.addLast("Envelope Encoder", clientEnvelopeEncoder);
	}
}
