/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.network.netty;

import eu.stratosphere.runtime.io.network.Envelope;
import eu.stratosphere.runtime.io.network.EnvelopeDispatcher;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class InboundEnvelopeDispatcher extends ChannelInboundHandlerAdapter {

	private final EnvelopeDispatcher envelopeDispatcher;

	public InboundEnvelopeDispatcher(EnvelopeDispatcher envelopeDispatcher) {
		this.envelopeDispatcher = envelopeDispatcher;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		Envelope envelope = (Envelope) msg;

		envelopeDispatcher.dispatchFromNetwork(envelope);
	}
}
