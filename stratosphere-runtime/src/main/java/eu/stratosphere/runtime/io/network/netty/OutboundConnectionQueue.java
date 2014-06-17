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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class OutboundConnectionQueue extends ChannelInboundHandlerAdapter implements ChannelFutureListener {

	private static final Log LOG = LogFactory.getLog(OutboundConnectionQueue.class);

	private final Channel channel;

	private final ArrayDeque<Envelope> queuedEnvelopes = new ArrayDeque<Envelope>();

	private final AtomicInteger numQueuedEnvelopes = new AtomicInteger(0);

	public OutboundConnectionQueue(Channel channel) {
		this.channel = channel;

		channel.pipeline().addFirst(this);
	}

	/**
	 * Enqueues an envelope to be sent later.
	 * <p/>
	 * This method is always invoked by the task thread that wants the envelope sent.
	 *
	 * @param env The envelope to be sent.
	 */
	public void enqueue(Envelope env) {
		// the user event trigger ensure thread-safe hand-over of the envelope
		this.channel.pipeline().fireUserEventTriggered(env);
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object envelopeToEnqueue) throws Exception {
		boolean triggerWrite = this.queuedEnvelopes.isEmpty();

		this.queuedEnvelopes.addLast((Envelope) envelopeToEnqueue);
		this.numQueuedEnvelopes.incrementAndGet();

		if (triggerWrite) {
			writeAndFlushNextEnvelopeIfPossible();
		}
	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		writeAndFlushNextEnvelopeIfPossible();
	}

	@Override
	public void operationComplete(ChannelFuture future) throws Exception {
		if (future.isSuccess()) {
			writeAndFlushNextEnvelopeIfPossible();
		}
		else if (future.cause() != null) {
			exceptionOccurred(future.cause());
		}
		else {
			exceptionOccurred(new Exception("Envelope send aborted."));
		}
	}

	public int getNumQueuedEnvelopes() {
		return this.numQueuedEnvelopes.intValue();
	}

	@Override
	public String toString() {
		return channel.toString();
	}

	private void writeAndFlushNextEnvelopeIfPossible() {
		if (this.channel.isWritable() && !this.queuedEnvelopes.isEmpty()) {
			Envelope nextEnvelope = this.queuedEnvelopes.pollFirst();
			this.numQueuedEnvelopes.decrementAndGet();

			this.channel.writeAndFlush(nextEnvelope).addListener(this);
		}
	}

	private void exceptionOccurred(Throwable t) throws Exception {
		LOG.error(String.format("An exception occurred in Channel %s: %s", this.channel, t.getMessage()));
		throw new Exception(t);
	}
}
