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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.Envelope;
import org.apache.flink.runtime.util.AtomicDisposableReferenceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class OutboundConnectionQueue extends ChannelInboundHandlerAdapter implements ChannelFutureListener {

	private static final Logger LOG = LoggerFactory.getLogger(OutboundConnectionQueue.class);

	private final Channel channel;

	private final ArrayDeque<Envelope> queuedEnvelopes = new ArrayDeque<Envelope>();

	private final AtomicInteger numQueuedEnvelopes = new AtomicInteger(0);

	private final AtomicDisposableReferenceCounter closeReferenceCounter = new AtomicDisposableReferenceCounter();

	public OutboundConnectionQueue(Channel channel) {
		this.channel = channel;

		channel.pipeline().addFirst(this);
	}

	boolean incrementReferenceCounter() {
		return closeReferenceCounter.incrementReferenceCounter();
	}

	boolean decrementReferenceCounter() {
		return closeReferenceCounter.decrementReferenceCounter();
	}

	void close() {
		channel.close();
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
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		releaseAllEnvelopes();

		super.channelInactive(ctx);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		releaseAllEnvelopes();

		super.exceptionCaught(ctx, cause);
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
		LOG.error("An exception occurred in Channel {}: {}", channel, t.getMessage());
		throw new Exception(t);
	}

	private void releaseAllEnvelopes() {
		Envelope envelope;
		while ((envelope = queuedEnvelopes.poll()) != null) {
			Buffer buffer = envelope.getBuffer();
			if (buffer != null) {
				buffer.recycleBuffer();
			}
		}
	}
}
