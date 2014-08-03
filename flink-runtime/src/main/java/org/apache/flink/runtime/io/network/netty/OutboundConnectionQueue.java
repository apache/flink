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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.runtime.io.network.Envelope;
import org.apache.flink.runtime.io.network.NetworkConnectionManager;
import org.apache.flink.runtime.io.network.RemoteReceiver;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class OutboundConnectionQueue extends ChannelInboundHandlerAdapter {

	private static enum QueueEvent {
		TRIGGER_WRITE
	}

	private static final Log LOG = LogFactory.getLog(OutboundConnectionQueue.class);

	private final ChannelWriteListener writeListener = new ChannelWriteListener();

	private final Channel channel;

	private final Queue<Envelope> queuedEnvelopes = new ArrayDeque<Envelope>();

	private final RemoteReceiver receiver;

	private final NetworkConnectionManager connectionManager;

	// Flag to indicate whether a channel close was requested. This flag is true,
	// iff there are no queued envelopes, when the channel is idling. After a
	// successful close request, enqueue should return false.
	private boolean hasRequestedClose = false;

	public OutboundConnectionQueue(
			Channel channel,
			RemoteReceiver receiver,
			NetworkConnectionManager connectionManager,
			int closeAfterIdleForMs) {

		this.channel = channel;
		this.receiver = receiver;
		this.connectionManager = connectionManager;

		channel.pipeline().addFirst("Outbound Connection Queue", this);
		channel.pipeline().addFirst("Idle State Handler",
				new IdleStateHandler(0, 0, closeAfterIdleForMs, TimeUnit.MILLISECONDS));
	}

	/**
	 * Enqueues an envelope to be sent.
	 * <p/>
	 * This method is always invoked by the task thread that wants the envelope sent.
	 * <p/>
	 * If this method returns <code>false</code>, the channel cannot be used to enqueue
	 * envelopes any more and the caller needs to establish a new connection to the target.
	 * The current envelope needs to be enqueued at the new channel.
	 *
	 * @param env the envelope to be sent
	 * @return true, if successfully enqueued or false, if the channel was requested to be closed
	 */
	public boolean enqueue(Envelope env) {
		boolean triggerWrite;

		synchronized (channel) {
			if (hasRequestedClose) {
				// The caller has to ensure that the envelope gets queued to
				// a new channel.
				return false;
			}

			// Initiate envelope processing, after the queue state has
			// changed from empty to non-empty.
			triggerWrite = queuedEnvelopes.isEmpty();

			queuedEnvelopes.add(env);
		}

		if (triggerWrite) {
			channel.pipeline().fireUserEventTriggered(QueueEvent.TRIGGER_WRITE);
		}

		return true;
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
		if (event.getClass() == QueueEvent.class) {
			// Initiate envelope processing, after the queue state has
			// changed from empty to non-empty.
			writeAndFlushNextEnvelopeIfPossible();
		}
		else if (event.getClass() == IdleStateEvent.class) {
			// Channel idle => try to close
			boolean closeConnection = false;

			// Only close the connection, if there are no queued envelopes. We have
			// to ensure that there is no race between closing the channel and
			// enqueuing a new envelope.
			synchronized (channel) {
				if (queuedEnvelopes.isEmpty() && !hasRequestedClose) {

					hasRequestedClose = true;

					closeConnection = true;

					// Notify the connection manager that this channel has been
					// closed.
					connectionManager.close(receiver);
				}
			}

			if (closeConnection) {
				ctx.close().addListener(new ChannelCloseListener());
			}
		}
		else {
			throw new IllegalStateException("Triggered unknown event.");
		}
	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		writeAndFlushNextEnvelopeIfPossible();
	}

	public int getNumQueuedEnvelopes() {
		synchronized (channel) {
			return queuedEnvelopes.size();
		}
	}

	@Override
	public String toString() {
		return channel.toString();
	}

	private void writeAndFlushNextEnvelopeIfPossible() {
		Envelope nextEnvelope = null;

		synchronized (channel) {
			if (channel.isWritable() && !queuedEnvelopes.isEmpty()) {
				nextEnvelope = queuedEnvelopes.poll();
			}
		}

		if (nextEnvelope != null) {
			channel.writeAndFlush(nextEnvelope).addListener(writeListener);
		}
	}

	private class ChannelWriteListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (future.isSuccess()) {
				writeAndFlushNextEnvelopeIfPossible();
			}
			else {
				exceptionOccurred(future.cause() == null
						? new Exception("Envelope send aborted.")
						: future.cause());
			}
		}
	}

	private class ChannelCloseListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (!future.isSuccess()) {
				exceptionOccurred(future.cause() == null
						? new Exception("Close failed.")
						: future.cause());
			}
		}
	}

	private void exceptionOccurred(Throwable t) throws Exception {
		LOG.error(String.format("Exception in Channel %s: %s", channel, t.getMessage()));
		throw new Exception(t);
	}
}
