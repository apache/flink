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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.ArrayDeque;

class OutboundQueue<T> extends ChannelInboundHandlerAdapter {

	private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

	private final ArrayDeque<T> queuedMessages = new ArrayDeque<T>();

	private final Class<T> msgType;

	public OutboundQueue(Class<T> msgType) {
		this.msgType = msgType;
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msgType.isAssignableFrom(msg.getClass())) {
			boolean triggerWrite = queuedMessages.isEmpty();

			queuedMessages.addLast((T) msg);

			if (triggerWrite) {
				writeAndFlushNextMessageIfPossible(ctx.channel());
			}
		}
		else {
			ctx.fireUserEventTriggered(msg);
		}
	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		writeAndFlushNextMessageIfPossible(ctx.channel());
	}

	private void writeAndFlushNextMessageIfPossible(Channel channel) {
		if (channel.isWritable() && !queuedMessages.isEmpty()) {
			channel.writeAndFlush(queuedMessages.pollFirst()).addListener(writeListener);
		}
	}

	private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (future.isSuccess()) {
				writeAndFlushNextMessageIfPossible(future.channel());
			}
			else if (future.cause() != null) {
				throw new Exception(future.cause());
			}
			else {
				throw new Exception("Sending cancelled.");
			}
		}
	}
}
