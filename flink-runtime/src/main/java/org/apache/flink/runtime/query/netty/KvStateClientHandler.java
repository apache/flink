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

package org.apache.flink.runtime.query.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.apache.flink.runtime.query.netty.message.KvStateRequestFailure;
import org.apache.flink.runtime.query.netty.message.KvStateRequestResult;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.query.netty.message.KvStateRequestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;

/**
 * This handler expects responses from {@link KvStateServerHandler}.
 *
 * <p>It deserializes the response and calls the registered callback, which is
 * responsible for actually handling the result (see {@link KvStateClient.EstablishedConnection}).
 */
class KvStateClientHandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(KvStateClientHandler.class);

	private final KvStateClientHandlerCallback callback;

	/**
	 * Creates a {@link KvStateClientHandler} with the callback.
	 *
	 * @param callback Callback for responses.
	 */
	KvStateClientHandler(KvStateClientHandlerCallback callback) {
		this.callback = callback;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		try {
			ByteBuf buf = (ByteBuf) msg;
			KvStateRequestType msgType = KvStateRequestSerializer.deserializeHeader(buf);

			if (msgType == KvStateRequestType.REQUEST_RESULT) {
				KvStateRequestResult result = KvStateRequestSerializer.deserializeKvStateRequestResult(buf);
				callback.onRequestResult(result.getRequestId(), result.getSerializedResult());
			} else if (msgType == KvStateRequestType.REQUEST_FAILURE) {
				KvStateRequestFailure failure = KvStateRequestSerializer.deserializeKvStateRequestFailure(buf);
				callback.onRequestFailure(failure.getRequestId(), failure.getCause());
			} else if (msgType == KvStateRequestType.SERVER_FAILURE) {
				throw KvStateRequestSerializer.deserializeServerFailure(buf);
			} else {
				throw new IllegalStateException("Unexpected response type '" + msgType + "'");
			}
		} catch (Throwable t1) {
			try {
				callback.onFailure(t1);
			} catch (Throwable t2) {
				LOG.error("Failed to notify callback about failure", t2);
			}
		} finally {
			ReferenceCountUtil.release(msg);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		try {
			callback.onFailure(cause);
		} catch (Throwable t) {
			LOG.error("Failed to notify callback about failure", t);
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// Only the client is expected to close the channel. Otherwise it
		// indicates a failure. Note that this will be invoked in both cases
		// though. If the callback closed the channel, the callback must be
		// ignored.
		try {
			callback.onFailure(new ClosedChannelException());
		} catch (Throwable t) {
			LOG.error("Failed to notify callback about failure", t);
		}
	}
}
