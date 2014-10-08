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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.io.network.BufferOrEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferFuture;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.util.SerializationUtil;
import org.apache.flink.runtime.util.event.EventListener;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.flink.runtime.io.network.netty.protocols.partition.ServerResponse.BufferOrEventResponse;

public class ClientPartitionRequestHandler extends ChannelInboundHandlerAdapter implements EventListener<BufferFuture> {

	private final ConcurrentMap<InputChannelID, PartitionRequestListener> listeners = null;

	private final ConcurrentMap<InputChannelID, Integer> listenerToSequenceNumberMap = null;

	private BufferOrEventResponse stagedBufferOrEvent;

	private AtomicReference<Buffer> bufferBroker = new AtomicReference<Buffer>(null);

	private ChannelHandlerContext ctx;

	private BufferAvailabilityChangedTask bufferAvailabilityChangedTask = new BufferAvailabilityChangedTask();

	void addListener(RemoteInputChannel listener) {

	}

	void removeListener(RemoteInputChannel listener) {

	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		if (ctx == null) {
			this.ctx = ctx;
		}

		super.channelActive(ctx);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (stagedBufferOrEvent != null) {
			throw new IllegalStateException("Inconsistent state: no channel read event should be fired" +
					"as long as there is a staged buffer or event response.");
		}

		if (msg.getClass() == BufferOrEventResponse.class) {
			BufferOrEventResponse boe = (BufferOrEventResponse) msg;

			PartitionRequestListener inputChannel = listeners.get(boe.getReceiverId());
			if (inputChannel == null) {
				boe.release();
				return;
			}

			Integer expectedSequenceNumber = listenerToSequenceNumberMap.get(boe.getReceiverId());
			if (expectedSequenceNumber == null || expectedSequenceNumber != boe.getSequenceNumber()) {
				boe.release();
				inputChannel.onError(new IllegalStateException("Received unexpected envelope"));
				return;
			}

			decodeBufferOrEvent(inputChannel, boe);
		}
	}

	private void decodeBufferOrEvent(PartitionRequestListener listener, BufferOrEventResponse boe) {
		boolean success = true;

		try {
			if (boe.isBuffer()) {
				BufferFuture bf = listener.requestBuffer(boe.getSize());

				if (bf != null) {
					if (bf.getBuffer() != null) {
						Buffer buffer = bf.getBuffer();
						ByteBuffer nioBuffer = buffer.getNioBuffer();
						boe.copyNettyBufferTo(nioBuffer);

						listener.onBufferOrEvent(new BufferOrEvent(buffer));
					}
					else {
						if (ctx.channel().config().isAutoRead()) {
							ctx.channel().config().setAutoRead(false);
						}

						stagedBufferOrEvent = boe;
						success = false;
						bf.addListener(this);
					}
				}
			}
			else if (boe.isEvent()) {
				ByteBuffer serializedEvent = ByteBuffer.allocate(boe.getSize());
				boe.copyNettyBufferTo(serializedEvent);
				AbstractEvent event = SerializationUtil.fromSerializedEvent(serializedEvent, getClass().getClassLoader());

				listener.onBufferOrEvent(new BufferOrEvent(event));
			}
			else {
				listener.onError(new IllegalStateException("Received illegal buffer or event response with neither buffer nor event attached."));
			}
		}
		finally {
			if (success) {
				boe.release();
			}
		}
	}

	@Override
	public void onEvent(BufferFuture bf) {
		if (bf.isSuccess()) {
			if (bufferBroker.compareAndSet(null, bf.getBuffer())) {
				ctx.channel().eventLoop().execute(bufferAvailabilityChangedTask);
			}
			else {
				bf.getBuffer().recycle();
			}
		}
		else {
			// ERROR CASE
		}
	}

	/**
	 * Continues the decoding of a staged buffer after a buffer has become available again.
	 * <p/>
	 * This task should be executed by the IO thread to ensure safe access to the staged buffer.
	 */
	private class BufferAvailabilityChangedTask implements Runnable {

		@Override
		public void run() {
			Buffer buffer = bufferBroker.getAndSet(null);
			checkState(buffer != null, "Called buffer availability changed task w/o a buffer.");

			stagedBufferOrEvent.copyNettyBufferTo(buffer.getNioBuffer());
			stagedBufferOrEvent.release();

			PartitionRequestListener inputChannel = listeners.get(stagedBufferOrEvent.getReceiverId());

			if (inputChannel != null) {
				inputChannel.onBufferOrEvent(new BufferOrEvent(buffer));

				ctx.channel().config().setAutoRead(true);
			}
		}
	}

}
