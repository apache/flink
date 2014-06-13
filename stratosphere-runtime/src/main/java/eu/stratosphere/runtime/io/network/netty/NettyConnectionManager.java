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

import eu.stratosphere.runtime.io.network.ChannelManager;
import eu.stratosphere.runtime.io.network.Envelope;
import eu.stratosphere.runtime.io.network.EnvelopeDispatcher;
import eu.stratosphere.runtime.io.network.NetworkConnectionManager;
import eu.stratosphere.runtime.io.network.RemoteReceiver;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProviderBroker;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NettyConnectionManager implements NetworkConnectionManager {

	private static final Log LOG = LogFactory.getLog(NettyConnectionManager.class);

	private static final int DEBUG_PRINT_QUEUED_ENVELOPES_EVERY_MS = 10000;

	private final ConcurrentMap<RemoteReceiver, Object> outConnections = new ConcurrentHashMap<RemoteReceiver, Object>();

	private final InetAddress bindAddress;

	private final int bindPort;

	private final int bufferSize;

	private final int numInThreads;

	private final int numOutThreads;

	private final int lowWaterMark;

	private final int highWaterMark;

	private ServerBootstrap in;

	private Bootstrap out;

	public NettyConnectionManager(InetAddress bindAddress, int bindPort, int bufferSize, int numInThreads,
								int numOutThreads, int lowWaterMark, int highWaterMark) {

		this.bindAddress = bindAddress;
		this.bindPort = bindPort;

		this.bufferSize = bufferSize;

		int defaultNumThreads = Math.max(Runtime.getRuntime().availableProcessors() / 4, 1);

		this.numInThreads = (numInThreads == -1) ? defaultNumThreads : numInThreads;
		this.numOutThreads = (numOutThreads == -1) ? defaultNumThreads : numOutThreads;

		this.lowWaterMark = (lowWaterMark == -1) ? bufferSize / 2 : lowWaterMark;
		this.highWaterMark = (highWaterMark == -1) ? bufferSize : highWaterMark;
	}

	@Override
	public void start(ChannelManager channelManager) throws IOException {
		LOG.info(String.format("Starting with %d incoming and %d outgoing connection threads.", numInThreads, numOutThreads));
		LOG.info(String.format("Setting low water mark to %d and high water mark to %d bytes.", lowWaterMark, highWaterMark));

		final BufferProviderBroker bufferProviderBroker = channelManager;
		final EnvelopeDispatcher envelopeDispatcher = channelManager;

		// --------------------------------------------------------------------
		// server bootstrap (incoming connections)
		// --------------------------------------------------------------------
		in = new ServerBootstrap();
		in.group(new NioEventLoopGroup(numInThreads))
				.channel(NioServerSocketChannel.class)
				.localAddress(bindAddress, bindPort)
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel channel) throws Exception {
						channel.pipeline()
								.addLast(new InboundEnvelopeDecoder(bufferProviderBroker))
								.addLast(new InboundEnvelopeDispatcher(envelopeDispatcher));
					}
				})
				.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(bufferSize))
				.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

		// --------------------------------------------------------------------
		// client bootstrap (outgoing connections)
		// --------------------------------------------------------------------
		out = new Bootstrap();
		out.group(new NioEventLoopGroup(numOutThreads))
				.channel(NioSocketChannel.class)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel channel) throws Exception {
						channel.pipeline()
								.addLast(new OutboundEnvelopeEncoder());
					}
				})
				.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, lowWaterMark)
				.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, highWaterMark)
				.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
				.option(ChannelOption.TCP_NODELAY, false)
				.option(ChannelOption.SO_KEEPALIVE, true);

		try {
			in.bind().sync();
		} catch (InterruptedException e) {
			throw new IOException("Interrupted while trying to bind server socket.");
		}

		if (LOG.isDebugEnabled()) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					Date date = new Date();

					while (true) {
						try {
							Thread.sleep(DEBUG_PRINT_QUEUED_ENVELOPES_EVERY_MS);

							date.setTime(System.currentTimeMillis());

							System.out.println(date);
							System.out.println(getNonZeroNumQueuedEnvelopes());
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}).start();
		}
	}

	@Override
	public void enqueue(Envelope envelope, RemoteReceiver receiver) throws IOException {
		// Get the channel. The channel may be
		// 1) a channel that already exists (usual case) -> just send the data
		// 2) a channel that is in buildup (sometimes) -> attach to the future and wait for the actual channel
		// 3) not yet existing -> establish the channel

		final Object entry = this.outConnections.get(receiver);
		final OutboundConnectionQueue channel;

		if (entry != null) {
			// existing channel or channel in buildup
			if (entry instanceof OutboundConnectionQueue) {
				channel = (OutboundConnectionQueue) entry;
			}
			else {
				ChannelInBuildup future = (ChannelInBuildup) entry;
				channel = future.waitForChannel();
			}
		}
		else {
			// No channel yet. Create one, but watch out for a race.
			// We create a "buildup future" and atomically add it to the map.
			// Only the thread that really added it establishes the channel.
			// The others need to wait on that original establisher's future.
			ChannelInBuildup inBuildup = new ChannelInBuildup(this.out, receiver);
			Object old = this.outConnections.putIfAbsent(receiver, inBuildup);

			if (old == null) {
				this.out.connect(receiver.getConnectionAddress()).addListener(inBuildup);
				channel = inBuildup.waitForChannel();

				Object previous = this.outConnections.put(receiver, channel);

				if (inBuildup != previous) {
					throw new IOException("Race condition during channel build up.");
				}
			}
			else if (old instanceof ChannelInBuildup) {
				channel = ((ChannelInBuildup) old).waitForChannel();
			}
			else {
				channel = (OutboundConnectionQueue) old;
			}
		}

		channel.enqueue(envelope);
	}

	@Override
	public void shutdown() throws IOException {
		if (!in.group().isShuttingDown()) {
			LOG.info("Shutting down incoming connections.");

			try {
				in.group().shutdownGracefully().sync();
			} catch (InterruptedException e) {
				throw new RuntimeException("Interrupted while trying to shutdown incoming connections.");
			}
		}

		if (!out.group().isShuttingDown()) {
			LOG.info("Shutting down outgoing connections.");

			try {
				out.group().shutdownGracefully().sync();
			} catch (InterruptedException e) {
				throw new RuntimeException("Interrupted while trying to shutdown outgoing connections.");
			}
		}
	}

	private String getNonZeroNumQueuedEnvelopes() {
		StringBuilder str = new StringBuilder();

		str.append(String.format("==== %d outgoing connections ===\n", this.outConnections.size()));

		for (Map.Entry<RemoteReceiver, Object> entry : this.outConnections.entrySet()) {
			RemoteReceiver receiver = entry.getKey();

			Object value = entry.getValue();
			if (value instanceof OutboundConnectionQueue) {
				OutboundConnectionQueue queue = (OutboundConnectionQueue) value;
				if (queue.getNumQueuedEnvelopes() > 0) {
					str.append(String.format("%s> Number of queued envelopes for %s with channel %s: %d\n",
							Thread.currentThread().getId(), receiver, queue.toString(), queue.getNumQueuedEnvelopes()));
				}
			}
			else if (value instanceof ChannelInBuildup) {
				str.append(String.format("%s> Connection to %s is still in buildup\n",
						Thread.currentThread().getId(), receiver));
			}
		}

		return str.toString();
	}

	// ------------------------------------------------------------------------

	private static final class ChannelInBuildup implements ChannelFutureListener {

		private final Object lock = new Object();

		private volatile OutboundConnectionQueue channel;

		private volatile Throwable error;

		private int numRetries = 3;

		private final Bootstrap out;

		private final RemoteReceiver receiver;

		private ChannelInBuildup(Bootstrap out, RemoteReceiver receiver) {
			this.out = out;
			this.receiver = receiver;
		}

		private void handInChannel(OutboundConnectionQueue c) {
			synchronized (this.lock) {
				this.channel = c;
				this.lock.notifyAll();
			}
		}

		private void notifyOfError(Throwable error) {
			synchronized (this.lock) {
				this.error = error;
				this.lock.notifyAll();
			}
		}

		private OutboundConnectionQueue waitForChannel() throws IOException {
			synchronized (this.lock) {
				while (this.error == null && this.channel == null) {
					try {
						this.lock.wait(2000);
					} catch (InterruptedException e) {
						throw new RuntimeException("Channel buildup interrupted.");
					}
				}
			}

			if (this.error != null) {
				throw new IOException("Connecting the channel failed: " + error.getMessage(), error);
			}

			return this.channel;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (future.isSuccess()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Channel %s connected", future.channel()));
				}

				handInChannel(new OutboundConnectionQueue(future.channel()));
			}
			else if (this.numRetries > 0) {
				LOG.debug(String.format("Connection request did not succeed, retrying (%d attempts left)", this.numRetries));

				this.out.connect(this.receiver.getConnectionAddress()).addListener(this);
				this.numRetries--;
			}
			else {
				if (future.getClass() != null) {
					notifyOfError(future.cause());
				}
				else {
					notifyOfError(new Exception("Connection could not be established."));
				}
			}
		}
	}
}
