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
import eu.stratosphere.runtime.io.network.RemoteReceiver;
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
import io.netty.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NettyConnectionManager {

	private static final Log LOG = LogFactory.getLog(NettyConnectionManager.class);

	private final ChannelManager channelManager;

	private final ServerBootstrap in;

	private final Bootstrap out;

	private final ConcurrentMap<RemoteReceiver, Object> outConnections;

	public NettyConnectionManager(ChannelManager channelManager, InetAddress bindAddress, int bindPort,
								int bufferSize, int numInThreads, int numOutThreads,
								int lowWaterMark, int highWaterMark) {
		this.outConnections = new ConcurrentHashMap<RemoteReceiver, Object>();
		this.channelManager = channelManager;

		// --------------------------------------------------------------------

		int defaultNumThreads = Math.max(Runtime.getRuntime().availableProcessors() / 4, 1);
		numInThreads = (numInThreads == -1) ? defaultNumThreads : numInThreads;
		numOutThreads = (numOutThreads == -1) ? defaultNumThreads : numOutThreads;
		LOG.info(String.format("Starting with %d incoming and %d outgoing connection threads.", numInThreads, numOutThreads));

		lowWaterMark = (lowWaterMark == -1) ? bufferSize / 2 : lowWaterMark;
		highWaterMark = (highWaterMark == -1) ? bufferSize : highWaterMark;
		LOG.info(String.format("Setting low water mark to %d and high water mark to %d bytes.", lowWaterMark, highWaterMark));

		// --------------------------------------------------------------------
		// server bootstrap (incoming connections)
		// --------------------------------------------------------------------
		this.in = new ServerBootstrap();
		this.in.group(new NioEventLoopGroup(numInThreads))
				.channel(NioServerSocketChannel.class)
				.localAddress(bindAddress, bindPort)
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel channel) throws Exception {
						channel.pipeline()
								.addLast(new InboundEnvelopeDecoder(NettyConnectionManager.this.channelManager))
								.addLast(new InboundEnvelopeDispatcherHandler(NettyConnectionManager.this.channelManager));
					}
				})
				.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(bufferSize))
				.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

		// --------------------------------------------------------------------
		// client bootstrap (outgoing connections)
		// --------------------------------------------------------------------
		this.out = new Bootstrap();
		this.out.group(new NioEventLoopGroup(numOutThreads))
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
			this.in.bind().sync();
		} catch (InterruptedException e) {
			throw new RuntimeException("Could not bind server socket for incoming connections.");
		}
	}

	public void shutdown() {
		Future<?> inShutdownFuture = this.in.group().shutdownGracefully();
		Future<?> outShutdownFuture = this.out.group().shutdownGracefully();

		try {
			inShutdownFuture.sync();
			outShutdownFuture.sync();
		} catch (InterruptedException e) {
			throw new RuntimeException("Could not properly shutdown connections.");
		}
	}

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

	// ------------------------------------------------------------------------

	private static final class ChannelInBuildup implements ChannelFutureListener {

		private Object lock = new Object();

		private volatile OutboundConnectionQueue channel;

		private volatile Throwable error;

		private int numRetries = 2;

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
