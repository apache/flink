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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

/**
 * A simple netty protocol, providing a set of client and server handlers.
 */
public abstract class SimpleNettyProtocol implements NettyProtocol {

	@Override
	public ChannelHandler getServerChannelHandler() {
		return new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel channel) throws Exception {
				channel.pipeline().addLast(getServerChannelHandlers());
			}
		};
	}

	@Override
	public ChannelHandler getClientChannelHandler() {
		return new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel channel) throws Exception {
				channel.pipeline().addLast(getClientChannelHandlers());
			}
		};
	}

	/**
	 * Get the server-side channel handlers.
	 *
	 * Invoked at channel initialization time.  The handlers need not be sharable.
	 */
	public abstract ChannelHandler[] getServerChannelHandlers();

	/**
	 * Get the client-side channel handlers.
	 *
	 * Invoked at channel initialization time.  The handlers need not be sharable.
	 */
	public abstract ChannelHandler[] getClientChannelHandlers();

}
