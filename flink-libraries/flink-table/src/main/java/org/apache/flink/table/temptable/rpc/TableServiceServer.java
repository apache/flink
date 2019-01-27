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

package org.apache.flink.table.temptable.rpc;

import org.apache.flink.table.temptable.TableService;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * A Netty RPC Server for TableService.
 */
public class TableServiceServer {
	private static final Logger LOG = LoggerFactory.getLogger(TableServiceServer.class);

	private TableService tableService;

	private ChannelFuture channelFuture;

	private EventLoopGroup masterGroup;

	private EventLoopGroup slaveGroup;

	private ServerBootstrap serverBootstrap;

	public TableService getTableService() {
		return tableService;
	}

	public void setTableService(TableService tableService) {
		this.tableService = tableService;
	}

	/**
	 * set up server and return port.
	 */
	public int bind() {
		if (masterGroup == null) {
			masterGroup = new NioEventLoopGroup();
			slaveGroup = new NioEventLoopGroup();
		}

		try {
			serverBootstrap = new ServerBootstrap();
			serverBootstrap.group(masterGroup, slaveGroup)
				.channel(NioServerSocketChannel.class)
				.option(ChannelOption.SO_BACKLOG, 1024)
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel socketChannel) throws Exception {
						socketChannel.pipeline().addLast(new LengthFieldBasedFrameDecoder(
							65536,
							0,
							Integer.BYTES,
							-Integer.BYTES,
							Integer.BYTES)
						);
						TableServiceServerHandler handler = new TableServiceServerHandler();
						handler.setTableService(tableService);
						socketChannel.pipeline().addLast(handler);
					}
				});

			channelFuture = serverBootstrap.bind(0).sync();
			return ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();

		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}

	}

	public void stop() {
		if (channelFuture != null) {
			try {
				channelFuture.channel().close().sync();
			} catch (InterruptedException e) {
				LOG.error(e.getMessage(), e);
			} finally {
				if (masterGroup != null) {
					masterGroup.shutdownGracefully();
				}
				if (slaveGroup != null) {
					slaveGroup.shutdownGracefully();
				}
			}
		}
	}

	public void start() {
		try {
			channelFuture.channel().closeFuture().sync();
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
}
