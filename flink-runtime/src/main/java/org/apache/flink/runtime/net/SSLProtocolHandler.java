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

package org.apache.flink.runtime.net;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.io.network.netty.NettyConfig;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

/**
 * Provides SSL security.
 *
 * Note that the SSL handler should be added first in the pipeline.  This handler
 * can (and should) be shared by numerous channels.
 */
@ChannelHandler.Sharable
public class SSLProtocolHandler extends ChannelHandlerAdapter {

	private final NettyConfig config;
	private final boolean clientMode;
	private final SSLContext sslContext;

	public SSLProtocolHandler(NettyConfig config, boolean clientMode) {
		this.config = config;
		this.clientMode = clientMode;
		try {
			// initialize the shared SSL context, which acts as a factory
			// for channel-specific objects.
			if (clientMode) {
				sslContext = config.createClientSSLContext();
			} else {
				sslContext = config.createServerSSLContext();
			}
		}
		catch(Exception ex) {
			throw new IllegalConfigurationException("Failed to initialize the SSL context", ex);
		}
	}

	@Override
	public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
		if (sslContext != null && (ctx.channel() instanceof SocketChannel)) {
			SSLEngine sslEngine;
			if(clientMode) {
				sslEngine = sslContext.createSSLEngine(
					config.getServerAddress().getHostAddress(),
					config.getServerPort());
				sslEngine.setUseClientMode(true);

				// Enable hostname verification for remote SSL connections
				if (!config.getServerAddress().isLoopbackAddress()) {
					SSLParameters newSSLParameters = sslEngine.getSSLParameters();
					config.setSSLVerifyHostname(newSSLParameters);
					sslEngine.setSSLParameters(newSSLParameters);
				}
			}
			else {
				sslEngine = sslContext.createSSLEngine();
				sslEngine.setUseClientMode(false);
			}

			// install a channel-specific instance of SSLHandler
			ctx.pipeline().replace(ctx.name(), "ssl", new SslHandler(sslEngine));
		}
		else {
			ctx.pipeline().remove(ctx.name());
		}
	}

	public static SslHandler getSSLHandler(Channel ch) {
		return (SslHandler) ch.pipeline().get("ssl");
	}
}
