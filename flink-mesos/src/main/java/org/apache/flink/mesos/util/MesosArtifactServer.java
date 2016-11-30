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

package org.apache.flink.mesos.util;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.router.Handler;
import io.netty.handler.codec.http.router.Routed;
import io.netty.handler.codec.http.router.Router;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.CharsetUtil;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.net.SSLUtils;
import org.jets3t.service.utils.Mimetypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;

import static io.netty.handler.codec.http.HttpHeaders.Names.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.HEAD;
import static io.netty.handler.codec.http.HttpResponseStatus.GONE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


/**
 * A generic Mesos artifact server, designed specifically for use by the Mesos Fetcher.
 *
 * More information:
 * http://mesos.apache.org/documentation/latest/fetcher/
 * http://mesos.apache.org/documentation/latest/fetcher-cache-internals/
 */
public class MesosArtifactServer {

	private static final Logger LOG = LoggerFactory.getLogger(MesosArtifactServer.class);

	private final Router router;

	private ServerBootstrap bootstrap;

	private Channel serverChannel;

	private URL baseURL;

	private final SSLContext serverSSLContext;

	public MesosArtifactServer(String sessionID, String serverHostname, int configuredPort, Configuration config)
			throws Exception {
		if (configuredPort < 0 || configuredPort > 0xFFFF) {
			throw new IllegalArgumentException("File server port is invalid: " + configuredPort);
		}

		// Config to enable https access to the web-ui
		boolean enableSSL = config.getBoolean(
				ConfigConstants.MESOS_ARTIFACT_SERVER_SSL_ENABLED,
				ConfigConstants.DEFAULT_MESOS_ARTIFACT_SERVER_SSL_ENABLED) &&
				SSLUtils.getSSLEnabled(config);

		if (enableSSL) {
			LOG.info("Enabling ssl for the artifact server");
			try {
				serverSSLContext = SSLUtils.createSSLServerContext(config);
			} catch (Exception e) {
				throw new IOException("Failed to initialize SSLContext for the artifact server", e);
			}
		} else {
			serverSSLContext = null;
		}

		router = new Router();

		ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {

			@Override
			protected void initChannel(SocketChannel ch) {
				Handler handler = new Handler(router);

				// SSL should be the first handler in the pipeline
				if (serverSSLContext != null) {
					SSLEngine sslEngine = serverSSLContext.createSSLEngine();
					sslEngine.setUseClientMode(false);
					ch.pipeline().addLast("ssl", new SslHandler(sslEngine));
				}

				ch.pipeline()
					.addLast(new HttpServerCodec())
					.addLast(handler.name(), handler)
					.addLast(new UnknownFileHandler());
			}
		};

		NioEventLoopGroup bossGroup   = new NioEventLoopGroup(1);
		NioEventLoopGroup workerGroup = new NioEventLoopGroup();

		this.bootstrap = new ServerBootstrap();
		this.bootstrap
			.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class)
			.childHandler(initializer);

		Channel ch = this.bootstrap.bind(serverHostname, configuredPort).sync().channel();
		this.serverChannel = ch;

		InetSocketAddress bindAddress = (InetSocketAddress) ch.localAddress();
		String address = bindAddress.getAddress().getHostAddress();
		int port = bindAddress.getPort();

		String httpProtocol = (serverSSLContext != null) ? "https": "http";

		baseURL = new URL(httpProtocol, serverHostname, port, "/" + sessionID + "/");

		LOG.info("Mesos Artifact Server Base URL: {}, listening at {}:{}", baseURL, address, port);
	}

	/**
	 * Get the server port on which the artifact server is listening.
	 */
	public synchronized int getServerPort() {
		Channel server = this.serverChannel;
		if (server != null) {
			try {
				return ((InetSocketAddress) server.localAddress()).getPort();
			} catch (Exception e) {
				LOG.error("Cannot access local server port", e);
			}
		}
		return -1;
	}

	/**
	 * Adds a file to the artifact server.
	 * @param localFile the local file to serve.
	 * @param remoteFile the remote path with which to locate the file.
	 * @return the fully-qualified remote path to the file.
	 * @throws MalformedURLException if the remote path is invalid.
	 */
	public synchronized URL addFile(File localFile, String remoteFile) throws MalformedURLException {
		URL fileURL = new URL(baseURL, remoteFile);
		router.ANY(fileURL.getPath(), new VirtualFileServerHandler(localFile));
		return fileURL;
	}

	/**
	 * Stops the artifact server.
	 * @throws Exception
	 */
	public synchronized void stop() throws Exception {
		if (this.serverChannel != null) {
			this.serverChannel.close().awaitUninterruptibly();
			this.serverChannel = null;
		}
		if (bootstrap != null) {
			if (bootstrap.group() != null) {
				bootstrap.group().shutdownGracefully();
			}
			bootstrap = null;
		}
	}

	/**
	 * Handle HEAD and GET requests for a specific file.
	 */
	@ChannelHandler.Sharable
	public static class VirtualFileServerHandler extends SimpleChannelInboundHandler<Routed> {

		private final File file;

		public VirtualFileServerHandler(File file) {
			this.file = file;
			if(!file.exists()) {
				throw new IllegalArgumentException("no such file: " + file.getAbsolutePath());
			}
		}

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, Routed routed) throws Exception {

			HttpRequest request = routed.request();

			if (LOG.isDebugEnabled()) {
				LOG.debug("{} request for file '{}'", request.getMethod(), file.getAbsolutePath());
			}

			if(!(request.getMethod() == GET || request.getMethod() == HEAD)) {
				sendMethodNotAllowed(ctx);
				return;
			}

			final RandomAccessFile raf;
			try {
				raf = new RandomAccessFile(file, "r");
			}
			catch (FileNotFoundException e) {
				sendError(ctx, GONE);
				return;
			}
			try {
				long fileLength = raf.length();

				// compose the response
				HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
				if (HttpHeaders.isKeepAlive(request)) {
					response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
				}
				HttpHeaders.setHeader(response, CACHE_CONTROL, "private");
				HttpHeaders.setHeader(response, CONTENT_TYPE, Mimetypes.MIMETYPE_OCTET_STREAM);
				HttpHeaders.setContentLength(response, fileLength);

				ctx.write(response);

				if (request.getMethod() == GET) {
					// write the content.  Netty's DefaultFileRegion will close the file.
					ctx.write(new DefaultFileRegion(raf.getChannel(), 0, fileLength), ctx.newProgressivePromise());
				}
				else {
					// close the file immediately in HEAD case
					raf.close();
				}
				ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

				// close the connection, if no keep-alive is needed
				if (!HttpHeaders.isKeepAlive(request)) {
					lastContentFuture.addListener(ChannelFutureListener.CLOSE);
				}
			}
			catch(Exception ex) {
				raf.close();
				throw ex;
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			if (ctx.channel().isActive()) {
				LOG.error("Caught exception", cause);
				sendError(ctx, INTERNAL_SERVER_ERROR);
			}
		}

		/**
		 * Send the "405 Method Not Allowed" response.
		 *
		 * @param ctx The channel context to write the response to.
		 */
		private static void sendMethodNotAllowed(ChannelHandlerContext ctx) {
			FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, METHOD_NOT_ALLOWED);

			// close the connection as soon as the error message is sent.
			ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
		}

		/**
		 * Writes a simple  error response message.
		 *
		 * @param ctx    The channel context to write the response to.
		 * @param status The response status.
		 */
		private static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
			FullHttpResponse response = new DefaultFullHttpResponse(
				HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + status + "\r\n", CharsetUtil.UTF_8));
			HttpHeaders.setHeader(response, CONTENT_TYPE, "text/plain; charset=UTF-8");

			// close the connection as soon as the error message is sent.
			ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
		}
	}


	/**
	 * Handle a request for a non-existent file.
	 */
	@ChannelHandler.Sharable
	public static class UnknownFileHandler extends SimpleChannelInboundHandler<Object> {

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, Object message) {
			sendNotFound(ctx);
		}

		private static void sendNotFound(ChannelHandlerContext ctx) {
			FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, NOT_FOUND);

			// close the connection as soon as the error message is sent.
			ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
		}
	}

}
