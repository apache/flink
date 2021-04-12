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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.mesos.configuration.MesosOptions;
import org.apache.flink.runtime.io.network.netty.SSLHandlerFactory;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.rest.handler.router.Router;
import org.apache.flink.runtime.rest.handler.router.RouterHandler;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpServerCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedStream;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.flink.shaded.netty4.io.netty.util.CharsetUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import scala.Option;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CACHE_CONTROL;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod.GET;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod.HEAD;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.GONE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/** Implemenation of {@link MesosArtifactServer}. */
public class MesosArtifactServerImpl implements MesosArtifactServer {

    private static final Logger LOG = LoggerFactory.getLogger(MesosArtifactServerImpl.class);

    private final Router router;

    private ServerBootstrap bootstrap;

    private Channel serverChannel;

    private final URL baseURL;

    private final Map<Path, URL> paths = new HashMap<>();

    public MesosArtifactServerImpl(
            String prefix, String serverHostname, int configuredPort, Configuration config)
            throws Exception {
        if (configuredPort < 0 || configuredPort > 0xFFFF) {
            throw new IllegalArgumentException("File server port is invalid: " + configuredPort);
        }

        // Config to enable https access to the artifact server
        final boolean enableSSL =
                config.getBoolean(MesosOptions.ARTIFACT_SERVER_SSL_ENABLED)
                        && SSLUtils.isRestSSLEnabled(config);

        final SSLHandlerFactory sslFactory;
        if (enableSSL) {
            LOG.info("Enabling ssl for the artifact server");
            try {
                sslFactory = SSLUtils.createRestServerSSLEngineFactory(config);
            } catch (Exception e) {
                throw new IOException("Failed to initialize SSLContext for the artifact server", e);
            }
        } else {
            sslFactory = null;
        }

        router = new Router();

        ChannelInitializer<SocketChannel> initializer =
                new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) {
                        RouterHandler handler = new RouterHandler(router, new HashMap<>());

                        // SSL should be the first handler in the pipeline
                        if (sslFactory != null) {
                            ch.pipeline()
                                    .addLast("ssl", sslFactory.createNettySSLHandler(ch.alloc()));
                        }

                        ch.pipeline()
                                .addLast(new HttpServerCodec())
                                .addLast(new ChunkedWriteHandler())
                                .addLast(handler.getName(), handler)
                                .addLast(new UnknownFileHandler());
                    }
                };

        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
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

        String httpProtocol = (sslFactory != null) ? "https" : "http";

        baseURL = new URL(httpProtocol, serverHostname, port, "/" + prefix + "/");

        LOG.info("Mesos Artifact Server Base URL: {}, listening at {}:{}", baseURL, address, port);
    }

    @Override
    public synchronized URL addPath(Path path, Path remoteFile) throws IOException {
        if (paths.containsKey(remoteFile)) {
            throw new IllegalArgumentException("duplicate path registered");
        }
        if (remoteFile.isAbsolute()) {
            throw new IllegalArgumentException("not expecting an absolute path");
        }
        URL fileURL = new URL(baseURL, remoteFile.toString());
        router.addAny(fileURL.getPath(), new VirtualFileServerHandler(path));

        paths.put(remoteFile, fileURL);

        return fileURL;
    }

    @Override
    public synchronized Option<URL> resolve(Path remoteFile) {
        Option<URL> resolved = Option.apply(paths.get(remoteFile));
        return resolved;
    }

    @Override
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

    /** Handle HEAD and GET requests for a specific file. */
    @ChannelHandler.Sharable
    public static class VirtualFileServerHandler
            extends SimpleChannelInboundHandler<RoutedRequest> {

        private FileSystem fs;
        private Path path;

        public VirtualFileServerHandler(Path path) throws IOException {
            this.path = path;
            if (!path.isAbsolute()) {
                throw new IllegalArgumentException("path must be absolute: " + path.toString());
            }
            this.fs = path.getFileSystem();
            if (!fs.exists(path) || fs.getFileStatus(path).isDir()) {
                throw new IllegalArgumentException("no such file: " + path.toString());
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RoutedRequest routedRequest)
                throws Exception {

            HttpRequest request = routedRequest.getRequest();

            if (LOG.isDebugEnabled()) {
                LOG.debug("{} request for file '{}'", request.getMethod(), path);
            }

            if (!(request.getMethod() == GET || request.getMethod() == HEAD)) {
                sendMethodNotAllowed(ctx);
                return;
            }

            final FileStatus status;
            try {
                status = fs.getFileStatus(path);
            } catch (IOException e) {
                LOG.error("unable to stat file", e);
                sendError(ctx, GONE);
                return;
            }

            // compose the response
            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
            HttpHeaders.setHeader(response, CONNECTION, HttpHeaders.Values.CLOSE);
            HttpHeaders.setHeader(response, CACHE_CONTROL, "private");
            HttpHeaders.setHeader(response, CONTENT_TYPE, "application/octet-stream");
            HttpHeaders.setContentLength(response, status.getLen());

            ctx.write(response);

            if (request.getMethod() == GET) {
                // write the content.  Netty will close the stream.
                final FSDataInputStream stream = fs.open(path);
                try {
                    ctx.write(new ChunkedStream(stream));
                } catch (Exception e) {
                    stream.close();
                    throw e;
                }
            }

            ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
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
         * Writes a simple error response message.
         *
         * @param ctx The channel context to write the response to.
         * @param status The response status.
         */
        private static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
            FullHttpResponse response =
                    new DefaultFullHttpResponse(
                            HTTP_1_1,
                            status,
                            Unpooled.copiedBuffer(
                                    "Failure: " + status + "\r\n", CharsetUtil.UTF_8));
            HttpHeaders.setHeader(response, CONTENT_TYPE, "text/plain; charset=UTF-8");

            // close the connection as soon as the error message is sent.
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    /** Handle a request for a non-existent file. */
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
