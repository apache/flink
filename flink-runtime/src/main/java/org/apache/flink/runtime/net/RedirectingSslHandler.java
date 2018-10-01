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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequestDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseEncoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** SSL handler which automatically redirects Non-SSL requests to SSL address. */
public class RedirectingSslHandler extends ByteToMessageDecoder {
	protected final Logger log = LoggerFactory.getLogger(getClass());

	/** the length of the ssl record header (in bytes). */
	private static final int SSL_RECORD_HEADER_LENGTH = 5;

	private final String confRedirectBaseUrl;
	private final CompletableFuture<String> redirectBaseUrl;
	private final SSLEngineFactory sslEngineFactory;

	public RedirectingSslHandler(
		String confRedirectHost,
		CompletableFuture<String> redirectBaseUrl,
		SSLEngineFactory sslEngineFactory) {
		this.confRedirectBaseUrl = "https://" + confRedirectHost + ":";
		this.redirectBaseUrl = redirectBaseUrl;
		this.sslEngineFactory = sslEngineFactory;
	}

	@Override
	protected void decode(ChannelHandlerContext context, ByteBuf in, List<Object> out) {
		if (in.readableBytes() < SSL_RECORD_HEADER_LENGTH) {
			return;
		}
		if (SslHandler.isEncrypted(in)) {
			handleSsl(context);
		} else {
			context.pipeline().replace(this, "http-decoder", new HttpRequestDecoder());
			context.pipeline().addAfter("http-decoder", "redirecting-non-ssl", newNonSslHandler());
		}
	}

	private void handleSsl(ChannelHandlerContext context) {
		SslHandler sslHandler = null;
		try {
			sslHandler = new SslHandler(sslEngineFactory.createSSLEngine());
			context.pipeline().replace(this, "ssl", sslHandler);
			sslHandler = null;
		} finally {
			// Since the SslHandler was not inserted into the pipeline the ownership of the SSLEngine was not
			// transferred to the SslHandler.
			if (sslHandler != null) {
				ReferenceCountUtil.safeRelease(sslHandler.engine());
			}
		}
	}

	private ChannelHandler newNonSslHandler() {
		return new ChannelInboundHandlerAdapter() {
			private HttpResponseEncoder encoder = new HttpResponseEncoder();

			@Override
			public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
				String path = msg instanceof HttpRequest ? ((HttpRequest) msg).uri() : "";
				String redirectUrl = getRedirectAddress(ctx) + path;
				log.debug("Received non-SSL request, returning redirect to {}", redirectUrl);
				FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
					HttpResponseStatus.MOVED_PERMANENTLY, Unpooled.EMPTY_BUFFER);
				response.headers().set(HttpHeaders.Names.LOCATION, redirectUrl);
				encoder.write(ctx, response, ctx.voidPromise());
				ctx.flush();
			}
		};
	}

	private String getRedirectAddress(ChannelHandlerContext ctx) throws Exception {
		return redirectBaseUrl.isDone() ? redirectBaseUrl.get() :
			confRedirectBaseUrl + ((InetSocketAddress) (ctx.channel()).localAddress()).getPort();
	}
}
