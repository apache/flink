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
package org.apache.flink.runtime.webmonitor.history;

/*****************************************************************************
 * This code is based on the "HttpStaticFileServerHandler" from the
 * Netty project's HTTP server example.
 *
 * See http://netty.io and
 * https://github.com/netty/netty/blob/4.0/example/src/main/java/io/netty/example/http/file/HttpStaticFileServerHandler.java
 *****************************************************************************/

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.router.Routed;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import org.apache.flink.runtime.webmonitor.utils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.util.Date;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.IF_MODIFIED_SINCE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.flink.util.Preconditions.checkNotNull;

@ChannelHandler.Sharable
public class HistoryServerStaticFileServerHandler extends SimpleChannelInboundHandler<Routed> {

	private static final Logger LOG = LoggerFactory.getLogger(HistoryServerStaticFileServerHandler.class);

	// ------------------------------------------------------------------------

	/** The path in which the static documents are */
	private final File rootPath;

	public HistoryServerStaticFileServerHandler(File rootPath) throws IOException {
		this.rootPath = checkNotNull(rootPath).getCanonicalFile();
	}

	// ------------------------------------------------------------------------
	//  Responses to requests
	// ------------------------------------------------------------------------

	@Override
	public void channelRead0(ChannelHandlerContext ctx, Routed routed) throws Exception {
		final HttpRequest request = routed.request();
		String requestPath = routed.path();

		// make sure we request the "index.html" in case there is a directory request
		if (requestPath.endsWith("/")) {
			requestPath = requestPath + "index.html";
		}

		if (!requestPath.contains(".")) {
			requestPath += ".json";
		}

		// convert to absolute path
		final File file = new File(rootPath, requestPath);

		if (!file.exists()) {
			// file does not exist. Try to load it with the classloader
			ClassLoader cl = getClass().getClassLoader();

			String pathToLoad = requestPath;
			pathToLoad = pathToLoad.replace("index.html", "index2.html");

			try (InputStream resourceStream = cl.getResourceAsStream("web" + pathToLoad)) {
				boolean success = false;
				try {
					if (resourceStream != null) {
						URL root = cl.getResource("web");
						URL requested = cl.getResource("web" + pathToLoad);

						if (root != null && requested != null) {
							URI rootURI = new URI(root.getPath()).normalize();
							URI requestedURI = new URI(requested.getPath()).normalize();

							// Check that we don't load anything from outside of the
							// expected scope.
							if (!rootURI.relativize(requestedURI).equals(requestedURI)) {
								LOG.debug("Loading missing file from classloader: {}", pathToLoad);
								// ensure that directory to file exists.
								file.getParentFile().mkdirs();
								Files.copy(resourceStream, file.toPath());

								success = true;
							}
						}
					}
				} catch (Throwable t) {
					LOG.error("error while responding", t);
				} finally {
					if (!success) {
						LOG.debug("Unable to load requested file {} from classloader", pathToLoad);
						HttpUtils.sendError(ctx, NOT_FOUND);
						return;
					}
				}
			}
		}

		if (!file.exists() || file.isHidden() || file.isDirectory() || !file.isFile()) {
			HttpUtils.sendError(ctx, NOT_FOUND);
			return;
		}

		if (!file.getCanonicalFile().toPath().startsWith(rootPath.toPath())) {
			HttpUtils.sendError(ctx, NOT_FOUND);
			return;
		}

		// cache validation
		final String ifModifiedSince = request.headers().get(IF_MODIFIED_SINCE);
		if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
			Date ifModifiedSinceDate = HttpUtils.convertTimestampToDate(ifModifiedSince);

			// Only compare up to the second because the datetime format we send to the client
			// does not have milliseconds
			long ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000;
			long fileLastModifiedSeconds = file.lastModified() / 1000;
			if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Responding 'NOT MODIFIED' for file '" + file.getAbsolutePath() + '\'');
				}

				HttpUtils.sendNotModified(ctx);
				return;
			}
		}

		LOG.debug("Responding with file '" + file.getAbsolutePath() + '\'');

		// Don't need to close this manually. Netty's DefaultFileRegion will take care of it.
		final RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(file, "r");
		} catch (FileNotFoundException e) {
			HttpUtils.sendError(ctx, NOT_FOUND);
			return;
		}
		long fileLength = raf.length();

		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
		HttpUtils.setContentTypeHeader(response, file);

		// the job overview should update immediately
		if (!requestPath.equals("/joboverview")) {
			HttpUtils.setDateAndCacheHeaders(response, file);
		}

		if (HttpHeaders.isKeepAlive(request)) {
			response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
		}
		HttpHeaders.setContentLength(response, fileLength);

		// write the initial line and the header.
		ctx.write(response);

		// write the content.
		ChannelFuture lastContentFuture;
		if (ctx.pipeline().get(SslHandler.class) == null) {
			ctx.write(new DefaultFileRegion(raf.getChannel(), 0, fileLength), ctx.newProgressivePromise());
			lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
		} else {
			lastContentFuture = ctx.writeAndFlush(new HttpChunkedInput(new ChunkedFile(raf, 0, fileLength, 8192)),
				ctx.newProgressivePromise());
			// HttpChunkedInput will write the end marker (LastHttpContent) for us.
		}

		// close the connection, if no keep-alive is needed
		if (!HttpHeaders.isKeepAlive(request)) {
			lastContentFuture.addListener(ChannelFutureListener.CLOSE);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		if (ctx.channel().isActive()) {
			LOG.error("Caught exception", cause);
			HttpUtils.sendError(ctx, INTERNAL_SERVER_ERROR);
		}
	}
}
