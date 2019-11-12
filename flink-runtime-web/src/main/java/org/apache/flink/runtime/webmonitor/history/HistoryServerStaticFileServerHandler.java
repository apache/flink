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

import org.apache.flink.runtime.rest.handler.legacy.files.StaticFileServerHandler;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultFileRegion;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpChunkedInput;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedFile;

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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.IF_MODIFIED_SINCE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple file server handler used by the {@link HistoryServer} that serves requests to web frontend's static files,
 * such as HTML, CSS, JS or JSON files.
 *
 * <p>This code is based on the "HttpStaticFileServerHandler" from the Netty project's HTTP server
 * example.
 *
 * <p>This class is a copy of the {@link StaticFileServerHandler}. The differences are that the request path is
 * modified to end on ".json" if it does not have a filename extension; when "index.html" is requested we load
 * "index_hs.html" instead to inject the modified HistoryServer WebInterface and that the caching of the "/joboverview"
 * page is prevented.
 */
@ChannelHandler.Sharable
public class HistoryServerStaticFileServerHandler extends SimpleChannelInboundHandler<RoutedRequest> {

	/** Default logger, if none is specified. */
	private static final Logger LOG = LoggerFactory.getLogger(HistoryServerStaticFileServerHandler.class);

	// ------------------------------------------------------------------------

	/** The path in which the static documents are. */
	private final File rootPath;

	public HistoryServerStaticFileServerHandler(File rootPath) throws IOException {
		this.rootPath = checkNotNull(rootPath).getCanonicalFile();
	}

	// ------------------------------------------------------------------------
	//  Responses to requests
	// ------------------------------------------------------------------------

	@Override
	public void channelRead0(ChannelHandlerContext ctx, RoutedRequest routedRequest) throws Exception {
		String requestPath = routedRequest.getPath();

		respondWithFile(ctx, routedRequest.getRequest(), requestPath);
	}

	/**
	 * Response when running with leading JobManager.
	 */
	private void respondWithFile(ChannelHandlerContext ctx, HttpRequest request, String requestPath)
		throws IOException, ParseException {

		// make sure we request the "index.html" in case there is a directory request
		if (requestPath.endsWith("/")) {
			requestPath = requestPath + "index.html";
		}

		if (!requestPath.contains(".")) { // we assume that the path ends in either .html or .js
			requestPath = requestPath + ".json";
		}

		// convert to absolute path
		final File file = new File(rootPath, requestPath);

		if (!file.exists()) {
			// file does not exist. Try to load it with the classloader
			ClassLoader cl = HistoryServerStaticFileServerHandler.class.getClassLoader();

			try (InputStream resourceStream = cl.getResourceAsStream("web" + requestPath)) {
				boolean success = false;
				try {
					if (resourceStream != null) {
						URL root = cl.getResource("web");
						URL requested = cl.getResource("web" + requestPath);

						if (root != null && requested != null) {
							URI rootURI = new URI(root.getPath()).normalize();
							URI requestedURI = new URI(requested.getPath()).normalize();

							// Check that we don't load anything from outside of the
							// expected scope.
							if (!rootURI.relativize(requestedURI).equals(requestedURI)) {
								LOG.debug("Loading missing file from classloader: {}", requestPath);
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
						LOG.debug("Unable to load requested file {} from classloader", requestPath);
						HandlerUtils.sendErrorResponse(
							ctx,
							request,
							new ErrorResponseBody("File not found."),
							NOT_FOUND,
							Collections.emptyMap());
						return;
					}
				}
			}
		}

		if (!file.exists() || file.isHidden() || file.isDirectory() || !file.isFile()) {
			HandlerUtils.sendErrorResponse(
				ctx,
				request,
				new ErrorResponseBody("File not found."),
				NOT_FOUND,
				Collections.emptyMap());
			return;
		}

		if (!file.getCanonicalFile().toPath().startsWith(rootPath.toPath())) {
			HandlerUtils.sendErrorResponse(
				ctx,
				request,
				new ErrorResponseBody("File not found."),
				NOT_FOUND,
				Collections.emptyMap());
			return;
		}

		// cache validation
		final String ifModifiedSince = request.headers().get(IF_MODIFIED_SINCE);
		if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
			SimpleDateFormat dateFormatter = new SimpleDateFormat(StaticFileServerHandler.HTTP_DATE_FORMAT, Locale.US);
			Date ifModifiedSinceDate = dateFormatter.parse(ifModifiedSince);

			// Only compare up to the second because the datetime format we send to the client
			// does not have milliseconds
			long ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000;
			long fileLastModifiedSeconds = file.lastModified() / 1000;
			if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Responding 'NOT MODIFIED' for file '" + file.getAbsolutePath() + '\'');
				}

				StaticFileServerHandler.sendNotModified(ctx);
				return;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Responding with file '" + file.getAbsolutePath() + '\'');
		}

		// Don't need to close this manually. Netty's DefaultFileRegion will take care of it.
		final RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(file, "r");
		} catch (FileNotFoundException e) {
			HandlerUtils.sendErrorResponse(
				ctx,
				request,
				new ErrorResponseBody("File not found."),
				NOT_FOUND,
				Collections.emptyMap());
			return;
		}

		try {
			long fileLength = raf.length();

			HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
			StaticFileServerHandler.setContentTypeHeader(response, file);

			// the job overview should be updated as soon as possible
			if (!requestPath.equals("/joboverview.json")) {
				StaticFileServerHandler.setDateAndCacheHeaders(response, file);
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
		} catch (Exception e) {
			raf.close();
			LOG.error("Failed to serve file.", e);
			HandlerUtils.sendErrorResponse(
				ctx,
				request,
				new ErrorResponseBody("Internal server error."),
				INTERNAL_SERVER_ERROR,
				Collections.emptyMap());
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		if (ctx.channel().isActive()) {
			LOG.error("Caught exception", cause);
			HandlerUtils.sendErrorResponse(
				ctx,
				false,
				new ErrorResponseBody("Internal server error."),
				INTERNAL_SERVER_ERROR,
				Collections.emptyMap());
		}
	}
}
