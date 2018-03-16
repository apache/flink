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

package org.apache.flink.runtime.rest.handler.legacy.files;

/*****************************************************************************
 * This code is based on the "HttpStaticFileServerHandler" from the
 * Netty project's HTTP server example.
 *
 * See http://netty.io and
 * https://github.com/netty/netty/blob/4.0/example/src/main/java/io/netty/example/http/file/HttpStaticFileServerHandler.java
 *****************************************************************************/

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.RedirectHandler;
import org.apache.flink.runtime.rest.handler.util.MimeTypes;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultFileRegion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpChunkedInput;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Routed;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedFile;
import org.apache.flink.shaded.netty4.io.netty.util.CharsetUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CACHE_CONTROL;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.DATE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.EXPIRES;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.IF_MODIFIED_SINCE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.LAST_MODIFIED;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple file server handler that serves requests to web frontend's static files, such as
 * HTML, CSS, or JS files.
 *
 * <p>This code is based on the "HttpStaticFileServerHandler" from the Netty project's HTTP server
 * example.</p>
 */
@ChannelHandler.Sharable
public class StaticFileServerHandler<T extends RestfulGateway> extends RedirectHandler<T> {

	/** Timezone in which this server answers its "if-modified" requests. */
	private static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT");

	/** Date format for HTTP. */
	public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";

	/** Be default, we allow files to be cached for 5 minutes. */
	private static final int HTTP_CACHE_SECONDS = 300;

	// ------------------------------------------------------------------------

	/** The path in which the static documents are. */
	private final File rootPath;

	public StaticFileServerHandler(
			GatewayRetriever<? extends T> retriever,
			CompletableFuture<String> localJobManagerAddressFuture,
			Time timeout,
			File rootPath) throws IOException {

		super(localJobManagerAddressFuture, retriever, timeout, Collections.emptyMap());

		this.rootPath = checkNotNull(rootPath).getCanonicalFile();
	}

	// ------------------------------------------------------------------------
	//  Responses to requests
	// ------------------------------------------------------------------------

	@Override
	protected void respondAsLeader(ChannelHandlerContext channelHandlerContext, Routed routed, T gateway) throws Exception {
		final HttpRequest request = routed.request();
		final String requestPath;

		// make sure we request the "index.html" in case there is a directory request
		if (routed.path().endsWith("/")) {
			requestPath = routed.path() + "index.html";
		}
		// in case the files being accessed are logs or stdout files, find appropriate paths.
		else if (routed.path().equals("/jobmanager/log") || routed.path().equals("/jobmanager/stdout")) {
			requestPath = "";
		} else {
			requestPath = routed.path();
		}

		respondToRequest(channelHandlerContext, request, requestPath);
	}

	/**
	 * Response when running with leading JobManager.
	 */
	private void respondToRequest(ChannelHandlerContext ctx, HttpRequest request, String requestPath)
			throws IOException, ParseException, URISyntaxException {

		// convert to absolute path
		final File file = new File(rootPath, requestPath);

		if (!file.exists()) {
			// file does not exist. Try to load it with the classloader
			ClassLoader cl = StaticFileServerHandler.class.getClassLoader();

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
								logger.debug("Loading missing file from classloader: {}", requestPath);
								// ensure that directory to file exists.
								file.getParentFile().mkdirs();
								Files.copy(resourceStream, file.toPath());

								success = true;
							}
						}
					}
				} catch (Throwable t) {
					logger.error("error while responding", t);
				} finally {
					if (!success) {
						logger.debug("Unable to load requested file {} from classloader", requestPath);
						sendError(ctx, NOT_FOUND);
						return;
					}
				}
			}
		}

		if (!file.exists() || file.isHidden() || file.isDirectory() || !file.isFile()) {
			sendError(ctx, NOT_FOUND);
			return;
		}

		if (!file.getCanonicalFile().toPath().startsWith(rootPath.toPath())) {
			sendError(ctx, NOT_FOUND);
			return;
		}

		// cache validation
		final String ifModifiedSince = request.headers().get(IF_MODIFIED_SINCE);
		if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
			SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
			Date ifModifiedSinceDate = dateFormatter.parse(ifModifiedSince);

			// Only compare up to the second because the datetime format we send to the client
			// does not have milliseconds
			long ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000;
			long fileLastModifiedSeconds = file.lastModified() / 1000;
			if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
				if (logger.isDebugEnabled()) {
					logger.debug("Responding 'NOT MODIFIED' for file '" + file.getAbsolutePath() + '\'');
				}

				sendNotModified(ctx);
				return;
			}
		}

		if (logger.isDebugEnabled()) {
			logger.debug("Responding with file '" + file.getAbsolutePath() + '\'');
		}

		// Don't need to close this manually. Netty's DefaultFileRegion will take care of it.
		final RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(file, "r");
		}
		catch (FileNotFoundException e) {
			sendError(ctx, NOT_FOUND);
			return;
		}

		try {
			long fileLength = raf.length();

			HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
			setContentTypeHeader(response, file);

			// since the log and out files are rapidly changing, we don't want to browser to cache them
			if (!(requestPath.contains("log") || requestPath.contains("out"))) {
				setDateAndCacheHeaders(response, file);
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
			logger.error("Failed to serve file.", e);
			sendError(ctx, INTERNAL_SERVER_ERROR);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		if (ctx.channel().isActive()) {
			logger.error("Caught exception", cause);
			sendError(ctx, INTERNAL_SERVER_ERROR);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities to encode headers and responses
	// ------------------------------------------------------------------------

	/**
	 * Writes a simple  error response message.
	 *
	 * @param ctx    The channel context to write the response to.
	 * @param status The response status.
	 */
	public static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
		FullHttpResponse response = new DefaultFullHttpResponse(
				HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + status + "\r\n", CharsetUtil.UTF_8));
		response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");

		// close the connection as soon as the error message is sent.
		ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
	}

	/**
	 * Send the "304 Not Modified" response. This response can be used when the
	 * file timestamp is the same as what the browser is sending up.
	 *
	 * @param ctx The channel context to write the response to.
	 */
	public static void sendNotModified(ChannelHandlerContext ctx) {
		FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, NOT_MODIFIED);
		setDateHeader(response);

		// close the connection as soon as the error message is sent.
		ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
	}

	/**
	 * Sets the "date" header for the HTTP response.
	 *
	 * @param response HTTP response
	 */
	public static void setDateHeader(FullHttpResponse response) {
		SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
		dateFormatter.setTimeZone(GMT_TIMEZONE);

		Calendar time = new GregorianCalendar();
		response.headers().set(DATE, dateFormatter.format(time.getTime()));
	}

	/**
	 * Sets the "date" and "cache" headers for the HTTP Response.
	 *
	 * @param response    The HTTP response object.
	 * @param fileToCache File to extract the modification timestamp from.
	 */
	public static void setDateAndCacheHeaders(HttpResponse response, File fileToCache) {
		SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
		dateFormatter.setTimeZone(GMT_TIMEZONE);

		// date header
		Calendar time = new GregorianCalendar();
		response.headers().set(DATE, dateFormatter.format(time.getTime()));

		// cache headers
		time.add(Calendar.SECOND, HTTP_CACHE_SECONDS);
		response.headers().set(EXPIRES, dateFormatter.format(time.getTime()));
		response.headers().set(CACHE_CONTROL, "private, max-age=" + HTTP_CACHE_SECONDS);
		response.headers().set(LAST_MODIFIED, dateFormatter.format(new Date(fileToCache.lastModified())));
	}

	/**
	 * Sets the content type header for the HTTP Response.
	 *
	 * @param response HTTP response
	 * @param file     file to extract content type
	 */
	public static void setContentTypeHeader(HttpResponse response, File file) {
		String mimeType = MimeTypes.getMimeTypeForFileName(file.getName());
		String mimeFinal = mimeType != null ? mimeType : MimeTypes.getDefaultMimeType();
		response.headers().set(CONTENT_TYPE, mimeFinal);
	}
}
