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

package org.apache.flink.runtime.webmonitor.files;

/*****************************************************************************
 * This code is based on the "HttpStaticFileServerHandler" from the
 * Netty project's HTTP server example.
 *
 * See http://netty.io and
 * https://github.com/netty/netty/blob/4.0/example/src/main/java/io/netty/example/http/file/HttpStaticFileServerHandler.java
 *****************************************************************************/

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.router.KeepAliveWrite;
import io.netty.handler.codec.http.router.Routed;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import io.netty.util.CharsetUtil;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.webmonitor.JobManagerRetriever;
import org.apache.flink.runtime.webmonitor.handlers.HandlerRedirectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

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
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import static io.netty.handler.codec.http.HttpHeaders.Names.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.DATE;
import static io.netty.handler.codec.http.HttpHeaders.Names.EXPIRES;
import static io.netty.handler.codec.http.HttpHeaders.Names.IF_MODIFIED_SINCE;
import static io.netty.handler.codec.http.HttpHeaders.Names.LAST_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple file server handler that serves requests to web frontend's static files, such as
 * HTML, CSS, or JS files.
 *
 * <p>This code is based on the "HttpStaticFileServerHandler" from the Netty project's HTTP server
 * example.</p>
 */
@ChannelHandler.Sharable
public class StaticFileServerHandler extends SimpleChannelInboundHandler<Routed> {

	/** Default logger, if none is specified */
	private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(StaticFileServerHandler.class);

	/** Timezone in which this server answers its "if-modified" requests */
	private static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT");

	/** Date format for HTTP */
	public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";

	/** Be default, we allow files to be cached for 5 minutes */
	private static final int HTTP_CACHE_SECONDS = 300;

	// ------------------------------------------------------------------------

	/** JobManager retriever */
	private final JobManagerRetriever retriever;

	private final Future<String> localJobManagerAddressFuture;

	private final FiniteDuration timeout;

	/** The path in which the static documents are */
	private final File rootPath;

	/** Whether the web service has https enabled */
	private final boolean httpsEnabled;

	/** The log for all error reporting */
	private final Logger logger;

	private String localJobManagerAddress;

	public StaticFileServerHandler(
			JobManagerRetriever retriever,
			Future<String> localJobManagerAddressPromise,
			FiniteDuration timeout,
			File rootPath,
			boolean httpsEnabled) throws IOException {

		this(retriever, localJobManagerAddressPromise, timeout, rootPath, httpsEnabled, DEFAULT_LOGGER);
	}

	public StaticFileServerHandler(
			JobManagerRetriever retriever,
			Future<String> localJobManagerAddressFuture,
			FiniteDuration timeout,
			File rootPath,
			boolean httpsEnabled,
			Logger logger) throws IOException {

		this.retriever = checkNotNull(retriever);
		this.localJobManagerAddressFuture = checkNotNull(localJobManagerAddressFuture);
		this.timeout = checkNotNull(timeout);
		this.rootPath = checkNotNull(rootPath).getCanonicalFile();
		this.httpsEnabled = httpsEnabled;
		this.logger = checkNotNull(logger);
	}

	// ------------------------------------------------------------------------
	//  Responses to requests
	// ------------------------------------------------------------------------

	@Override
	public void channelRead0(ChannelHandlerContext ctx, Routed routed) throws Exception {
		if (localJobManagerAddressFuture.isCompleted()) {
			if (localJobManagerAddress == null) {
				localJobManagerAddress = Await.result(localJobManagerAddressFuture, timeout);
			}

			final HttpRequest request = routed.request();
			String requestPath = routed.path();

			// make sure we request the "index.html" in case there is a directory request
			if (requestPath.endsWith("/")) {
				requestPath = requestPath + "index.html";
			}

			// in case the files being accessed are logs or stdout files, find appropriate paths.
			if (requestPath.equals("/jobmanager/log") || requestPath.equals("/jobmanager/stdout")) {
				requestPath = "";
			}

			Option<Tuple2<ActorGateway, Integer>> jobManager = retriever.getJobManagerGatewayAndWebPort();

			if (jobManager.isDefined()) {
				// Redirect to leader if necessary
				String redirectAddress = HandlerRedirectUtils.getRedirectAddress(
					localJobManagerAddress, jobManager.get());

				if (redirectAddress != null) {
					HttpResponse redirect = HandlerRedirectUtils.getRedirectResponse(
						redirectAddress, requestPath, httpsEnabled);
					KeepAliveWrite.flush(ctx, routed.request(), redirect);
				}
				else {
					respondAsLeader(ctx, request, requestPath);
				}
			}
			else {
				KeepAliveWrite.flush(ctx, routed.request(), HandlerRedirectUtils.getUnavailableResponse());
			}
		} else {
			KeepAliveWrite.flush(ctx, routed.request(), HandlerRedirectUtils.getUnavailableResponse());
		}
	}

	/**
	 * Response when running with leading JobManager.
	 */
	private void respondAsLeader(ChannelHandlerContext ctx, HttpRequest request, String requestPath)
			throws IOException, ParseException, URISyntaxException {

		// convert to absolute path
		final File file = new File(rootPath, requestPath);

		if (!file.exists()) {
			// file does not exist. Try to load it with the classloader
			ClassLoader cl = StaticFileServerHandler.class.getClassLoader();

			try(InputStream resourceStream = cl.getResourceAsStream("web" + requestPath)) {
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
