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
package org.apache.flink.runtime.webmonitor.utils;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import org.apache.flink.runtime.webmonitor.files.MimeTypes;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import static io.netty.handler.codec.http.HttpHeaders.Names.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.DATE;
import static io.netty.handler.codec.http.HttpHeaders.Names.EXPIRES;
import static io.netty.handler.codec.http.HttpHeaders.Names.LAST_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpUtils {

	/** Timezone in which this server answers its "if-modified" requests */
	private static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT");

	/** Date format for HTTP */
	private static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";

	/** Be default, we allow files to be cached for 5 minutes */
	private static final int HTTP_CACHE_SECONDS = 300;

	// ------------------------------------------------------------------------
	//  Utilities to encode headers and responses
	// ------------------------------------------------------------------------

	public static Date convertTimestampToDate(String timestamp) throws ParseException {
		SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
		return dateFormatter.parse(timestamp);
	}
	
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
	private static void setDateHeader(FullHttpResponse response) {
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
