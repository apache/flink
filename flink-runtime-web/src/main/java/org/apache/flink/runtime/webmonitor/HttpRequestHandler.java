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

package org.apache.flink.runtime.webmonitor;

/*****************************************************************************
 * This code is based on the "HttpUploadServerHandler" from the
 * Netty project's HTTP server example.
 *
 * See http://netty.io and
 * https://github.com/netty/netty/blob/netty-4.0.31.Final/example/src/main/java/io/netty/example/http/upload/HttpUploadServerHandler.java
 *****************************************************************************/

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.QueryStringEncoder;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.DiskFileUpload;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder.EndOfDataDecoderException;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.util.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;

/**
 * Simple code which handles all HTTP requests from the user, and passes them to the Router
 * handler directly if they do not involve file upload requests.
 * If a file is required to be uploaded, it handles the upload, and in the http request to the
 * next handler, passes the name of the file to the next handler.
 */
@ChannelHandler.Sharable
public class HttpRequestHandler extends SimpleChannelInboundHandler<HttpObject> {

	private static final Charset ENCODING = ConfigConstants.DEFAULT_CHARSET;

	private static final Logger LOG = LoggerFactory.getLogger(HttpRequestHandler.class);

	/** A decoder factory that always stores POST chunks on disk */
	private static final HttpDataFactory DATA_FACTORY = new DefaultHttpDataFactory(true);

	private final File tmpDir;

	private HttpRequest currentRequest;

	private HttpPostRequestDecoder currentDecoder;
	private String currentRequestPath;

	private final String secureCookie;

	public HttpRequestHandler(Configuration config, File tmpDir) {
		boolean securityEnabled = SecurityUtils.isSecurityEnabled(config);
		secureCookie = config.getString(ConfigConstants.SECURITY_COOKIE, null);
		if(securityEnabled && secureCookie == null) {
			throw new IllegalConfigurationException(ConfigConstants.SECURITY_COOKIE + " must be configured.");
		}
		this.tmpDir = tmpDir;
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		if (currentDecoder != null) {
			currentDecoder.cleanFiles();
		}
	}

	@Override
	public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
		try {
			if (msg instanceof HttpRequest) {
				currentRequest = (HttpRequest) msg;
				currentRequestPath = null;

				if (currentDecoder != null) {
					currentDecoder.destroy();
					currentDecoder = null;
				}

				if(secureCookie != null) {

					LOG.debug("Secure auth enabled. Going to validate auth header");

					//decode the request and see if Auth header is available
					String auth = currentRequest.headers().get("Authorization");
					LOG.debug("authorization header: {}", auth);

					if(auth == null || !auth.startsWith("Basic")) {
						String message = (auth == null) ? "Could not find authorization header" :
								"Only Basic authorization is supported";
						LOG.info(message+ ". Routing the request to prompt for credentials");

						ctx.writeAndFlush(buildUnauthorizedHTTPResponse());
						return;

					} else {
						String base64Credentials = auth.substring("Basic".length()).trim();
						String credentials = new String(
								DatatypeConverter.parseBase64Binary(base64Credentials),
								ENCODING);
						final String[] values = credentials.split(":",2);
						if(values.length != 2) {
							String message = "Credentials supplied does not have proper values";
							LOG.error(message);
							ctx.writeAndFlush(buildErrorHTTPResponse());
							return;
						}
						if(!values[1].equals(secureCookie)) {
							LOG.warn("User supplied cookie does not match with the configured cookie");
							ctx.writeAndFlush(buildUnauthorizedHTTPResponse());
							return;
						}
						LOG.debug("URL request is authorized since the cookie entered matches with the configured secure value");
					}
				}

				if (currentRequest.getMethod() == HttpMethod.GET || currentRequest.getMethod() == HttpMethod.DELETE) {
					// directly delegate to the router
					ctx.fireChannelRead(currentRequest);
				}
				else if (currentRequest.getMethod() == HttpMethod.POST) {
					// POST comes in multiple objects. First the request, then the contents
					// keep the request and path for the remaining objects of the POST request
					currentRequestPath = new QueryStringDecoder(currentRequest.getUri(), ENCODING).path();
					currentDecoder = new HttpPostRequestDecoder(DATA_FACTORY, currentRequest, ENCODING);
				}
				else {
					throw new IOException("Unsupported HTTP method: " + currentRequest.getMethod().name());
				}
			}
			else if (currentDecoder != null && msg instanceof HttpContent) {
				// received new chunk, give it to the current decoder
				HttpContent chunk = (HttpContent) msg;
				currentDecoder.offer(chunk);

				try {
					while (currentDecoder.hasNext()) {
						InterfaceHttpData data = currentDecoder.next();

						// IF SOMETHING EVER NEEDS POST PARAMETERS, THIS WILL BE THE PLACE TO HANDLE IT
						// all fields values will be passed with type Attribute.

						if (data.getHttpDataType() == HttpDataType.FileUpload) {
							DiskFileUpload file = (DiskFileUpload) data;
							if (file.isCompleted()) {
								String name = file.getFilename();

								File target = new File(tmpDir, UUID.randomUUID() + "_" + name);
								file.renameTo(target);

								QueryStringEncoder encoder = new QueryStringEncoder(currentRequestPath);
								encoder.addParam("filepath", target.getAbsolutePath());
								encoder.addParam("filename", name);

								currentRequest.setUri(encoder.toString());
							}
						}

						data.release();
					}
				}
				catch (EndOfDataDecoderException ignored) {}

				if (chunk instanceof LastHttpContent) {
					HttpRequest request = currentRequest;
					currentRequest = null;
					currentRequestPath = null;

					currentDecoder.destroy();
					currentDecoder = null;

					// fire next channel handler
					ctx.fireChannelRead(request);
				}
			}
		}
		catch (Throwable t) {
			currentRequest = null;
			currentRequestPath = null;

			if (currentDecoder != null) {
				currentDecoder.destroy();
				currentDecoder = null;
			}

			if (ctx.channel().isActive()) {
				byte[] bytes = ExceptionUtils.stringifyException(t).getBytes(ENCODING);

				DefaultFullHttpResponse response = new DefaultFullHttpResponse(
					HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR,
					Unpooled.wrappedBuffer(bytes));

				response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
				response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

				ctx.writeAndFlush(response);
			}
		}
	}

	private DefaultFullHttpResponse buildUnauthorizedHTTPResponse() {
		String message = "Unable to authorize the request. " +
				"Please provide secure cookie details to access the cluster.";
		byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
		DefaultFullHttpResponse response = new DefaultFullHttpResponse(
				HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED, Unpooled.wrappedBuffer(bytes));
		response.headers().set(HttpHeaders.Names.WWW_AUTHENTICATE,
				"Basic realm=\"To access web portal, please provide the secure cookie value in the password field. " +
						"User name can be of any value.\"");
		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());
		return response;
	}

	private DefaultFullHttpResponse buildErrorHTTPResponse() {
		String message = "Credentials supplied does not have proper values" ;
		byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
		DefaultFullHttpResponse response = new DefaultFullHttpResponse(
				HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST, Unpooled.wrappedBuffer(bytes));
		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());
		return response;
	}
}
