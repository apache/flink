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

import org.apache.flink.util.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

/**
 * Simple code which handles all HTTP requests from the user, and passes them to the Router
 * handler directly if they do not involve file upload requests.
 * If a file is required to be uploaded, it handles the upload, and in the http request to the
 * next handler, passes the name of the file to the next handler.
 */
@ChannelHandler.Sharable
public class HttpRequestHandler extends SimpleChannelInboundHandler<HttpObject> {

	private static final Charset ENCODING = Charset.forName("UTF-8");
	
	/** A decoder factory that always stores POST chunks on disk */
	private static final HttpDataFactory DATA_FACTORY = new DefaultHttpDataFactory(true);
	
	private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"));
	
		
	private HttpRequest currentRequest;

	private HttpPostRequestDecoder currentDecoder;
	
	private String currentRequestPath;


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
				
				if (currentRequest.getMethod() == HttpMethod.GET || currentRequest.getMethod() == HttpMethod.DELETE) {
					// directly delegate to the router
					ctx.fireChannelRead(currentRequest);
				}
				else if (currentRequest.getMethod() == HttpMethod.POST) {
					// POST comes in multiple objects. First the request, then the contents
					// keep the request and path for the remaining objects of the POST request
					currentRequestPath = new QueryStringDecoder(currentRequest.getUri()).path();
					currentDecoder = new HttpPostRequestDecoder(DATA_FACTORY, currentRequest);
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
								
								File target = new File(TMP_DIR, UUID.randomUUID() + "_" + name);
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
	
				response.headers().set(HttpHeaders.Names.CONTENT_ENCODING, "utf-8");
				response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
				response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

				ctx.writeAndFlush(response);
			}
		}
	}
}
