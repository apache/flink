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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
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

import java.io.File;
import java.util.UUID;

/**
 * Simple code which handles all HTTP requests from the user, and passes them to the Router
 * handler directly if they do not involve file upload requests.
 * If a file is required to be uploaded, it handles the upload, and in the http request to the
 * next handler, passes the name of the file to the next handler.
 */
public class HttpRequestHandler extends SimpleChannelInboundHandler<HttpObject> {

	private HttpRequest request;

	private boolean readingChunks;

	private static final HttpDataFactory factory = new DefaultHttpDataFactory(true); // use disk

	private String requestPath;

	private HttpPostRequestDecoder decoder;

	private final File uploadDir;

	/**
	 * The directory where files should be uploaded.
	 */
	public HttpRequestHandler(File uploadDir) {
		this.uploadDir = uploadDir;
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		if (decoder != null) {
			decoder.cleanFiles();
		}
	}

	@Override
	public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
		if (msg instanceof HttpRequest) {
			request = (HttpRequest) msg;
			requestPath = new QueryStringDecoder(request.getUri()).path();
			if (request.getMethod() != HttpMethod.POST) {
				ctx.fireChannelRead(request);
			} else {
				// try to decode the posted data now.
				decoder = new HttpPostRequestDecoder(factory, request);
				readingChunks = HttpHeaders.isTransferEncodingChunked(request);
				if (readingChunks) {
					readingChunks = true;
				}
			}
		} else if (decoder != null && msg instanceof HttpContent) {
			// New chunk is received
			HttpContent chunk = (HttpContent) msg;
			decoder.offer(chunk);
			try {
				while (decoder.hasNext()) {
					InterfaceHttpData data = decoder.next();
					// IF SOMETHING EVER NEEDS POST PARAMETERS, THIS WILL BE THE PLACE TO HANDLE IT
					// all fields values will be passed with type Attribute.
					if (data.getHttpDataType() == HttpDataType.FileUpload) {
						DiskFileUpload file = (DiskFileUpload) data;
						if (file.isCompleted()) {
							String newName = UUID.randomUUID() + "_" + file.getFilename();
							file.renameTo(new File(uploadDir, newName));
							QueryStringEncoder encoder = new QueryStringEncoder(requestPath);
							encoder.addParam("file", newName);
							request.setUri(encoder.toString());
						}
					}
					data.release();
				}
			} catch (EndOfDataDecoderException e) {
				//
			}
			if (chunk instanceof LastHttpContent) {
				readingChunks = false;
				decoder.destroy();
				decoder = null;
				ctx.fireChannelRead(request);
			}
		}
	}
}
