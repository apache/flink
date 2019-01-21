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

package org.apache.flink.runtime.rest.handler.files;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.AbstractHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.util.MimeTypes;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.LogFilenameQueryParameter;
import org.apache.flink.runtime.rest.messages.LogSizeQueryParameter;
import org.apache.flink.runtime.rest.messages.LogStartOffsetQueryParameter;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.util.FileOffsetRange;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultFileRegion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpChunkedInput;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedFile;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Rest handler which serves the log files of job manager.
 */
public class JobManagerLogFileHandler extends AbstractHandler<RestfulGateway, EmptyRequestBody, MessageParameters> {
	private final File rootPath;

	public JobManagerLogFileHandler(
		@Nonnull GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		@Nonnull Time timeout,
		@Nonnull Map<String, String> responseHeaders,
		@Nonnull UntypedResponseMessageHeaders<EmptyRequestBody, MessageParameters> untypedResponseMessageHeaders,
		File rootPath) {
		super(leaderRetriever, timeout, responseHeaders, untypedResponseMessageHeaders);
		this.rootPath = rootPath;
	}

	@Override
	protected CompletableFuture<Void> respondToRequest(
		ChannelHandlerContext ctx,
		HttpRequest httpRequest,
		HandlerRequest<EmptyRequestBody, MessageParameters> handlerRequest,
		RestfulGateway gateway) throws RestHandlerException {
		final List<String> filenames = handlerRequest.getQueryParameter(LogFilenameQueryParameter.class);
		final String filename = filenames.isEmpty() ? null : filenames.get(0);
		final List<Long> start = handlerRequest.getQueryParameter(LogStartOffsetQueryParameter.class);
		final List<Long> size = handlerRequest.getQueryParameter(LogSizeQueryParameter.class);
		FileOffsetRange range = (start.isEmpty() || size.isEmpty()) ?
			FileOffsetRange.MAX_FILE_OFFSET_RANGE : new FileOffsetRange(start.get(0), size.get(0) + start.get(0));
		final File file = filename != null ? new File(rootPath.getParent() + "/" + filename) : rootPath;
		final RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(file, "r");
		}
		catch (FileNotFoundException e) {
			throw new RestHandlerException("File Not Found", HttpResponseStatus.EXPECTATION_FAILED);
		}

		try {
			long startOffset = range.getStartOffsetForFile(file);
			long endOffset = range.getEndOffsetForFile(file);
			HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
			setContentTypeHeader(response, file);

			if (HttpHeaders.isKeepAlive(httpRequest)) {
				response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
			}
			HttpHeaders.setContentLength(response, range.getLengthForFile(file));

			// write the initial line and the header.
			ctx.write(response);

			// write the content.
			ChannelFuture lastContentFuture;
			if (ctx.pipeline().get(SslHandler.class) == null) {
				ctx.write(new DefaultFileRegion(raf.getChannel(), startOffset, endOffset - startOffset), ctx.newProgressivePromise());
				lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
			} else {
				lastContentFuture = ctx.writeAndFlush(new HttpChunkedInput(new ChunkedFile(raf, startOffset, endOffset - startOffset, 8192)),
					ctx.newProgressivePromise());
				// HttpChunkedInput will write the end marker (LastHttpContent) for us.
			}
			raf.close();

			// close the connection, if no keep-alive is needed
			if (!HttpHeaders.isKeepAlive(httpRequest)) {
				lastContentFuture.addListener(ChannelFutureListener.CLOSE);
			}
		} catch (Exception e) {
			logger.error("Failed to serve file.", e);
			throw new RestHandlerException("Failed to read file.", HttpResponseStatus.INTERNAL_SERVER_ERROR);
		}
		return CompletableFuture.completedFuture(null);
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
