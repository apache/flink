/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.LogFileNamePathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.LogSearchInfoParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.LogSearchLineParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerLogSearchInfoParameters;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultFileRegion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpChunkedInput;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedFile;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.Future;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.GenericFutureListener;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Rest handler which serves the search file of the {@link TaskExecutor}.
 */
public class TaskManagerLogSearchInfoHandler extends AbstractTaskManagerFileHandler<TaskManagerLogSearchInfoParameters> {
	private final String readWrite = "rw";
	private final String searchLogPrefix = "result";
	private final String searchLogSuffix = ".tmp";
	private final String lineSeparator = System.getProperty("line.separator", "\n");

	public TaskManagerLogSearchInfoHandler(
		@Nonnull GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		@Nonnull Time timeout,
		@Nonnull Map<String, String> responseHeaders,
		@Nonnull UntypedResponseMessageHeaders<EmptyRequestBody, TaskManagerLogSearchInfoParameters> untypedResponseMessageHeaders,
		@Nonnull GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
		@Nonnull TransientBlobService transientBlobService,
		@Nonnull Time cacheEntryDuration) {
		super(leaderRetriever, timeout, responseHeaders, untypedResponseMessageHeaders, resourceManagerGatewayRetriever, transientBlobService, cacheEntryDuration);

	}

	@Override
	protected CompletableFuture<Void> respondToRequest(
		ChannelHandlerContext ctx,
		HttpRequest httpRequest,
		HandlerRequest<EmptyRequestBody, TaskManagerLogSearchInfoParameters> handlerRequest,
		RestfulGateway gateway) throws RestHandlerException {

		final ResourceID taskManagerId = handlerRequest.getPathParameter(TaskManagerIdPathParameter.class);
		final String filename = handlerRequest.getPathParameter(LogFileNamePathParameter.class);
		final String searchWord = handlerRequest.getPathParameter(LogSearchInfoParameter.class);
		final String lines = handlerRequest.getPathParameter(LogSearchLineParameter.class);
		final Tuple2<ResourceID, String> taskManagerIdAndFileName = new Tuple2<>(taskManagerId, filename);

		final CompletableFuture<TransientBlobKey> blobKeyFuture = loadTaskManagerFile(taskManagerIdAndFileName);
		final CompletableFuture<Void> resultFuture = blobKeyFuture.thenAcceptAsync(
			(TransientBlobKey blobKey) -> {
				final File file;
				try {
					file = super.transientBlobService.getFile(blobKey);
				} catch (IOException e) {
					throw new CompletionException(new FlinkException("Could not retrieve file from transient blob store.", e));
				}

				try {
					searchLogFile(
						ctx,
						file,
						httpRequest,
						searchWord,
						lines
					);
				} catch (FlinkException e) {
					throw new CompletionException(new FlinkException("Could not transfer file to client.", e));
				}
			},
			ctx.executor());

		return resultFuture.whenComplete(
			(Void ignored, Throwable throwable) -> {
				if (throwable != null) {
					log.error("Failed to transfer file from TaskExecutor {}.", taskManagerId, throwable);

					final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);

					if (strippedThrowable instanceof UnknownTaskExecutorException) {
						throw new CompletionException(
							new NotFoundException(
								String.format("Failed to search log file from TaskExecutor %s because it was unknown.", taskManagerId),
								strippedThrowable));
					} else {
						throw new CompletionException(
							new FlinkException(
								String.format("Failed to search log file from TaskExecutor %s.", taskManagerId),
								strippedThrowable));
					}
				}
			});
	}

	private void searchLogFile(ChannelHandlerContext ctx, File file, HttpRequest httpRequest, String searchWord, String lines) throws FlinkException {
		final LineIterator lineIterator;
		final RandomAccessFile resultAccessFile;
		final File resultFile;

		try {
			resultFile = File.createTempFile(searchLogPrefix, searchLogSuffix, new File(file.getParent()));
			lineIterator = FileUtils.lineIterator(file, StandardCharsets.UTF_8.name());
			resultAccessFile = new RandomAccessFile(resultFile, readWrite);
		} catch (IOException e) {
			throw new FlinkException("Can not find file " + file + ".", e);
		}

		int num;
		try {
			num = Integer.parseInt(lines);
		} catch (NumberFormatException e) {
			num = 0;
		}

		try {
			boolean flag = false;
			int count = 0;
			while (lineIterator.hasNext()) {
				String line = lineIterator.nextLine();
				if (line.toLowerCase().contains(searchWord.toLowerCase())) {
					resultAccessFile.writeBytes(line);
					resultAccessFile.writeBytes(lineSeparator);
					flag = true;
					continue;
				}
				if (flag && (count < num)) {
					resultAccessFile.writeBytes(line);
					resultAccessFile.writeBytes(lineSeparator);
					count++;
				} else {
					flag = false;
					count = 0;
				}
			}

			if (!(resultAccessFile.length() > 0)) {
				resultAccessFile.writeBytes("No content matching the search keywords");
			}

			final long fileLength = resultAccessFile.length();
			final FileChannel fileChannel = resultAccessFile.getChannel();

			try {
				HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
				response.headers().set(CONTENT_TYPE, "text/plain");

				if (HttpHeaders.isKeepAlive(httpRequest)) {
					response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
				}
				HttpHeaders.setContentLength(response, fileLength);

				// write the initial line and the header.
				ctx.write(response);

				// write the content.
				final ChannelFuture lastContentFuture;
				final GenericFutureListener<Future<? super Void>> completionListener = future -> {
					fileChannel.close();
					resultAccessFile.close();
					lineIterator.close();
				};

				if (ctx.pipeline().get(SslHandler.class) == null) {
					ctx.write(
						new DefaultFileRegion(fileChannel, 0, fileLength), ctx.newProgressivePromise())
						.addListener(completionListener);
					lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
				} else {
					lastContentFuture = ctx
						.writeAndFlush(
							new HttpChunkedInput(new ChunkedFile(resultAccessFile, 0, fileLength, 8192)),
							ctx.newProgressivePromise())
						.addListener(completionListener);
				}

				// close the connection, if no keep-alive is needed
				if (!HttpHeaders.isKeepAlive(httpRequest)) {
					lastContentFuture.addListener(ChannelFutureListener.CLOSE);
				}

				//rm result log file
				resultFile.deleteOnExit();
			} catch (IOException ex) {
				fileChannel.close();
				throw ex;
			}
		} catch (IOException ioe) {
			try {
				resultAccessFile.close();
				lineIterator.close();
			} catch (IOException e) {
				throw new FlinkException("Close file error.", e);
			}

			throw new FlinkException("Could not search log file " + file + " to the client.", ioe);
		}
	}

	@Override
	protected CompletableFuture<TransientBlobKey> requestFileUpload(ResourceManagerGateway resourceManagerGateway, Tuple2<ResourceID, String> taskManagerIdAndFileName) {
		return resourceManagerGateway.requestTaskManagerFileUploadByName(taskManagerIdAndFileName.f0, taskManagerIdAndFileName.f1, timeout);
	}

	@Override
	protected String getFileName(HandlerRequest<EmptyRequestBody, TaskManagerLogSearchInfoParameters> handlerRequest) {
		return handlerRequest.getPathParameter(LogFileNamePathParameter.class);
	}
}
