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

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.AbstractHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;
import org.apache.flink.shaded.guava18.com.google.common.cache.RemovalNotification;
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
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.Future;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.GenericFutureListener;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Base class for serving files from the {@link TaskExecutor}.
 */
public abstract class AbstractTaskManagerFileHandler<M extends TaskManagerMessageParameters> extends AbstractHandler<RestfulGateway, EmptyRequestBody, M> {

	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
	private final TransientBlobService transientBlobService;

	private final LoadingCache<ResourceID, CompletableFuture<TransientBlobKey>> fileBlobKeys;

	protected AbstractTaskManagerFileHandler(
			@Nonnull CompletableFuture<String> localAddressFuture,
			@Nonnull GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			@Nonnull Time timeout,
			@Nonnull Map<String, String> responseHeaders,
			@Nonnull UntypedResponseMessageHeaders<EmptyRequestBody, M> untypedResponseMessageHeaders,
			@Nonnull GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
			@Nonnull TransientBlobService transientBlobService,
			@Nonnull Time cacheEntryDuration) {
		super(localAddressFuture, leaderRetriever, timeout, responseHeaders, untypedResponseMessageHeaders);

		this.resourceManagerGatewayRetriever = Preconditions.checkNotNull(resourceManagerGatewayRetriever);

		this.transientBlobService = Preconditions.checkNotNull(transientBlobService);

		this.fileBlobKeys = CacheBuilder
			.newBuilder()
			.expireAfterWrite(cacheEntryDuration.toMilliseconds(), TimeUnit.MILLISECONDS)
			.removalListener(this::removeBlob)
			.build(
				new CacheLoader<ResourceID, CompletableFuture<TransientBlobKey>>() {
					@Override
					public CompletableFuture<TransientBlobKey> load(ResourceID resourceId) throws Exception {
						return loadTaskManagerFile(resourceId);
					}
			});
	}

	@Override
	protected CompletableFuture<Void> respondToRequest(ChannelHandlerContext ctx, HttpRequest httpRequest, HandlerRequest<EmptyRequestBody, M> handlerRequest, RestfulGateway gateway) throws RestHandlerException {
		final ResourceID taskManagerId = handlerRequest.getPathParameter(TaskManagerIdPathParameter.class);

		final CompletableFuture<TransientBlobKey> blobKeyFuture;
		try {
			blobKeyFuture = fileBlobKeys.get(taskManagerId);
		} catch (ExecutionException e) {
			final Throwable cause = ExceptionUtils.stripExecutionException(e);
			throw new RestHandlerException("Could not retrieve file blob key future.", HttpResponseStatus.INTERNAL_SERVER_ERROR, cause);
		}

		final CompletableFuture<Void> resultFuture = blobKeyFuture.thenAcceptAsync(
			(TransientBlobKey blobKey) -> {
				final File file;
				try {
					file = transientBlobService.getFile(blobKey);
				} catch (IOException e) {
					throw new CompletionException(new FlinkException("Could not retrieve file from transient blob store.", e));
				}

				try {
					transferFile(
						ctx,
						file,
						httpRequest);
				} catch (FlinkException e) {
					throw new CompletionException(new FlinkException("Could not transfer file to client.", e));
				}
			},
			ctx.executor());

		return resultFuture.whenComplete(
			(Void ignored, Throwable throwable) -> {
				if (throwable != null) {
					log.error("Failed to transfer file from TaskExecutor {}.", taskManagerId, throwable);
					fileBlobKeys.invalidate(taskManagerId);

					final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);

					if (strippedThrowable instanceof UnknownTaskExecutorException) {
						throw new CompletionException(
							new NotFoundException(
								String.format("Failed to transfer file from TaskExecutor %s because it was unknown.", taskManagerId),
								strippedThrowable));
					} else {
						throw new CompletionException(
							new FlinkException(
								String.format("Failed to transfer file from TaskExecutor %s.", taskManagerId),
								strippedThrowable));
					}
				}
			});
	}

	protected abstract CompletableFuture<TransientBlobKey> requestFileUpload(ResourceManagerGateway resourceManagerGateway, ResourceID taskManagerResourceId);

	private CompletableFuture<TransientBlobKey> loadTaskManagerFile(ResourceID taskManagerResourceId) throws RestHandlerException {
		log.debug("Load file from TaskManager {}.", taskManagerResourceId);

		final ResourceManagerGateway resourceManagerGateway = resourceManagerGatewayRetriever
			.getNow()
			.orElseThrow(() -> {
				log.debug("Could not connect to ResourceManager right now.");
				return new RestHandlerException(
					"Cannot connect to ResourceManager right now. Please try to refresh.",
					HttpResponseStatus.NOT_FOUND);
			});

		return requestFileUpload(resourceManagerGateway, taskManagerResourceId);
	}

	private void removeBlob(RemovalNotification<ResourceID, CompletableFuture<TransientBlobKey>> removalNotification) {
		log.debug("Remove cached file for TaskExecutor {}.", removalNotification.getKey());

		final CompletableFuture<TransientBlobKey> value = removalNotification.getValue();

		if (value != null) {
			value.thenAccept(transientBlobService::deleteFromCache);
		}
	}

	private void transferFile(ChannelHandlerContext ctx, File file, HttpRequest httpRequest) throws FlinkException {
		final RandomAccessFile randomAccessFile;

		try {
			randomAccessFile = new RandomAccessFile(file, "r");
		} catch (FileNotFoundException e) {
			throw new FlinkException("Can not find file " + file + ".", e);
		}

		try {

			final long fileLength = randomAccessFile.length();
			final FileChannel fileChannel = randomAccessFile.getChannel();

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
					randomAccessFile.close();
				};

				if (ctx.pipeline().get(SslHandler.class) == null) {
					ctx.write(
						new DefaultFileRegion(fileChannel, 0, fileLength), ctx.newProgressivePromise())
						.addListener(completionListener);
					lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

				} else {
					lastContentFuture = ctx
						.writeAndFlush(
							new HttpChunkedInput(new ChunkedFile(randomAccessFile, 0, fileLength, 8192)),
							ctx.newProgressivePromise())
						.addListener(completionListener);

					// HttpChunkedInput will write the end marker (LastHttpContent) for us.
				}

				// close the connection, if no keep-alive is needed
				if (!HttpHeaders.isKeepAlive(httpRequest)) {
					lastContentFuture.addListener(ChannelFutureListener.CLOSE);
				}
			} catch (IOException ex) {
				fileChannel.close();
				throw ex;
			}
		} catch (IOException ioe) {
			try {
				randomAccessFile.close();
			} catch (IOException e) {
				throw new FlinkException("Close file or channel error.", e);
			}

			throw new FlinkException("Could not transfer file " + file + " to the client.", ioe);
		}
	}
}
