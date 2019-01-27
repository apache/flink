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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.FileRangeMessageParameters;
import org.apache.flink.runtime.rest.messages.taskmanager.FileReadCountQueryParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.FileReadStartQueryParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.LogDetail;
import org.apache.flink.runtime.rest.messages.taskmanager.LogFileNamePathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.util.FileOffsetRange;
import org.apache.flink.runtime.util.FileReadDetail;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;
import org.apache.flink.shaded.guava18.com.google.common.cache.RemovalNotification;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Base class for serving files from the {@link TaskExecutor}.
 */
public abstract class TaskManagerFileRangeHandler extends AbstractTaskManagerHandler<RestfulGateway, EmptyRequestBody, LogDetail, FileRangeMessageParameters> {

	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
	private final TransientBlobService transientBlobService;

	private final LoadingCache<FileReadDetail, CompletableFuture<Tuple2<TransientBlobKey, Long>>> fileBlobKey2lengths;

	protected TaskManagerFileRangeHandler(
		@Nonnull CompletableFuture<String> localAddressFuture,
		@Nonnull GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		@Nonnull Time timeout,
		@Nonnull Map<String, String> responseHeaders,
		@Nonnull MessageHeaders<EmptyRequestBody, LogDetail, FileRangeMessageParameters> messageHeaders,
		@Nonnull GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
		@Nonnull TransientBlobService transientBlobService,
		@Nonnull Time cacheEntryDuration) {
		super(localAddressFuture, leaderRetriever, timeout, responseHeaders, messageHeaders, resourceManagerGatewayRetriever);

		this.resourceManagerGatewayRetriever = Preconditions.checkNotNull(resourceManagerGatewayRetriever);

		this.transientBlobService = Preconditions.checkNotNull(transientBlobService);

		this.fileBlobKey2lengths = CacheBuilder
			.newBuilder()
			.expireAfterWrite(cacheEntryDuration.toMilliseconds(), TimeUnit.MILLISECONDS)
			.removalListener(this::removeBlob)
			.build(
				new CacheLoader<FileReadDetail, CompletableFuture<Tuple2<TransientBlobKey, Long>>>() {
					@Override
					public CompletableFuture<Tuple2<TransientBlobKey, Long>> load(FileReadDetail fd) throws Exception {
						return loadTaskManagerFile(fd);
					}
				});
	}

	@Override
	protected CompletableFuture<LogDetail> handleRequest(
		@Nonnull HandlerRequest<EmptyRequestBody, FileRangeMessageParameters> handlerRequest,
		@Nonnull ResourceManagerGateway gateway) throws RestHandlerException {
		final ResourceID taskManagerId = handlerRequest.getPathParameter(TaskManagerIdPathParameter.class);
		String filename;
		final Long fileReadCount;
		final Long fileStart;
		try {
			filename = handlerRequest.getPathParameter(LogFileNamePathParameter.class);
		} catch (IllegalStateException e) {
			filename = null;
		}
		if (null == filename) {
			fileStart = null;
			fileReadCount = null;
		} else {
			List<Long> fileReadCountsTmp = handlerRequest.getQueryParameter(FileReadCountQueryParameter.class);
			fileReadCount = fileReadCountsTmp.isEmpty() ? FileOffsetRange.getSizeDefaultValue() : Math.abs(fileReadCountsTmp.get(0));
			List<Long> fileReadStartsTmp = handlerRequest.getQueryParameter(FileReadStartQueryParameter.class);
			fileStart = fileReadStartsTmp.isEmpty() ? FileOffsetRange.getStartDefaultValue() : fileReadStartsTmp.get(0);
		}

		final FileReadDetail fd = new FileReadDetail(taskManagerId, filename, fileStart, fileReadCount);
		final CompletableFuture<Tuple2<TransientBlobKey, Long>> blobKey2lengthFuture;
		try {
			blobKey2lengthFuture = fileBlobKey2lengths.get(fd);
		} catch (ExecutionException e) {
			final Throwable cause = ExceptionUtils.stripExecutionException(e);
			if (cause instanceof RestHandlerException) {
				throw (RestHandlerException) cause;
			} else {
				throw new RestHandlerException("Could not retrieve file blob key future.", HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
			}
		}

		final CompletableFuture<LogDetail> resultFuture = blobKey2lengthFuture.thenApplyAsync(
			blobKey2length -> {
				final File file;
				final LogDetail logDetail;
				try {
					file = transientBlobService.getFile(blobKey2length.f0);
				} catch (IOException e) {
					throw new CompletionException(new FlinkException("Could not retrieve file from transient blob store.", e));
				}

				try {
					String fileContent = org.apache.commons.io.FileUtils.readFileToString(file, "UTF-8");
					logDetail = new LogDetail(fileContent, blobKey2length.f1);
				} catch (Exception e) {
					throw new CompletionException(new FlinkException("Could not transfer file to client.", e));
				}
				return logDetail;
			});

		return resultFuture.thenApply(fileContent -> fileContent).exceptionally(
			(Throwable throwable) -> {
				final Throwable strippedThrowable = ExceptionUtils.stripExecutionException(throwable);

				if (strippedThrowable instanceof UnknownTaskExecutorException) {
					throw new CompletionException(
						new RestHandlerException(
							"Could not find TaskExecutor " + taskManagerId + '.',
							HttpResponseStatus.NOT_FOUND,
							strippedThrowable));
				} else {
					throw new CompletionException(strippedThrowable);
				}

			});
	}

	protected abstract CompletableFuture<Tuple2<TransientBlobKey, Long>> requestTaskManagerFileUploadReturnLength(ResourceManagerGateway resourceManagerGateway, FileReadDetail fd);

	private CompletableFuture<Tuple2<TransientBlobKey, Long>> loadTaskManagerFile(FileReadDetail fd) throws RestHandlerException {
		log.debug("Load file range from FileReadDetail:[{}].", fd);
		final ResourceManagerGateway resourceManagerGateway = getResourceManagerGateway(resourceManagerGatewayRetriever);
		return requestTaskManagerFileUploadReturnLength(resourceManagerGateway, fd);
	}

	private void removeBlob(RemovalNotification<FileReadDetail, CompletableFuture<Tuple2<TransientBlobKey, Long>>> removalNotification) {
		log.debug("Remove cached file for TaskExecutor {}.", removalNotification.getKey());

		final CompletableFuture<Tuple2<TransientBlobKey, Long>> value = removalNotification.getValue();

		if (value != null) {
			value.thenAccept(blobKey2length -> transientBlobService.deleteFromCache(blobKey2length.f0));
		}
	}
}
