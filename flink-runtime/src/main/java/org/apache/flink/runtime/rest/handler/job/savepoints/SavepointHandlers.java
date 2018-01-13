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

package org.apache.flink.runtime.rest.handler.job.savepoints;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.SavepointTriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointResponseBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerId;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerResponseBody;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.types.Either;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * HTTP handlers for asynchronous triggering of savepoints.
 *
 * <p>Drawing savepoints is a potentially long-running operation. To avoid blocking HTTP
 * connections, savepoints must be drawn in two steps. First, an HTTP request is issued to trigger
 * the savepoint asynchronously. The request will be assigned a {@link SavepointTriggerId},
 * which is returned in the response body. Next, the returned id should be used to poll the status
 * of the savepoint until it is finished.
 *
 * <p>A savepoint is triggered by sending an HTTP {@code POST} request to
 * {@code /jobs/:jobid/savepoints}. The HTTP request may contain a JSON body to specify the target
 * directory of the savepoint, e.g.,
 * <pre>
 * { "target-directory": "/tmp" }
 * </pre>
 * If the body is omitted, or the field {@code target-property} is {@code null}, the default
 * savepoint directory as specified by {@link CoreOptions#SAVEPOINT_DIRECTORY} will be used.
 * As written above, the response will contain a request id, e.g.,
 * <pre>
 * { "request-id": "7d273f5a62eb4730b9dea8e833733c1e" }
 * </pre>
 *
 * <p>To poll for the status of an ongoing savepoint, an HTTP {@code GET} request is issued to
 * {@code /jobs/:jobid/savepoints/:savepointtriggerid}. If the specified savepoint is still ongoing,
 * the response will be
 * <pre>
 * {
 *     "status": {
 *         "id": "IN_PROGRESS"
 *     }
 * }
 * </pre>
 * If the specified savepoint has completed, the status id will transition to {@code COMPLETED}, and
 * the response will additionally contain information about the savepoint, such as the location:
 * <pre>
 * {
 *     "status": {
 *         "id": "COMPLETED"
 *     },
 *     "savepoint": {
 *         "request-id": "7d273f5a62eb4730b9dea8e833733c1e",
 *         "location": "/tmp/savepoint-d9813b-8a68e674325b"
 *     }
 * }
 * </pre>
 */
public class SavepointHandlers {

	private final CompletedSavepointCache completedSavepointCache = new CompletedSavepointCache();

	@Nullable
	private final String defaultSavepointDir;

	public SavepointHandlers(@Nullable final String defaultSavepointDir) {
		this.defaultSavepointDir = defaultSavepointDir;
	}

	/**
	 * HTTP handler to trigger savepoints.
	 */
	public class SavepointTriggerHandler
			extends AbstractRestHandler<RestfulGateway, SavepointTriggerRequestBody, SavepointTriggerResponseBody, SavepointTriggerMessageParameters> {

		public SavepointTriggerHandler(
				final CompletableFuture<String> localRestAddress,
				final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
				final Time timeout,
				final Map<String, String> responseHeaders) {
			super(localRestAddress, leaderRetriever, timeout, responseHeaders, SavepointTriggerHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<SavepointTriggerResponseBody> handleRequest(
				@Nonnull final HandlerRequest<SavepointTriggerRequestBody, SavepointTriggerMessageParameters> request,
				@Nonnull final RestfulGateway gateway) throws RestHandlerException {

			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			final String requestedTargetDirectory = request.getRequestBody().getTargetDirectory();

			if (requestedTargetDirectory == null && defaultSavepointDir == null) {
				return FutureUtils.completedExceptionally(
					new RestHandlerException(
						String.format("Config key [%s] is not set. Property [%s] must be provided.",
							CoreOptions.SAVEPOINT_DIRECTORY.key(),
							SavepointTriggerRequestBody.FIELD_NAME_TARGET_DIRECTORY),
						HttpResponseStatus.BAD_REQUEST));
			}

			final String targetDirectory = requestedTargetDirectory != null ? requestedTargetDirectory : defaultSavepointDir;
			final CompletableFuture<String> savepointLocationFuture =
				gateway.triggerSavepoint(jobId, targetDirectory, RpcUtils.INF_TIMEOUT);
			final SavepointTriggerId savepointTriggerId = new SavepointTriggerId();
			completedSavepointCache.registerOngoingSavepoint(
				SavepointKey.of(savepointTriggerId, jobId),
				savepointLocationFuture);
			return CompletableFuture.completedFuture(
				new SavepointTriggerResponseBody(savepointTriggerId));
		}
	}

	/**
	 * HTTP handler to query for the status of the savepoint.
	 */
	public class SavepointStatusHandler
			extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, SavepointResponseBody, SavepointStatusMessageParameters> {

		public SavepointStatusHandler(
				final CompletableFuture<String> localRestAddress,
				final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
				final Time timeout,
				final Map<String, String> responseHeaders) {
			super(localRestAddress, leaderRetriever, timeout, responseHeaders, SavepointStatusHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<SavepointResponseBody> handleRequest(
				@Nonnull final HandlerRequest<EmptyRequestBody, SavepointStatusMessageParameters> request,
				@Nonnull final RestfulGateway gateway) throws RestHandlerException {

			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			final SavepointTriggerId savepointTriggerId = request.getPathParameter(
				SavepointTriggerIdPathParameter.class);
			final Either<Throwable, String> savepointLocationOrError;
			try {
				savepointLocationOrError = completedSavepointCache.get(SavepointKey.of(
					savepointTriggerId, jobId));
			} catch (UnknownSavepointKeyException e) {
				return FutureUtils.completedExceptionally(
					new NotFoundException("Savepoint not found. Savepoint trigger id: " +
						savepointTriggerId + ", job id: " + jobId));
			}

			if (savepointLocationOrError != null) {
				if (savepointLocationOrError.isLeft()) {
					return CompletableFuture.completedFuture(new SavepointResponseBody(
						QueueStatus.completed(),
						new SavepointInfo(savepointTriggerId, null, new SerializedThrowable(
							savepointLocationOrError.left()))));
				} else {
					final String externalPointer = savepointLocationOrError.right();
					return CompletableFuture.completedFuture(new SavepointResponseBody(
						QueueStatus.completed(),
						new SavepointInfo(savepointTriggerId, externalPointer, null)));
				}
			} else {
				return CompletableFuture.completedFuture(SavepointResponseBody.inProgress());
			}
		}
	}

	/**
	 * Cache to manage ongoing savepoints.
	 *
	 * <p>The cache allows to register ongoing savepoints by calling
	 * {@link #registerOngoingSavepoint(SavepointKey, CompletableFuture)}, where the
	 * {@code CompletableFuture} contains the savepoint location. Completed savepoints will be
	 * removed from the cache automatically after a fixed timeout.
	 */
	@ThreadSafe
	static class CompletedSavepointCache {

		private static final long COMPLETED_SAVEPOINTS_CACHE_DURATION_SECONDS = 300;

		/**
		 * Stores SavepointKeys of ongoing savepoint.
		 * If the savepoint completes, it will be moved to {@link #completedSavepoints}.
		 */
		private final Set<SavepointKey> registeredSavepointTriggers = ConcurrentHashMap.newKeySet();

		/** Caches the location of completed savepoint. */
		private final Cache<SavepointKey, Either<Throwable, String>> completedSavepoints =
			CacheBuilder.newBuilder()
				.expireAfterWrite(COMPLETED_SAVEPOINTS_CACHE_DURATION_SECONDS, TimeUnit.SECONDS)
				.build();

		/**
		 * Registers an ongoing savepoint with the cache.
		 * @param savepointLocationFuture A future containing the savepoint location.
		 */
		void registerOngoingSavepoint(
				final SavepointKey savepointKey,
				final CompletableFuture<String> savepointLocationFuture) {
			registeredSavepointTriggers.add(savepointKey);
			savepointLocationFuture.whenComplete((savepointLocation, error) -> {
				if (error == null) {
					completedSavepoints.put(savepointKey, Either.Right(savepointLocation));
				} else {
					completedSavepoints.put(savepointKey, Either.Left(error));
				}
				registeredSavepointTriggers.remove(savepointKey);
			});
		}

		/**
		 * Returns the savepoint location or a {@code Throwable} if the {@code CompletableFuture}
		 * finished, otherwise {@code null}.
		 *
		 * @throws UnknownSavepointKeyException If the savepoint is not found, and there is no ongoing
		 *                                   savepoint under the provided key.
		 */
		@Nullable
		Either<Throwable, String> get(
				final SavepointKey savepointKey) throws UnknownSavepointKeyException {
			Either<Throwable, String> savepointLocationOrError = null;
			if (!registeredSavepointTriggers.contains(savepointKey)
				&& (savepointLocationOrError = completedSavepoints.getIfPresent(savepointKey)) == null) {
				throw new UnknownSavepointKeyException(savepointKey);
			}
			return savepointLocationOrError;
		}
	}

	/**
	 * A pair of {@link JobID} and {@link SavepointTriggerId} used as a key to a hash based
	 * collection.
	 *
	 * @see CompletedSavepointCache
	 */
	@Immutable
	static class SavepointKey {

		private final SavepointTriggerId savepointTriggerId;

		private final JobID jobId;

		private SavepointKey(final SavepointTriggerId savepointTriggerId, final JobID jobId) {
			this.savepointTriggerId = requireNonNull(savepointTriggerId);
			this.jobId = requireNonNull(jobId);
		}

		private static SavepointKey of(final SavepointTriggerId savepointTriggerId, final JobID jobId) {
			return new SavepointKey(savepointTriggerId, jobId);
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			final SavepointKey that = (SavepointKey) o;

			if (!savepointTriggerId.equals(that.savepointTriggerId)) {
				return false;
			}
			return jobId.equals(that.jobId);
		}

		@Override
		public int hashCode() {
			int result = savepointTriggerId.hashCode();
			result = 31 * result + jobId.hashCode();
			return result;
		}
	}

	/**
	 * Exception that indicates that there is no ongoing or completed savepoint for a given
	 * {@link JobID} and {@link SavepointTriggerId} pair.
	 */
	static class UnknownSavepointKeyException extends FlinkException {
		private static final long serialVersionUID = 1L;

		UnknownSavepointKeyException(final SavepointKey savepointKey) {
			super("No ongoing savepoints for " + savepointKey);
		}
	}
}
