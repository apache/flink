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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AbstractAsynchronousOperationHandlers;
import org.apache.flink.runtime.rest.handler.async.OperationKey;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * HTTP handlers for asynchronous triggering of savepoints.
 *
 * <p>Drawing savepoints is a potentially long-running operation. To avoid blocking HTTP
 * connections, savepoints must be drawn in two steps. First, an HTTP request is issued to trigger
 * the savepoint asynchronously. The request will be assigned a {@link TriggerId},
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
 * savepoint directory as specified by {@link CheckpointingOptions#SAVEPOINT_DIRECTORY} will be used.
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
 *         "location": "/tmp/savepoint-d9813b-8a68e674325b"
 *     }
 * }
 * </pre>
 */
public class SavepointHandlers extends AbstractAsynchronousOperationHandlers<SavepointHandlers.SavepointKey, String> {

	@Nullable
	private final String defaultSavepointDir;

	public SavepointHandlers(@Nullable final String defaultSavepointDir) {
		this.defaultSavepointDir = defaultSavepointDir;
	}

	/**
	 * HTTP handler to trigger savepoints.
	 */
	public class SavepointTriggerHandler extends TriggerHandler<RestfulGateway, SavepointTriggerRequestBody, SavepointTriggerMessageParameters> {

		public SavepointTriggerHandler(
				final CompletableFuture<String> localRestAddress,
				final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
				final Time timeout,
				final Map<String, String> responseHeaders) {
			super(localRestAddress, leaderRetriever, timeout, responseHeaders, SavepointTriggerHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<String> triggerOperation(HandlerRequest<SavepointTriggerRequestBody, SavepointTriggerMessageParameters> request, RestfulGateway gateway) throws RestHandlerException {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			final String requestedTargetDirectory = request.getRequestBody().getTargetDirectory();

			if (requestedTargetDirectory == null && defaultSavepointDir == null) {
				throw new RestHandlerException(
						String.format("Config key [%s] is not set. Property [%s] must be provided.",
							CheckpointingOptions.SAVEPOINT_DIRECTORY.key(),
							SavepointTriggerRequestBody.FIELD_NAME_TARGET_DIRECTORY),
						HttpResponseStatus.BAD_REQUEST);
			}

			final String targetDirectory = requestedTargetDirectory != null ? requestedTargetDirectory : defaultSavepointDir;
			return gateway.triggerSavepoint(jobId, targetDirectory, RpcUtils.INF_TIMEOUT);
		}

		@Override
		protected SavepointKey createOperationKey(HandlerRequest<SavepointTriggerRequestBody, SavepointTriggerMessageParameters> request) {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			return SavepointKey.of(new TriggerId(), jobId);
		}
	}

	/**
	 * HTTP handler to query for the status of the savepoint.
	 */
	public class SavepointStatusHandler extends StatusHandler<RestfulGateway, SavepointInfo, SavepointStatusMessageParameters> {

		public SavepointStatusHandler(
				final CompletableFuture<String> localRestAddress,
				final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
				final Time timeout,
				final Map<String, String> responseHeaders) {
			super(localRestAddress, leaderRetriever, timeout, responseHeaders, SavepointStatusHeaders.getInstance());
		}

		@Override
		protected SavepointKey getOperationKey(HandlerRequest<EmptyRequestBody, SavepointStatusMessageParameters> request) {
			final TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			return SavepointKey.of(triggerId, jobId);
		}

		@Override
		protected SavepointInfo exceptionalOperationResultResponse(Throwable throwable) {
			return new SavepointInfo(null, new SerializedThrowable(throwable));
		}

		@Override
		protected SavepointInfo operationResultResponse(String operationResult) {
			return new SavepointInfo(operationResult, null);
		}
	}

	/**
	 * A pair of {@link JobID} and {@link TriggerId} used as a key to a hash based
	 * collection.
	 *
	 * @see AbstractAsynchronousOperationHandlers.CompletedOperationCache
	 */
	@Immutable
	public static class SavepointKey extends OperationKey {

		private final JobID jobId;

		private SavepointKey(final TriggerId triggerId, final JobID jobId) {
			super(triggerId);
			this.jobId = requireNonNull(jobId);
		}

		private static SavepointKey of(final TriggerId triggerId, final JobID jobId) {
			return new SavepointKey(triggerId, jobId);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			if (!super.equals(o)) {
				return false;
			}

			SavepointKey that = (SavepointKey) o;
			return Objects.equals(jobId, that.jobId);
		}

		@Override
		public int hashCode() {
			return Objects.hash(super.hashCode(), jobId);
		}
	}
}
