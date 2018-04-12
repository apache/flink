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

package org.apache.flink.runtime.rest.handler.job.rescaling;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobmaster.RescalingBehaviour;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AbstractAsynchronousOperationHandlers;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationInfo;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.RescalingParallelismQueryParameter;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Rest handler to trigger and poll the rescaling of a running job.
 */
public class RescalingHandlers extends AbstractAsynchronousOperationHandlers<AsynchronousJobOperationKey, Acknowledge> {

	/**
	 * Handler which triggers the rescaling of the specified job.
	 */
	public class RescalingTriggerHandler extends TriggerHandler<RestfulGateway, EmptyRequestBody, RescalingTriggerMessageParameters> {

		public RescalingTriggerHandler(
				CompletableFuture<String> localRestAddress,
				GatewayRetriever<? extends RestfulGateway> leaderRetriever,
				Time timeout,
				Map<String, String> responseHeaders) {
			super(
				localRestAddress,
				leaderRetriever,
				timeout,
				responseHeaders,
				RescalingTriggerHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<Acknowledge> triggerOperation(HandlerRequest<EmptyRequestBody, RescalingTriggerMessageParameters> request, RestfulGateway gateway) throws RestHandlerException {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			final List<Integer> queryParameter = request.getQueryParameter(RescalingParallelismQueryParameter.class);

			if (queryParameter.isEmpty()) {
				throw new RestHandlerException("No new parallelism was specified.", HttpResponseStatus.BAD_REQUEST);
			}

			final int newParallelism = queryParameter.get(0);

			final CompletableFuture<Acknowledge> rescalingFuture = gateway.rescaleJob(
				jobId,
				newParallelism,
				RescalingBehaviour.STRICT,
				RpcUtils.INF_TIMEOUT);

			return rescalingFuture;
		}

		@Override
		protected AsynchronousJobOperationKey createOperationKey(HandlerRequest<EmptyRequestBody, RescalingTriggerMessageParameters> request) {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			return AsynchronousJobOperationKey.of(new TriggerId(), jobId);
		}
	}

	/**
	 * Handler which reports the status of the rescaling operation.
	 */
	public class RescalingStatusHandler extends StatusHandler<RestfulGateway, AsynchronousOperationInfo, RescalingStatusMessageParameters> {

		public RescalingStatusHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders) {
			super(
				localRestAddress,
				leaderRetriever,
				timeout,
				responseHeaders,
				RescalingStatusHeaders.getInstance());
		}

		@Override
		protected AsynchronousJobOperationKey getOperationKey(HandlerRequest<EmptyRequestBody, RescalingStatusMessageParameters> request) {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			final TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);

			return AsynchronousJobOperationKey.of(triggerId, jobId);
		}

		@Override
		protected AsynchronousOperationInfo exceptionalOperationResultResponse(Throwable throwable) {
			return AsynchronousOperationInfo.completeExceptional(new SerializedThrowable(throwable));
		}

		@Override
		protected AsynchronousOperationInfo operationResultResponse(Acknowledge operationResult) {
			return AsynchronousOperationInfo.complete();
		}
	}
}
