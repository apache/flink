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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.async.AbstractAsynchronousOperationHandlers;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationInfo;
import org.apache.flink.runtime.rest.handler.async.OperationKey;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalRequest;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalTriggerHeaders;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.SerializedThrowable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handlers to trigger the disposal of a savepoint.
 */
public class SavepointDisposalHandlers extends AbstractAsynchronousOperationHandlers<OperationKey, Acknowledge> {

	/**
	 * {@link TriggerHandler} implementation for the savepoint disposal operation.
	 */
	public class SavepointDisposalTriggerHandler extends TriggerHandler<RestfulGateway, SavepointDisposalRequest, EmptyMessageParameters> {

		public SavepointDisposalTriggerHandler(
				CompletableFuture<String> localRestAddress,
				GatewayRetriever<? extends RestfulGateway> leaderRetriever,
				Time timeout,
				Map<String, String> responseHeaders) {
			super(
				localRestAddress,
				leaderRetriever,
				timeout,
				responseHeaders,
				SavepointDisposalTriggerHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<Acknowledge> triggerOperation(HandlerRequest<SavepointDisposalRequest, EmptyMessageParameters> request, RestfulGateway gateway) {
			final String savepointPath = request.getRequestBody().getSavepointPath();
			return gateway.disposeSavepoint(savepointPath, RpcUtils.INF_TIMEOUT);
		}

		@Override
		protected OperationKey createOperationKey(HandlerRequest<SavepointDisposalRequest, EmptyMessageParameters> request) {
			return new OperationKey(new TriggerId());
		}
	}

	/**
	 * {@link StatusHandler} implementation for the savepoint disposal operation.
	 */
	public class SavepointDisposalStatusHandler extends StatusHandler<RestfulGateway, AsynchronousOperationInfo, SavepointDisposalStatusMessageParameters> {

		public SavepointDisposalStatusHandler(
				CompletableFuture<String> localRestAddress,
				GatewayRetriever<? extends RestfulGateway> leaderRetriever,
				Time timeout,
				Map<String, String> responseHeaders) {
			super(
				localRestAddress,
				leaderRetriever,
				timeout,
				responseHeaders,
				SavepointDisposalStatusHeaders.getInstance());
		}

		@Override
		protected OperationKey getOperationKey(HandlerRequest<EmptyRequestBody, SavepointDisposalStatusMessageParameters> request) {
			final TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);
			return new OperationKey(triggerId);
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
