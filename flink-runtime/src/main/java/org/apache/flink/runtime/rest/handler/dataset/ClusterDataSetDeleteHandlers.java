/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.dataset;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AbstractAsynchronousOperationHandlers;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationInfo;
import org.apache.flink.runtime.rest.handler.async.OperationKey;
import org.apache.flink.runtime.rest.handler.resourcemanager.AbstractResourceManagerHandler;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetDeleteStatusHeaders;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetDeleteStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetDeleteTriggerHeaders;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetDeleteTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetIdPathParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.SerializedThrowable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handler for {@link ClusterDataSetDeleteTriggerHeaders}.
 */
public class ClusterDataSetDeleteHandlers extends AbstractAsynchronousOperationHandlers<OperationKey, Void> {

	/**
	 * {@link TriggerHandler} implementation for the cluster data set delete operation.
	 */
	public class ClusterDataSetDeleteTriggerHandler extends TriggerHandler<RestfulGateway, EmptyRequestBody, ClusterDataSetDeleteTriggerMessageParameters> {

		private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

		public ClusterDataSetDeleteTriggerHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
			super(
				leaderRetriever,
				timeout,
				responseHeaders,
				ClusterDataSetDeleteTriggerHeaders.INSTANCE);
			this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
		}

		@Override
		protected CompletableFuture<Void> triggerOperation(HandlerRequest<EmptyRequestBody, ClusterDataSetDeleteTriggerMessageParameters> request, RestfulGateway gateway) throws RestHandlerException {
			final IntermediateDataSetID clusterPartitionId = request.getPathParameter(ClusterDataSetIdPathParameter.class);
			ResourceManagerGateway resourceManagerGateway = AbstractResourceManagerHandler.getResourceManagerGateway(resourceManagerGatewayRetriever);
			return resourceManagerGateway.releaseClusterPartitions(clusterPartitionId);
		}

		@Override
		protected OperationKey createOperationKey(HandlerRequest<EmptyRequestBody, ClusterDataSetDeleteTriggerMessageParameters> request) {
			return new OperationKey(new TriggerId());
		}
	}

	/**
	 * {@link StatusHandler} implementation for the cluster data set delete operation.
	 */
	public class ClusterDataSetDeleteStatusHandler extends StatusHandler<RestfulGateway, AsynchronousOperationInfo, ClusterDataSetDeleteStatusMessageParameters> {

		public ClusterDataSetDeleteStatusHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders) {
			super(
				leaderRetriever,
				timeout,
				responseHeaders,
				ClusterDataSetDeleteStatusHeaders.INSTANCE);
		}

		@Override
		protected OperationKey getOperationKey(HandlerRequest<EmptyRequestBody, ClusterDataSetDeleteStatusMessageParameters> request) {
			final TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);
			return new OperationKey(triggerId);
		}

		@Override
		protected AsynchronousOperationInfo exceptionalOperationResultResponse(Throwable throwable) {
			return AsynchronousOperationInfo.completeExceptional(new SerializedThrowable(throwable));
		}

		@Override
		protected AsynchronousOperationInfo operationResultResponse(Void ignored) {
			return AsynchronousOperationInfo.complete();
		}
	}
}
