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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Returns an overview over all registered TaskManagers of the job.
 */
public class JobTaskManagersHandler extends AbstractTaskManagerHandler<RestfulGateway, EmptyRequestBody, TaskManagersInfo, JobMessageParameters> {

	private final ExecutionGraphCache executionGraphCache;
	private final GatewayRetriever<? extends RestfulGateway> leaderRetriever;

	public JobTaskManagersHandler(
		CompletableFuture<String> localRestAddress,
		GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		Time timeout,
		Map<String, String> responseHeaders,
		MessageHeaders<EmptyRequestBody, TaskManagersInfo, JobMessageParameters> messageHeaders,
		GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
		ExecutionGraphCache executionGraphCache) {
		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			resourceManagerGatewayRetriever);
		this.executionGraphCache = executionGraphCache;
		this.leaderRetriever = leaderRetriever;
	}

	@Override
	protected CompletableFuture<TaskManagersInfo> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, JobMessageParameters> request, @Nonnull ResourceManagerGateway gateway) throws RestHandlerException {
		JobID jobId = request.getPathParameter(JobIDPathParameter.class);
		CompletableFuture<Collection<TaskManagerInfo>> taskManagerInfosFuture = gateway.requestTaskManagerInfo(timeout);
		RestfulGateway restfulGateway = leaderRetriever.getNow().get();
		return taskManagerInfosFuture.thenApply(taskManagerInfos -> getJobTaskManagerInfo(taskManagerInfos, jobId, executionGraphCache, restfulGateway));
	}

	private static TaskManagersInfo getJobTaskManagerInfo(Collection<TaskManagerInfo> taskManagerInfos, JobID jobId, ExecutionGraphCache executionGraphCache, RestfulGateway restfulGateway){
		AccessExecutionGraph executionGraph = null;
		Collection<TaskManagerInfo> jobTaskManagerInfos = new HashSet<>();
		Map<String, TaskManagerInfo> resourceId2taskManagerInfo = new HashMap<>();
		TaskManagersInfo tms;
		try {
			executionGraph = executionGraphCache.getExecutionGraph(jobId, restfulGateway).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		if (executionGraph != null) {
			for (TaskManagerInfo t : taskManagerInfos) {
				resourceId2taskManagerInfo.put(t.getResourceId().toString(), t);
			}
			for (AccessExecutionJobVertex vertex : executionGraph.getVerticesTopologically()) {
				for (AccessExecutionVertex task : vertex.getTaskVertices()) {
					TaskManagerLocation taskManagerLocation = task.getCurrentAssignedResourceLocation();
					ResourceID tmId = taskManagerLocation.getResourceID();
					TaskManagerInfo taskManagerInfo = resourceId2taskManagerInfo.get(tmId.toString());
					if (taskManagerInfo != null) {
						jobTaskManagerInfos.add(taskManagerInfo);
					}
				}
			}
		}
		tms = new TaskManagersInfo(jobTaskManagerInfos);
		return tms;
	}
}
