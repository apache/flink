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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.util.JsonUtils;
import org.apache.flink.util.FlinkException;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * TaskManagerLogListHandler serves the request which gets all log files list.
 */
public class TaskManagerLogListHandler extends AbstractJsonRequestHandler{
	private static final String TASKMANAGER_LOG_FILE_LIST_PATH = "/taskmanagers/:taskmanagerid/loglist";
	private static final String TASKMANAGER_ID_KEY = "taskmanagerid";
	private final Time timeout;
	public TaskManagerLogListHandler(Executor executor, Time timeout) {
		super(executor);
		this.timeout = timeout;
	}

	@Override
	public String[] getPaths() {
		return new String[]{TASKMANAGER_LOG_FILE_LIST_PATH};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		final String unescapedString;
		try {
			unescapedString = URLDecoder.decode(pathParams.get(TASKMANAGER_ID_KEY), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return FutureUtils.completedExceptionally(new FlinkException("Could not decode task manager id: " + pathParams.get(TASKMANAGER_ID_KEY) + '.', e));
		}

		ResourceID resourceId = new ResourceID(unescapedString);
		CompletableFuture<Optional<Instance>> tmInstanceFuture = jobManagerGateway.requestTaskManagerInstance(resourceId, timeout);

		return tmInstanceFuture.thenApplyAsync(
			(Optional<Instance> optTaskManager) -> {
				try {
					return optTaskManager.get().getTaskManagerGateway().requestTaskManagerLogList(timeout).get();
				} catch (Exception e) {
					throw new CompletionException(new FlinkException("Could not get TaskManagers log file list.", e));
				}
			},
			executor)
			.thenApply(JsonUtils::createSortedFilesJson);
	}
}
