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

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.taskmanager.TaskManagerLogListHandler;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.LogListInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Headers for the {@link TaskManagerLogListHandler}.
 */
public class TaskManagerLogsHeaders implements MessageHeaders<EmptyRequestBody, LogListInfo, TaskManagerMessageParameters> {

	private static final TaskManagerLogsHeaders INSTANCE = new TaskManagerLogsHeaders();

	private static final String URL = String.format("/taskmanagers/:%s/logs", TaskManagerIdPathParameter.KEY);

	private TaskManagerLogsHeaders() {}

	public static TaskManagerLogsHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public Class<LogListInfo> getResponseClass() {
		return LogListInfo.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public String getDescription() {
		return "Returns the list of log files on a TaskManager.";
	}

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public TaskManagerMessageParameters getUnresolvedMessageParameters() {
		return new TaskManagerMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}
}
