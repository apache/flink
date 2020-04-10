/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.taskmanager.TaskManagerLogSearchInfoHandler;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;

/**
 * Headers for the {@link TaskManagerLogSearchInfoHandler}.
 */
public class TaskManagerLogSearchInfoHeaders implements
	UntypedResponseMessageHeaders<EmptyRequestBody, TaskManagerLogSearchInfoParameters> {

	private static final TaskManagerLogSearchInfoHeaders INSTANCE = new TaskManagerLogSearchInfoHeaders();

	private static final String URL = String.format("/taskmanagers/:%s/logs/:%s/:%s/:%s", TaskManagerIdPathParameter.KEY, LogFileNamePathParameter.KEY, LogSearchInfoParameter.KEY, LogSearchLineParameter.KEY);

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public TaskManagerLogSearchInfoParameters getUnresolvedMessageParameters() {
		return new TaskManagerLogSearchInfoParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static TaskManagerLogSearchInfoHeaders getInstance() {
		return INSTANCE;
	}
}
