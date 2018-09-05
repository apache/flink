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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.job.SubtaskCurrentAttemptDetailsHandler;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.SubtaskIndexPathParameter;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Message headers for the {@link SubtaskCurrentAttemptDetailsHandler}.
 */
public class SubtaskCurrentAttemptDetailsHeaders implements MessageHeaders<EmptyRequestBody, SubtaskExecutionAttemptDetailsInfo, SubtaskMessageParameters> {

	private static final SubtaskCurrentAttemptDetailsHeaders INSTANCE = new SubtaskCurrentAttemptDetailsHeaders();

	public static final String URL = String.format(
		"/jobs/:%s/vertices/:%s/subtasks/:%s",
		JobIDPathParameter.KEY,
		JobVertexIdPathParameter.KEY,
		SubtaskIndexPathParameter.KEY);

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public Class<SubtaskExecutionAttemptDetailsInfo> getResponseClass() {
		return SubtaskExecutionAttemptDetailsInfo.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public SubtaskMessageParameters getUnresolvedMessageParameters() {
		return new SubtaskMessageParameters();
	}

	public static SubtaskCurrentAttemptDetailsHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public String getDescription() {
		return "Returns details of the current or latest execution attempt of a subtask.";
	}
}
