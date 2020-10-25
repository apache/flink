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

package org.apache.flink.runtime.rest.messages.job.savepoints;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationStatusMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * These headers define the protocol for triggering a savepoint.
 */
public class SavepointStatusHeaders
		extends AsynchronousOperationStatusMessageHeaders<SavepointInfo, SavepointStatusMessageParameters> {

	private static final SavepointStatusHeaders INSTANCE = new SavepointStatusHeaders();

	private static final String URL = String.format(
		"/jobs/:%s/savepoints/:%s",
		JobIDPathParameter.KEY,
		TriggerIdPathParameter.KEY);

	private SavepointStatusHeaders() {
	}

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public SavepointStatusMessageParameters getUnresolvedMessageParameters() {
		return new SavepointStatusMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static SavepointStatusHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public Class<SavepointInfo> getValueClass() {
		return SavepointInfo.class;
	}

	@Override
	public String getDescription() {
		return "Returns the status of a savepoint operation.";
	}
}
