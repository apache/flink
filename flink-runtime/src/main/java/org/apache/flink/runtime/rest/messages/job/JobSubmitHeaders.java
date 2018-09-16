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

import org.apache.flink.runtime.rest.FileUploadHandler;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * These headers define the protocol for submitting a job to a flink cluster.
 */
public class JobSubmitHeaders implements MessageHeaders<JobSubmitRequestBody, JobSubmitResponseBody, EmptyMessageParameters> {

	private static final String URL = "/jobs";
	private static final JobSubmitHeaders INSTANCE = new JobSubmitHeaders();

	private JobSubmitHeaders() {
	}

	@Override
	public Class<JobSubmitRequestBody> getRequestClass() {
		return JobSubmitRequestBody.class;
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.POST;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	@Override
	public Class<JobSubmitResponseBody> getResponseClass() {
		return JobSubmitResponseBody.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.ACCEPTED;
	}

	@Override
	public EmptyMessageParameters getUnresolvedMessageParameters() {
		return EmptyMessageParameters.getInstance();
	}

	public static JobSubmitHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public String getDescription() {
		return "Submits a job. This call is primarily intended to be used by the Flink client. This call expects a " +
			"multipart/form-data request that consists of file uploads for the serialized JobGraph, jars and " +
			"distributed cache artifacts and an attribute named \"" + FileUploadHandler.HTTP_ATTRIBUTE_REQUEST + "\" for " +
			"the JSON payload.";
	}

	@Override
	public boolean acceptsFileUploads() {
		return true;
	}
}
