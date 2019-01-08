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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.MessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * {@link MessageHeaders} for {@link JarRunHandler}.
 */
public class JarRunHeaders implements MessageHeaders<JarRunRequestBody, JarRunResponseBody, JarRunMessageParameters> {

	private static final JarRunHeaders INSTANCE = new JarRunHeaders();

	private JarRunHeaders() {}

	@Override
	public Class<JarRunResponseBody> getResponseClass() {
		return JarRunResponseBody.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public Class<JarRunRequestBody> getRequestClass() {
		return JarRunRequestBody.class;
	}

	@Override
	public JarRunMessageParameters getUnresolvedMessageParameters() {
		return new JarRunMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.POST;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return "/jars/:" + JarIdPathParameter.KEY + "/run";
	}

	public static JarRunHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public String getDescription() {
		return "Submits a job by running a jar previously uploaded via '" + JarUploadHeaders.URL + "'.";
	}
}
