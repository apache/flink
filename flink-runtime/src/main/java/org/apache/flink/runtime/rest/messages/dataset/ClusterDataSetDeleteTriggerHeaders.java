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

package org.apache.flink.runtime.rest.messages.dataset;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationTriggerMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Specification for triggering the deletion of a cluster data set.
 *
 * @see ClusterDataSetDeleteStatusHeaders
 */
public class ClusterDataSetDeleteTriggerHeaders extends AsynchronousOperationTriggerMessageHeaders<EmptyRequestBody, ClusterDataSetDeleteTriggerMessageParameters> {

	public static final ClusterDataSetDeleteTriggerHeaders INSTANCE = new ClusterDataSetDeleteTriggerHeaders();

	private static final String URL = ClusterDataSetListHeaders.URL + "/:" + ClusterDataSetIdPathParameter.KEY;

	private ClusterDataSetDeleteTriggerHeaders() {
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.ACCEPTED;
	}

	@Override
	protected String getAsyncOperationDescription() {
		return "Triggers the deletion of a cluster data set.";
	}

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public ClusterDataSetDeleteTriggerMessageParameters getUnresolvedMessageParameters() {
		return new ClusterDataSetDeleteTriggerMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.DELETE;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}
}
