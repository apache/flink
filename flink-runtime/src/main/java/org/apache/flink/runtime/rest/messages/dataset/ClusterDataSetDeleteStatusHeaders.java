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
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationInfo;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationStatusMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Specification for retrieving the status for the delete operation of a cluster data set.
 *
 * @see ClusterDataSetDeleteStatusHeaders
 */
public class ClusterDataSetDeleteStatusHeaders extends AsynchronousOperationStatusMessageHeaders<AsynchronousOperationInfo, ClusterDataSetDeleteStatusMessageParameters> {

	public static final ClusterDataSetDeleteStatusHeaders INSTANCE = new ClusterDataSetDeleteStatusHeaders();

	private static final String URL = ClusterDataSetListHeaders.URL + "/delete/:" + TriggerIdPathParameter.KEY;

	private ClusterDataSetDeleteStatusHeaders() {
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public String getDescription() {
		return "Returns the status for the delete operation of a cluster data set.";
	}

	@Override
	public Class<AsynchronousOperationInfo> getValueClass() {
		return AsynchronousOperationInfo.class;
	}

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public ClusterDataSetDeleteStatusMessageParameters getUnresolvedMessageParameters() {
		return new ClusterDataSetDeleteStatusMessageParameters();
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
