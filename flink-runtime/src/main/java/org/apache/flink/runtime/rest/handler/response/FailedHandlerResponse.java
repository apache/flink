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

package org.apache.flink.runtime.rest.handler.response;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * {@link HandlerResponse} that contains the result of an unsuccessful request handling, consisting of an error message
 * and status code.
 *
 * @param <P> expected response type for a successful handling
 */
public class FailedHandlerResponse<P extends ResponseBody> implements HandlerResponse<P> {

	private final String error;
	private final HttpResponseStatus errorCode;

	FailedHandlerResponse(String error, HttpResponseStatus errorCode) {
		this.error = error;
		this.errorCode = errorCode;
	}

	/**
	 * Returns the error message for the failed processing of a request.
	 *
	 * @return error message for a failed processing of a request
	 */
	public String getErrorMessage() {
		return error;
	}

	/**
	 * Returns the http error code for the failed processing of a request.
	 *
	 * @return http error code for a failed processing of a request
	 */
	public HttpResponseStatus getErrorCode() {
		return errorCode;
	}
}
