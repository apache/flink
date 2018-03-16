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

package org.apache.flink.runtime.rest.util;

import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * An exception that is thrown if the failure of a REST operation was detected on the client.
 */
public class RestClientException extends FlinkException {

	private static final long serialVersionUID = 937914622022344423L;

	private final int responseCode;

	public RestClientException(String message, HttpResponseStatus responseStatus) {
		super(message);

		Preconditions.checkNotNull(responseStatus);
		responseCode = responseStatus.code();
	}

	public RestClientException(String message, Throwable cause, HttpResponseStatus responseStatus) {
		super(message, cause);

		responseCode = responseStatus.code();
	}

	public HttpResponseStatus getHttpResponseStatus() {
		return HttpResponseStatus.valueOf(responseCode);
	}
}
