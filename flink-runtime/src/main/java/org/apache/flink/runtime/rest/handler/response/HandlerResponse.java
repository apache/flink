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
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

/**
 * Simple container for the response of a handler, that contains
 * a response of type {@code P} if the incoming request was handled successfully ({@link SuccessfulHandlerResponse}),
 * otherwise it contains an error message and an associated error code ({@link FailedHandlerResponse}).
 *
 * @param <P> type of successful response
 */
public interface HandlerResponse<P extends ResponseBody> {

	/**
	 * Creates a new {@link HandlerResponse} that contains the given response, signaling the successful processing of
	 * the request.
	 *
	 * @param response response to send
	 * @param <P>      type of response
	 * @return successful handler response containing the response
	 */
	static <P extends ResponseBody> SuccessfulHandlerResponse<P> successful(@Nonnull P response) {
		Preconditions.checkNotNull(response);
		return new SuccessfulHandlerResponse<>(response);
	}

	/**
	 * Creates a new {@link HandlerResponse} that contains an error message and code, signaling the failed processing of
	 * a request.
	 *
	 * @param error     error message
	 * @param errorCode http error code
	 * @param <P>       type of the expected response
	 * @return failed handler response containing the error
	 */
	static <P extends ResponseBody> FailedHandlerResponse<P> error(@Nonnull String error, @Nonnull HttpResponseStatus errorCode) {
		Preconditions.checkNotNull(error);
		Preconditions.checkNotNull(errorCode);
		return new FailedHandlerResponse<>(error, errorCode);
	}
}
