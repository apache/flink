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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

/**
 * Simple container for the response of a handler, that contains either a response of type {@code P} if the incoming
 * request was handled successfully, otherwise it contains an error message and an associated error code.
 *
 * @param <P> type of successful response
 */
public final class HandlerResponse<P extends ResponseBody> {

	private final P response;
	private final String error;
	private final HttpResponseStatus errorCode;

	private HandlerResponse(P response) {
		this.response = response;
		this.error = null;
		this.errorCode = null;
	}

	private HandlerResponse(String error, HttpResponseStatus errorCode) {
		this.response = null;
		this.error = error;
		this.errorCode = errorCode;
	}

	/**
	 * Returns whether the request was handled successfully.
	 *
	 * @return boolean indicating whether the request was handled successfully
	 */
	public boolean wasSuccessful() {
		return response != null;
	}

	/**
	 * Returns the response for the successful processing of a request.
	 *
	 * @return response for a successful processing of a request
	 * @throws IllegalStateException if this HandlerResponse wasn't successful
	 */
	public P getResponse() {
		Preconditions.checkState(wasSuccessful());
		return response;
	}

	/**
	 * Returns the error message for the failed processing of a request.
	 *
	 * @return error message for a failed processing of a request
	 * @throws IllegalStateException if this HandlerResponse was successful
	 */
	public String getErrorMessage() {
		Preconditions.checkState(!wasSuccessful());
		return error;
	}

	/**
	 * Returns the http error code for the failed processing of a request.
	 *
	 * @return http error code for a failed processing of a request
	 * @throws IllegalStateException if this HandlerResponse was successful
	 */
	public HttpResponseStatus getErrorCode() {
		Preconditions.checkState(!wasSuccessful());
		return errorCode;
	}

	/**
	 * Creates a new {@link HandlerResponse} that contains the given response, signaling the successful processing of
	 * the request.
	 *
	 * @param response response to send
	 * @param <X>      type of response
	 * @return successful handler response containing the response
	 */
	public static <X extends ResponseBody> HandlerResponse<X> successful(@Nonnull X response) {
		Preconditions.checkNotNull(response);
		return new HandlerResponse<>(response);
	}

	/**
	 * Creates a new {@link HandlerResponse} that contains an error message and code, signaling the failed processing of
	 * a request.
	 *
	 * @param error     error message
	 * @param errorCode http error code
	 * @param <X>       type of the expected response
	 * @return failed handler response containing the error
	 */
	public static <X extends ResponseBody> HandlerResponse<X> error(@Nonnull String error, @Nonnull HttpResponseStatus errorCode) {
		Preconditions.checkNotNull(error);
		Preconditions.checkNotNull(errorCode);
		return new HandlerResponse<>(error, errorCode);
	}
}
