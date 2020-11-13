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

import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * An exception that is thrown if the failure of a REST operation was detected by a handler.
 */
public class RestHandlerException extends FlinkException {
	private static final long serialVersionUID = -1358206297964070876L;

	private final int responseCode;

	// This flag indicates whether the AbstractHandler should log about this exception on INFO level or not.
	private final LoggingBehavior loggingBehavior;

	public RestHandlerException(String errorMessage, HttpResponseStatus httpResponseStatus) {
		super(errorMessage);
		this.responseCode = httpResponseStatus.code();
		this.loggingBehavior = LoggingBehavior.LOG;
	}

	public RestHandlerException(String errorMessage, HttpResponseStatus httpResponseStatus, LoggingBehavior loggingBehavior) {
		super(errorMessage);
		this.responseCode = httpResponseStatus.code();
		this.loggingBehavior = loggingBehavior;
	}

	public RestHandlerException(String errorMessage, HttpResponseStatus httpResponseStatus, Throwable cause) {
		super(errorMessage, cause);
		this.responseCode = httpResponseStatus.code();
		this.loggingBehavior = LoggingBehavior.LOG;
	}

	public HttpResponseStatus getHttpResponseStatus() {
		return HttpResponseStatus.valueOf(responseCode);
	}

	public boolean logException() {
		return LoggingBehavior.LOG == loggingBehavior;
	}

	/**
	 * Enum to control logging behavior of RestHandlerExceptions.
	 */
	public enum LoggingBehavior {
		LOG, IGNORE
	}
}
