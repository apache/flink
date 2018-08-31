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

package org.apache.flink.runtime.rest;

import org.apache.flink.runtime.rest.handler.RestHandlerException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * A special exception that indicates that an element was not found and that the
 * request should be answered with a {@code 404} return code.
 */
public class NotFoundException extends RestHandlerException {

	private static final long serialVersionUID = -4036006746423754639L;

	public NotFoundException(String message) {
		super(message, HttpResponseStatus.NOT_FOUND);
	}

	public NotFoundException(String message, Throwable cause) {
		super(message, HttpResponseStatus.NOT_FOUND, cause);
	}
}
