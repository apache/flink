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

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;

/**
 * This class wraps netty's {@link HttpMethod}s into an enum, allowing us to use them in switches.
 */
public enum HttpMethodWrapper {
	GET(HttpMethod.GET),
	POST(HttpMethod.POST),
	DELETE(HttpMethod.DELETE),
	PATCH(HttpMethod.PATCH);

	private HttpMethod nettyHttpMethod;

	HttpMethodWrapper(HttpMethod nettyHttpMethod) {
		this.nettyHttpMethod = nettyHttpMethod;
	}

	public HttpMethod getNettyHttpMethod() {
		return nettyHttpMethod;
	}
}
