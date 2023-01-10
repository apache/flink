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

import org.apache.flink.runtime.rest.messages.ErrorResponseBody;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * The exception to describe the server internal error. It's equal to the {@link ErrorResponseBody}.
 */
public class RestServerClientException extends RestClientException {

    private static final long serialVersionUID = 8700567337263658114L;

    private final String rootCause;

    public RestServerClientException(String message, String rootCause, HttpResponseStatus status) {
        super(message, status);
        this.rootCause = rootCause;
    }

    public String getRootCause() {
        return rootCause;
    }
}
