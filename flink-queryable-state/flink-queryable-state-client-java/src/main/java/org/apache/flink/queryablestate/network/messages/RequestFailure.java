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

package org.apache.flink.queryablestate.network.messages;

import org.apache.flink.annotation.Internal;

/** A message indicating a protocol-related error. */
@Internal
public class RequestFailure {

    /** ID of the request responding to. */
    private final long requestId;

    /** Failure cause. Not allowed to be a user type. */
    private final Throwable cause;

    /**
     * Creates a failure response to a {@link MessageBody}.
     *
     * @param requestId ID for the request responding to
     * @param cause Failure cause (not allowed to be a user type)
     */
    public RequestFailure(long requestId, Throwable cause) {
        this.requestId = requestId;
        this.cause = cause;
    }

    /**
     * Returns the request ID responding to.
     *
     * @return Request ID responding to
     */
    public long getRequestId() {
        return requestId;
    }

    /**
     * Returns the failure cause.
     *
     * @return Failure cause
     */
    public Throwable getCause() {
        return cause;
    }

    @Override
    public String toString() {
        return "RequestFailure{" + "requestId=" + requestId + ", cause=" + cause + '}';
    }
}
