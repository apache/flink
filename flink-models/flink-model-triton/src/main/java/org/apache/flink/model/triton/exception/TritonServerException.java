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

package org.apache.flink.model.triton.exception;

/**
 * Exception for server-side errors (HTTP 5xx status codes).
 *
 * <p>Indicates Triton Inference Server issues such as inference crashes, out of memory, or service
 * overload. Some server errors are retryable (e.g., 503 Service Unavailable, 504 Gateway Timeout).
 *
 * <p><b>Common Scenarios:</b>
 *
 * <ul>
 *   <li>500 Internal Server Error: Model inference crash (NOT retryable)
 *   <li>503 Service Unavailable: Server overloaded (retryable)
 *   <li>504 Gateway Timeout: Inference took too long (retryable)
 * </ul>
 */
public class TritonServerException extends TritonException {
    private static final long serialVersionUID = 1L;

    private final int httpStatus;

    /**
     * Creates a new server exception.
     *
     * @param message The detailed error message
     * @param httpStatus The HTTP status code (5xx)
     */
    public TritonServerException(String message, int httpStatus) {
        super(String.format("[HTTP %d] %s", httpStatus, message));
        this.httpStatus = httpStatus;
    }

    /**
     * Returns the HTTP status code.
     *
     * @return The HTTP status code (5xx)
     */
    public int getHttpStatus() {
        return httpStatus;
    }

    @Override
    public boolean isRetryable() {
        // 503 Service Unavailable and 504 Gateway Timeout are retryable
        // 500 Internal Server Error typically requires investigation
        return httpStatus == 503 || httpStatus == 504;
    }

    @Override
    public ErrorCategory getCategory() {
        return ErrorCategory.SERVER_ERROR;
    }
}
