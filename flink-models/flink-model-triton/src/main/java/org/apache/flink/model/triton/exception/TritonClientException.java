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
 * Exception for client-side errors (HTTP 4xx status codes).
 *
 * <p>Indicates user configuration or input data issues that should be fixed by the user. These
 * errors are NOT retryable as they require configuration changes.
 *
 * <p><b>Common Scenarios:</b>
 *
 * <ul>
 *   <li>400 Bad Request: Invalid input shape or data format
 *   <li>404 Not Found: Model name or version doesn't exist
 *   <li>401 Unauthorized: Invalid authentication token
 * </ul>
 */
public class TritonClientException extends TritonException {
    private static final long serialVersionUID = 1L;

    private final int httpStatus;

    /**
     * Creates a new client exception.
     *
     * @param message The detailed error message
     * @param httpStatus The HTTP status code (4xx)
     */
    public TritonClientException(String message, int httpStatus) {
        super(String.format("[HTTP %d] %s", httpStatus, message));
        this.httpStatus = httpStatus;
    }

    /**
     * Returns the HTTP status code.
     *
     * @return The HTTP status code (4xx)
     */
    public int getHttpStatus() {
        return httpStatus;
    }

    @Override
    public boolean isRetryable() {
        // Client errors require configuration fixes, not retries
        return false;
    }

    @Override
    public ErrorCategory getCategory() {
        return ErrorCategory.CLIENT_ERROR;
    }
}
