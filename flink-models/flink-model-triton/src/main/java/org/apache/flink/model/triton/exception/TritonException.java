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
 * Base exception for all Triton Inference Server integration errors.
 *
 * <p>This exception hierarchy provides typed error handling for different failure scenarios:
 *
 * <ul>
 *   <li>{@link TritonClientException}: HTTP 4xx errors (user configuration issues)
 *   <li>{@link TritonServerException}: HTTP 5xx errors (server-side issues)
 *   <li>{@link TritonNetworkException}: Network/connection failures
 *   <li>{@link TritonSchemaException}: Shape/type mismatch errors
 * </ul>
 */
public class TritonException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /** Error category for classification and monitoring. */
    public enum ErrorCategory {
        /** Client-side errors (4xx): Bad configuration, invalid input, etc. */
        CLIENT_ERROR,

        /** Server-side errors (5xx): Inference failure, service unavailable, etc. */
        SERVER_ERROR,

        /** Network errors: Connection timeout, DNS failure, etc. */
        NETWORK_ERROR,

        /** Schema/type errors: Shape mismatch, incompatible types, etc. */
        SCHEMA_ERROR,

        /** Unknown or unclassified errors. */
        UNKNOWN
    }

    /**
     * Creates a new Triton exception with the specified message.
     *
     * @param message The detailed error message
     */
    public TritonException(String message) {
        super(message);
    }

    /**
     * Creates a new Triton exception with the specified message and cause.
     *
     * @param message The detailed error message
     * @param cause The underlying cause
     */
    public TritonException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Returns true if this error is retryable with exponential backoff.
     *
     * <p>Default implementation returns false. Subclasses should override if the error condition is
     * transient (e.g., 503 Service Unavailable).
     *
     * @return true if the operation can be retried
     */
    public boolean isRetryable() {
        return false;
    }

    /**
     * Returns the error category for logging, monitoring, and alerting purposes.
     *
     * <p>This can be used to:
     *
     * <ul>
     *   <li>Route errors to appropriate handling logic
     *   <li>Aggregate metrics by error type
     *   <li>Configure different retry strategies
     * </ul>
     *
     * @return The error category
     */
    public ErrorCategory getCategory() {
        return ErrorCategory.UNKNOWN;
    }
}
