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
 * Exception for network-level errors (connection failures, timeouts).
 *
 * <p>Indicates transient network issues such as DNS resolution failures, connection timeouts, or
 * socket errors. These errors are typically retryable with exponential backoff.
 *
 * <p><b>Common Scenarios:</b>
 *
 * <ul>
 *   <li>Connection refused: Server not reachable
 *   <li>Connection timeout: Network latency or firewall issues
 *   <li>DNS resolution failure: Hostname cannot be resolved
 *   <li>Socket timeout: Long-running request exceeded timeout
 * </ul>
 */
public class TritonNetworkException extends TritonException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new network exception.
     *
     * @param message The detailed error message
     * @param cause The underlying IOException or network error
     */
    public TritonNetworkException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public boolean isRetryable() {
        // Network errors are typically transient and retryable
        return true;
    }

    @Override
    public ErrorCategory getCategory() {
        return ErrorCategory.NETWORK_ERROR;
    }
}
