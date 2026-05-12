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
 * Exception thrown when the circuit breaker is in OPEN state and requests are not allowed.
 *
 * <p>This exception indicates that the Triton server is considered unhealthy based on recent
 * failures, and the circuit breaker is preventing additional requests to avoid cascading failures.
 *
 * <p>The circuit will automatically transition to HALF_OPEN state after a configured timeout and
 * will attempt to probe the server health with limited requests.
 *
 * <p><b>Retry semantics:</b> This exception is considered retryable because the underlying cause is
 * a transient server-health condition; once the OPEN timeout elapses the circuit will move to
 * HALF_OPEN and subsequent requests may succeed. It is categorised as {@link
 * ErrorCategory#SERVER_ERROR} to align with the reason the breaker tripped.
 *
 * @see org.apache.flink.model.triton.TritonCircuitBreaker
 */
public class TritonCircuitBreakerOpenException extends TritonException {

    private static final long serialVersionUID = 1L;

    public TritonCircuitBreakerOpenException(String message) {
        super(message);
    }

    public TritonCircuitBreakerOpenException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public boolean isRetryable() {
        // The circuit opens in response to server-side unhealthiness, and will automatically
        // transition to HALF_OPEN after its configured timeout. Upstream retry logic should be
        // allowed to retry (typically after a backoff) rather than giving up on the request.
        return true;
    }

    @Override
    public ErrorCategory getCategory() {
        // The breaker trips because the Triton server is considered unhealthy, so classify this
        // as a server-side error for metrics and alerting.
        return ErrorCategory.SERVER_ERROR;
    }
}
