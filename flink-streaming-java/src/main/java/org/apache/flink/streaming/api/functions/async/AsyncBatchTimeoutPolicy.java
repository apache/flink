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

package org.apache.flink.streaming.api.functions.async;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.time.Duration;

/**
 * Configuration for batch-level timeout behavior in async batch operations.
 *
 * <p>This policy defines:
 *
 * <ul>
 *   <li>The timeout duration for a batch async operation
 *   <li>The behavior when a timeout occurs (fail or emit partial results)
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Fail on timeout after 5 seconds
 * AsyncBatchTimeoutPolicy policy = AsyncBatchTimeoutPolicy.failOnTimeout(Duration.ofSeconds(5));
 *
 * // Allow partial results on timeout after 10 seconds
 * AsyncBatchTimeoutPolicy policy = AsyncBatchTimeoutPolicy.allowPartialOnTimeout(Duration.ofSeconds(10));
 * }</pre>
 *
 * <p><b>Note:</b> Timeout is measured from the moment the batch async function is invoked until the
 * result future is completed. If the timeout expires before completion:
 *
 * <ul>
 *   <li>With {@link TimeoutBehavior#FAIL}: The operator throws a timeout exception
 *   <li>With {@link TimeoutBehavior#ALLOW_PARTIAL}: The operator emits whatever results are
 *       available (may be empty)
 * </ul>
 */
@PublicEvolving
public class AsyncBatchTimeoutPolicy implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Constant indicating timeout is disabled. */
    private static final long NO_TIMEOUT = 0L;

    /** A policy that disables timeout (no timeout). */
    public static final AsyncBatchTimeoutPolicy NO_TIMEOUT_POLICY =
            new AsyncBatchTimeoutPolicy(NO_TIMEOUT, TimeoutBehavior.FAIL);

    /** The timeout behavior when a batch operation times out. */
    public enum TimeoutBehavior {
        /**
         * Fail the operator when timeout occurs. This will cause the job to fail unless handled by
         * a restart strategy.
         */
        FAIL,

        /**
         * Allow partial results when timeout occurs. If no results are available, an empty
         * collection is emitted.
         */
        ALLOW_PARTIAL
    }

    private final long timeoutMs;
    private final TimeoutBehavior behavior;

    private AsyncBatchTimeoutPolicy(long timeoutMs, TimeoutBehavior behavior) {
        this.timeoutMs = timeoutMs;
        this.behavior = behavior;
    }

    /**
     * Creates a timeout policy that fails the operator on timeout.
     *
     * @param timeout the timeout duration
     * @return a timeout policy configured to fail on timeout
     */
    public static AsyncBatchTimeoutPolicy failOnTimeout(Duration timeout) {
        return new AsyncBatchTimeoutPolicy(timeout.toMillis(), TimeoutBehavior.FAIL);
    }

    /**
     * Creates a timeout policy that fails the operator on timeout.
     *
     * @param timeoutMs the timeout in milliseconds
     * @return a timeout policy configured to fail on timeout
     */
    public static AsyncBatchTimeoutPolicy failOnTimeout(long timeoutMs) {
        return new AsyncBatchTimeoutPolicy(timeoutMs, TimeoutBehavior.FAIL);
    }

    /**
     * Creates a timeout policy that allows partial results on timeout.
     *
     * @param timeout the timeout duration
     * @return a timeout policy configured to allow partial results
     */
    public static AsyncBatchTimeoutPolicy allowPartialOnTimeout(Duration timeout) {
        return new AsyncBatchTimeoutPolicy(timeout.toMillis(), TimeoutBehavior.ALLOW_PARTIAL);
    }

    /**
     * Creates a timeout policy that allows partial results on timeout.
     *
     * @param timeoutMs the timeout in milliseconds
     * @return a timeout policy configured to allow partial results
     */
    public static AsyncBatchTimeoutPolicy allowPartialOnTimeout(long timeoutMs) {
        return new AsyncBatchTimeoutPolicy(timeoutMs, TimeoutBehavior.ALLOW_PARTIAL);
    }

    /**
     * Returns whether timeout is enabled.
     *
     * @return true if timeout is enabled, false otherwise
     */
    public boolean isTimeoutEnabled() {
        return timeoutMs > NO_TIMEOUT;
    }

    /**
     * Returns the timeout duration in milliseconds.
     *
     * @return timeout in milliseconds
     */
    public long getTimeoutMs() {
        return timeoutMs;
    }

    /**
     * Returns the timeout behavior.
     *
     * @return the behavior when timeout occurs
     */
    public TimeoutBehavior getBehavior() {
        return behavior;
    }

    /**
     * Returns whether partial results should be allowed on timeout.
     *
     * @return true if partial results are allowed, false if failure should occur
     */
    public boolean shouldAllowPartialOnTimeout() {
        return behavior == TimeoutBehavior.ALLOW_PARTIAL;
    }

    @Override
    public String toString() {
        return "AsyncBatchTimeoutPolicy{"
                + "timeoutMs="
                + timeoutMs
                + ", behavior="
                + behavior
                + '}';
    }
}
