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

/**
 * Interface encapsulates a retry strategy for batch async operations.
 *
 * <p>This is the batch-equivalent of {@link AsyncRetryStrategy}, designed specifically for {@link
 * AsyncBatchFunction} operations. It defines:
 *
 * <ul>
 *   <li>Maximum number of retry attempts
 *   <li>Backoff delay between retries
 *   <li>Conditions under which retry should be triggered
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * AsyncBatchRetryStrategy<String> strategy = new AsyncBatchRetryStrategies
 *     .FixedDelayRetryStrategyBuilder<String>(3, 1000L)
 *     .ifException(e -> e instanceof TimeoutException)
 *     .build();
 * }</pre>
 *
 * @param <OUT> The type of the output elements.
 */
@PublicEvolving
public interface AsyncBatchRetryStrategy<OUT> extends Serializable {

    /**
     * Determines whether a retry attempt can be made based on the current number of attempts.
     *
     * @param currentAttempts the number of attempts already made (starts from 1)
     * @return true if another retry attempt can be made, false otherwise
     */
    boolean canRetry(int currentAttempts);

    /**
     * Returns the backoff time in milliseconds before the next retry attempt.
     *
     * @param currentAttempts the number of attempts already made
     * @return backoff time in milliseconds, or -1 if no retry should be performed
     */
    long getBackoffTimeMillis(int currentAttempts);

    /**
     * Returns the retry predicate that determines when a retry should be triggered.
     *
     * @return the retry predicate for this strategy
     */
    AsyncBatchRetryPredicate<OUT> getRetryPredicate();
}
