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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.functions.async.AsyncRetryPredicate;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.table.data.RowData;

import java.util.Collection;
import java.util.function.Predicate;

/**
 * A utility class to wrap the data stream api {@link AsyncRetryStrategy} to support both sync and
 * async retry in table module. The main consideration is making the class name not bind to async
 * scope, and also highlight the retry predicate is only over the result (not exception).
 */
public class ResultRetryStrategy implements AsyncRetryStrategy<RowData> {
    public static final ResultRetryStrategy NO_RETRY_STRATEGY =
            new ResultRetryStrategy(AsyncRetryStrategies.NO_RETRY_STRATEGY);
    private AsyncRetryStrategy retryStrategy;

    @VisibleForTesting
    public ResultRetryStrategy(AsyncRetryStrategy retryStrategy) {
        this.retryStrategy = retryStrategy;
    }

    /** Create a fixed-delay retry strategy by given params. */
    public static ResultRetryStrategy fixedDelayRetry(
            int maxAttempts,
            long backoffTimeMillis,
            Predicate<Collection<RowData>> resultPredicate) {
        return new ResultRetryStrategy(
                new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder(
                                maxAttempts, backoffTimeMillis)
                        .ifResult(resultPredicate)
                        .build());
    }

    @Override
    public boolean canRetry(int currentAttempts) {
        return retryStrategy.canRetry(currentAttempts);
    }

    @Override
    public long getBackoffTimeMillis(int currentAttempts) {
        return retryStrategy.getBackoffTimeMillis(currentAttempts);
    }

    @Override
    public AsyncRetryPredicate<RowData> getRetryPredicate() {
        return retryStrategy.getRetryPredicate();
    }
}
