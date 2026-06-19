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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;

import java.time.Duration;

import static org.apache.flink.table.runtime.operators.calc.async.RetryPredicates.ANY_EXCEPTION;
import static org.apache.flink.table.runtime.operators.calc.async.RetryPredicates.EMPTY_RESPONSE;

/** Contains utilities for {@link org.apache.flink.table.functions.AsyncScalarFunction}. */
public class AsyncScalarUtil extends FunctionCallUtil {

    /**
     * Gets the options required to run the operator.
     *
     * @param config The config from which to fetch the options
     * @return Extracted options
     */
    public static AsyncOptions getAsyncOptions(ExecNodeConfig config) {
        return new AsyncOptions(
                config.get(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_MAX_CONCURRENT_OPERATIONS),
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_TIMEOUT).toMillis(),
                false,
                AsyncDataStream.OutputMode.ORDERED);
    }

    @SuppressWarnings("unchecked")
    public static AsyncRetryStrategy<RowData> getResultRetryStrategy(ExecNodeConfig config) {
        ExecutionConfigOptions.RetryStrategy retryStrategy =
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_RETRY_STRATEGY);
        Duration retryDelay =
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_RETRY_DELAY);
        int retryMaxAttempts =
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_MAX_ATTEMPTS);
        // Only fixed delay is allowed at the moment, so just ignore the config.
        if (retryStrategy == ExecutionConfigOptions.RetryStrategy.FIXED_DELAY) {
            return new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<RowData>(
                            retryMaxAttempts, retryDelay.toMillis())
                    .ifResult(EMPTY_RESPONSE)
                    .ifException(ANY_EXCEPTION)
                    .build();
        }
        return AsyncRetryStrategies.NO_RETRY_STRATEGY;
    }
}
