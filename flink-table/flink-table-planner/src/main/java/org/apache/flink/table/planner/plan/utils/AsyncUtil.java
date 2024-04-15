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
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.table.runtime.operators.calc.async.RetryPredicates.ANY_EXCEPTION;
import static org.apache.flink.table.runtime.operators.calc.async.RetryPredicates.EMPTY_RESPONSE;

/** Contains utilities for {@link org.apache.flink.table.functions.AsyncScalarFunction}. */
public class AsyncUtil {

    /**
     * Checks whether it contains the specified kind of async function call in the specified node.
     *
     * @param node the RexNode to check
     * @return true if it contains an async function call in the specified node.
     */
    public static boolean containsAsyncCall(RexNode node) {
        return node.accept(new FunctionFinder(true, true));
    }

    /**
     * Checks whether it contains non-async function call in the specified node.
     *
     * @param node the RexNode to check
     * @return true if it contains a non-async function call in the specified node.
     */
    public static boolean containsNonAsyncCall(RexNode node) {
        return node.accept(new FunctionFinder(false, true));
    }

    /**
     * Checks whether the specified node is the specified kind of async function call.
     *
     * @param node the RexNode to check
     * @return true if the specified node is an async function call.
     */
    public static boolean isAsyncCall(RexNode node) {
        return node.accept(new FunctionFinder(true, false));
    }

    /**
     * Checks whether the specified node is a non-async function call.
     *
     * @param node the RexNode to check
     * @return true if the specified node is a non-async function call.
     */
    public static boolean isNonAsyncCall(RexNode node) {
        return node.accept(new FunctionFinder(false, false));
    }

    /**
     * Gets the options required to run the operator.
     *
     * @param config The config from which to fetch the options
     * @return Extracted options
     */
    public static AsyncUtil.Options getAsyncOptions(ExecNodeConfig config) {
        return new AsyncUtil.Options(
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_BUFFER_CAPACITY),
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_TIMEOUT).toMillis(),
                AsyncDataStream.OutputMode.ORDERED,
                getResultRetryStrategy(
                        config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_RETRY_STRATEGY),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_RETRY_DELAY),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_MAX_ATTEMPTS)));
    }

    /** Options for configuring async behavior. */
    public static class Options {

        public final int asyncBufferCapacity;
        public final long asyncTimeout;
        public final AsyncDataStream.OutputMode asyncOutputMode;
        public final AsyncRetryStrategy<RowData> asyncRetryStrategy;

        public Options(
                int asyncBufferCapacity,
                long asyncTimeout,
                AsyncDataStream.OutputMode asyncOutputMode,
                AsyncRetryStrategy<RowData> asyncRetryStrategy) {
            this.asyncBufferCapacity = asyncBufferCapacity;
            this.asyncTimeout = asyncTimeout;
            this.asyncOutputMode = asyncOutputMode;
            this.asyncRetryStrategy = asyncRetryStrategy;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Options that = (Options) o;
            return asyncBufferCapacity == that.asyncBufferCapacity
                    && asyncTimeout == that.asyncTimeout
                    && asyncOutputMode == that.asyncOutputMode;
        }

        @Override
        public int hashCode() {
            return Objects.hash(asyncBufferCapacity, asyncTimeout, asyncOutputMode);
        }

        @Override
        public String toString() {
            return asyncOutputMode + ", " + asyncTimeout + "ms, " + asyncBufferCapacity;
        }
    }

    @SuppressWarnings("unchecked")
    private static AsyncRetryStrategy<RowData> getResultRetryStrategy(
            ExecutionConfigOptions.RetryStrategy retryStrategy,
            Duration retryDelay,
            int retryMaxAttempts) {
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

    private static class FunctionFinder extends RexDefaultVisitor<Boolean> {

        private final boolean findAsyncCall;
        private final boolean recursive;

        public FunctionFinder(boolean findAsyncCall, boolean recursive) {
            this.findAsyncCall = findAsyncCall;
            this.recursive = recursive;
        }

        @Override
        public Boolean visitNode(RexNode rexNode) {
            return false;
        }

        private boolean isImmediateAsyncCall(RexCall call) {
            FunctionDefinition definition = ShortcutUtils.unwrapFunctionDefinition(call);
            return definition != null && definition.getKind() == FunctionKind.ASYNC_SCALAR;
        }

        @Override
        public Boolean visitCall(RexCall call) {
            boolean isImmediateAsyncCall = isImmediateAsyncCall(call);
            return findAsyncCall == isImmediateAsyncCall
                    || (recursive
                            && call.getOperands().stream().anyMatch(node -> node.accept(this)));
        }
    }
}
