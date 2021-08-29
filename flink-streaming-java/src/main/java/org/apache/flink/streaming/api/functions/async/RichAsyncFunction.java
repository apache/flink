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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.types.Value;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * Rich variant of the {@link AsyncFunction}. As a {@link RichFunction}, it gives access to the
 * {@link RuntimeContext} and provides setup and teardown methods: {@link
 * RichFunction#open(org.apache.flink.configuration.Configuration)} and {@link
 * RichFunction#close()}.
 *
 * <p>State related apis in {@link RuntimeContext} are not supported yet because the key may get
 * changed while accessing states in the working thread.
 *
 * <p>{@link IterationRuntimeContext#getIterationAggregator(String)} is not supported since the
 * aggregator may be modified by multiple threads.
 *
 * @param <IN> The type of the input elements.
 * @param <OUT> The type of the returned elements.
 */
@PublicEvolving
public abstract class RichAsyncFunction<IN, OUT> extends AbstractRichFunction
        implements AsyncFunction<IN, OUT> {

    private static final long serialVersionUID = 3858030061138121840L;

    @Override
    public void setRuntimeContext(RuntimeContext runtimeContext) {
        Preconditions.checkNotNull(runtimeContext);

        if (runtimeContext instanceof IterationRuntimeContext) {
            super.setRuntimeContext(
                    new RichAsyncFunctionIterationRuntimeContext(
                            (IterationRuntimeContext) runtimeContext));
        } else {
            super.setRuntimeContext(new RichAsyncFunctionRuntimeContext(runtimeContext));
        }
    }

    @Override
    public abstract void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception;

    // -----------------------------------------------------------------------------------------
    // Wrapper classes
    // -----------------------------------------------------------------------------------------

    /**
     * A wrapper class for async function's {@link RuntimeContext}. The async function runtime
     * context only supports basic operations which are thread safe. Consequently, state access,
     * accumulators, broadcast variables and the distributed cache are disabled.
     */
    private static class RichAsyncFunctionRuntimeContext implements RuntimeContext {
        private final RuntimeContext runtimeContext;

        RichAsyncFunctionRuntimeContext(RuntimeContext context) {
            runtimeContext = Preconditions.checkNotNull(context);
        }

        @Override
        public JobID getJobId() {
            return runtimeContext.getJobId();
        }

        @Override
        public String getTaskName() {
            return runtimeContext.getTaskName();
        }

        @Override
        public OperatorMetricGroup getMetricGroup() {
            return runtimeContext.getMetricGroup();
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return runtimeContext.getNumberOfParallelSubtasks();
        }

        @Override
        public int getMaxNumberOfParallelSubtasks() {
            return runtimeContext.getMaxNumberOfParallelSubtasks();
        }

        @Override
        public int getIndexOfThisSubtask() {
            return runtimeContext.getIndexOfThisSubtask();
        }

        @Override
        public int getAttemptNumber() {
            return runtimeContext.getAttemptNumber();
        }

        @Override
        public String getTaskNameWithSubtasks() {
            return runtimeContext.getTaskNameWithSubtasks();
        }

        @Override
        public ExecutionConfig getExecutionConfig() {
            return runtimeContext.getExecutionConfig();
        }

        @Override
        public ClassLoader getUserCodeClassLoader() {
            return runtimeContext.getUserCodeClassLoader();
        }

        @Override
        public void registerUserCodeClassLoaderReleaseHookIfAbsent(
                String releaseHookName, Runnable releaseHook) {
            runtimeContext.registerUserCodeClassLoaderReleaseHookIfAbsent(
                    releaseHookName, releaseHook);
        }

        @Override
        public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
            return runtimeContext.getExternalResourceInfos(resourceName);
        }

        // -----------------------------------------------------------------------------------
        // Unsupported operations
        // -----------------------------------------------------------------------------------

        @Override
        public DistributedCache getDistributedCache() {
            throw new UnsupportedOperationException(
                    "Distributed cache is not supported in rich async functions.");
        }

        @Override
        public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
            throw new UnsupportedOperationException(
                    "State is not supported in rich async functions.");
        }

        @Override
        public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
            throw new UnsupportedOperationException(
                    "State is not supported in rich async functions.");
        }

        @Override
        public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
            throw new UnsupportedOperationException(
                    "State is not supported in rich async functions.");
        }

        @Override
        public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
                AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
            throw new UnsupportedOperationException(
                    "State is not supported in rich async functions.");
        }

        @Override
        public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
            throw new UnsupportedOperationException(
                    "State is not supported in rich async functions.");
        }

        @Override
        public <V, A extends Serializable> void addAccumulator(
                String name, Accumulator<V, A> accumulator) {
            throw new UnsupportedOperationException(
                    "Accumulators are not supported in rich async functions.");
        }

        @Override
        public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
            throw new UnsupportedOperationException(
                    "Accumulators are not supported in rich async functions.");
        }

        @Override
        public IntCounter getIntCounter(String name) {
            throw new UnsupportedOperationException(
                    "Int counters are not supported in rich async functions.");
        }

        @Override
        public LongCounter getLongCounter(String name) {
            throw new UnsupportedOperationException(
                    "Long counters are not supported in rich async functions.");
        }

        @Override
        public DoubleCounter getDoubleCounter(String name) {
            throw new UnsupportedOperationException(
                    "Double counters are not supported in rich async functions.");
        }

        @Override
        public Histogram getHistogram(String name) {
            throw new UnsupportedOperationException(
                    "Histograms are not supported in rich async functions.");
        }

        @Override
        public boolean hasBroadcastVariable(String name) {
            throw new UnsupportedOperationException(
                    "Broadcast variables are not supported in rich async functions.");
        }

        @Override
        public <RT> List<RT> getBroadcastVariable(String name) {
            throw new UnsupportedOperationException(
                    "Broadcast variables are not supported in rich async functions.");
        }

        @Override
        public <T, C> C getBroadcastVariableWithInitializer(
                String name, BroadcastVariableInitializer<T, C> initializer) {
            throw new UnsupportedOperationException(
                    "Broadcast variables are not supported in rich async functions.");
        }
    }

    private static class RichAsyncFunctionIterationRuntimeContext
            extends RichAsyncFunctionRuntimeContext implements IterationRuntimeContext {

        private final IterationRuntimeContext iterationRuntimeContext;

        RichAsyncFunctionIterationRuntimeContext(IterationRuntimeContext iterationRuntimeContext) {
            super(iterationRuntimeContext);

            this.iterationRuntimeContext = Preconditions.checkNotNull(iterationRuntimeContext);
        }

        @Override
        public int getSuperstepNumber() {
            return iterationRuntimeContext.getSuperstepNumber();
        }

        // -----------------------------------------------------------------------------------
        // Unsupported operations
        // -----------------------------------------------------------------------------------

        @Override
        public <T extends Aggregator<?>> T getIterationAggregator(String name) {
            throw new UnsupportedOperationException(
                    "Iteration aggregators are not supported in rich async functions.");
        }

        @Override
        public <T extends Value> T getPreviousIterationAggregate(String name) {
            throw new UnsupportedOperationException(
                    "Iteration aggregators are not supported in rich async functions.");
        }

        @Override
        public JobID getJobId() {
            return iterationRuntimeContext.getJobId();
        }
    }
}
