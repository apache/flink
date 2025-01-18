/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.AsyncEvictingWindowOperator;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.AsyncWindowOperator;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.functions.InternalAggregateProcessAsyncWindowFunction;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.functions.InternalAsyncWindowFunction;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.functions.InternalIterableAsyncWindowFunction;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.functions.InternalIterableProcessAsyncWindowFunction;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.functions.InternalSingleValueAsyncWindowFunction;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.functions.InternalSingleValueProcessAsyncWindowFunction;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncCountTrigger;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncEventTimeTrigger;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncProcessingTimeTrigger;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncPurgingTrigger;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncTrigger;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows.EndOfStreamTrigger;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalAggregateProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.lang.reflect.Type;
import java.time.Duration;

/**
 * A builder for creating {@link WindowOperator WindowOperators}.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <T> The type of the incoming elements.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
public class WindowOperatorBuilder<T, K, W extends Window> {

    private static final String WINDOW_STATE_NAME = "window-contents";

    private final ExecutionConfig config;

    private final WindowAssigner<? super T, W> windowAssigner;

    private final TypeInformation<T> inputType;

    private final KeySelector<T, K> keySelector;

    private final TypeInformation<K> keyType;

    private Trigger<? super T, ? super W> trigger;

    private AsyncTrigger<? super T, ? super W> asyncTrigger;

    @Nullable private Evictor<? super T, ? super W> evictor;

    private long allowedLateness = 0L;

    @Nullable private OutputTag<T> lateDataOutputTag;

    public WindowOperatorBuilder(
            WindowAssigner<? super T, W> windowAssigner,
            Trigger<? super T, ? super W> trigger,
            ExecutionConfig config,
            TypeInformation<T> inputType,
            KeySelector<T, K> keySelector,
            TypeInformation<K> keyType) {
        this.windowAssigner = windowAssigner;
        this.config = config;
        this.inputType = inputType;
        this.keySelector = keySelector;
        this.keyType = keyType;
        this.trigger = trigger;
    }

    public void trigger(Trigger<? super T, ? super W> trigger) {
        Preconditions.checkNotNull(trigger, "Window triggers cannot be null");

        if (windowAssigner instanceof MergingWindowAssigner && !trigger.canMerge()) {
            throw new UnsupportedOperationException(
                    "A merging window assigner cannot be used with a trigger that does not support merging.");
        }

        this.trigger = trigger;
    }

    public WindowOperatorBuilder<T, K, W> asyncTrigger(
            AsyncTrigger<? super T, ? super W> asyncTrigger) {
        Preconditions.checkNotNull(asyncTrigger, "AsyncTrigger cannot be null");

        if (windowAssigner instanceof MergingWindowAssigner && !asyncTrigger.canMerge()) {
            throw new UnsupportedOperationException(
                    "A merging window assigner cannot be used with a trigger that does not support merging.");
        }

        this.asyncTrigger = asyncTrigger;

        return this;
    }

    public void allowedLateness(Duration lateness) {
        Preconditions.checkNotNull(lateness, "Allowed lateness cannot be null");

        final long millis = lateness.toMillis();
        Preconditions.checkArgument(millis >= 0, "The allowed lateness cannot be negative.");

        this.allowedLateness = millis;
    }

    public void sideOutputLateData(OutputTag<T> outputTag) {
        Preconditions.checkNotNull(outputTag, "Side output tag must not be null.");
        this.lateDataOutputTag = outputTag;
    }

    public void evictor(Evictor<? super T, ? super W> evictor) {
        Preconditions.checkNotNull(evictor, "Evictor cannot be null");

        this.evictor = evictor;
    }

    public <R> OneInputStreamOperator<T, R> reduce(
            ReduceFunction<T> reduceFunction, WindowFunction<T, R, K, W> function) {
        Preconditions.checkNotNull(reduceFunction, "ReduceFunction cannot be null");
        Preconditions.checkNotNull(function, "WindowFunction cannot be null");

        if (reduceFunction instanceof RichFunction) {
            throw new UnsupportedOperationException(
                    "ReduceFunction of apply can not be a RichFunction.");
        }

        if (evictor != null) {
            return buildEvictingWindowOperator(
                    new InternalIterableWindowFunction<>(
                            new ReduceApplyWindowFunction<>(reduceFunction, function)));
        } else {
            ReducingStateDescriptor<T> stateDesc =
                    new ReducingStateDescriptor<>(
                            WINDOW_STATE_NAME,
                            reduceFunction,
                            inputType.createSerializer(config.getSerializerConfig()));

            return buildWindowOperator(
                    stateDesc, new InternalSingleValueWindowFunction<>(function));
        }
    }

    public <R> OneInputStreamOperator<T, R> asyncReduce(
            ReduceFunction<T> reduceFunction, WindowFunction<T, R, K, W> function) {
        Preconditions.checkNotNull(reduceFunction, "ReduceFunction cannot be null");
        Preconditions.checkNotNull(function, "WindowFunction cannot be null");

        if (reduceFunction instanceof RichFunction) {
            throw new UnsupportedOperationException(
                    "ReduceFunction of apply can not be a RichFunction.");
        }

        if (evictor != null) {
            return buildAsyncEvictingWindowOperator(
                    new InternalIterableAsyncWindowFunction<>(
                            new ReduceApplyWindowFunction<>(reduceFunction, function)));
        } else {
            org.apache.flink.api.common.state.v2.ReducingStateDescriptor<T> stateDesc =
                    new org.apache.flink.api.common.state.v2.ReducingStateDescriptor<>(
                            WINDOW_STATE_NAME,
                            reduceFunction,
                            inputType.createSerializer(config.getSerializerConfig()));

            return buildAsyncWindowOperator(
                    stateDesc, new InternalSingleValueAsyncWindowFunction<>(function));
        }
    }

    public <R> OneInputStreamOperator<T, R> reduce(
            ReduceFunction<T> reduceFunction, ProcessWindowFunction<T, R, K, W> function) {
        Preconditions.checkNotNull(reduceFunction, "ReduceFunction cannot be null");
        Preconditions.checkNotNull(function, "ProcessWindowFunction cannot be null");

        if (reduceFunction instanceof RichFunction) {
            throw new UnsupportedOperationException(
                    "ReduceFunction of apply can not be a RichFunction.");
        }

        if (evictor != null) {
            return buildEvictingWindowOperator(
                    new InternalIterableProcessWindowFunction<>(
                            new ReduceApplyProcessWindowFunction<>(reduceFunction, function)));
        } else {
            ReducingStateDescriptor<T> stateDesc =
                    new ReducingStateDescriptor<>(
                            WINDOW_STATE_NAME,
                            reduceFunction,
                            inputType.createSerializer(config.getSerializerConfig()));

            return buildWindowOperator(
                    stateDesc, new InternalSingleValueProcessWindowFunction<>(function));
        }
    }

    public <R> OneInputStreamOperator<T, R> asyncReduce(
            ReduceFunction<T> reduceFunction, ProcessWindowFunction<T, R, K, W> function) {
        Preconditions.checkNotNull(reduceFunction, "ReduceFunction cannot be null");
        Preconditions.checkNotNull(function, "ProcessWindowFunction cannot be null");

        if (reduceFunction instanceof RichFunction) {
            throw new UnsupportedOperationException(
                    "ReduceFunction of apply can not be a RichFunction.");
        }

        if (evictor != null) {
            return buildAsyncEvictingWindowOperator(
                    new InternalIterableProcessAsyncWindowFunction<>(
                            new ReduceApplyProcessWindowFunction<>(reduceFunction, function)));
        } else {
            org.apache.flink.api.common.state.v2.ReducingStateDescriptor<T> stateDesc =
                    new org.apache.flink.api.common.state.v2.ReducingStateDescriptor<>(
                            WINDOW_STATE_NAME,
                            reduceFunction,
                            inputType.createSerializer(config.getSerializerConfig()));

            return buildAsyncWindowOperator(
                    stateDesc, new InternalSingleValueProcessAsyncWindowFunction<>(function));
        }
    }

    public <ACC, V, R> OneInputStreamOperator<T, R> aggregate(
            AggregateFunction<T, ACC, V> aggregateFunction,
            WindowFunction<V, R, K, W> windowFunction,
            TypeInformation<ACC> accumulatorType) {

        Preconditions.checkNotNull(aggregateFunction, "AggregateFunction cannot be null");
        Preconditions.checkNotNull(windowFunction, "WindowFunction cannot be null");

        if (aggregateFunction instanceof RichFunction) {
            throw new UnsupportedOperationException(
                    "This aggregate function cannot be a RichFunction.");
        }

        if (evictor != null) {
            return buildEvictingWindowOperator(
                    new InternalIterableWindowFunction<>(
                            new AggregateApplyWindowFunction<>(aggregateFunction, windowFunction)));
        } else {
            AggregatingStateDescriptor<T, ACC, V> stateDesc =
                    new AggregatingStateDescriptor<>(
                            WINDOW_STATE_NAME,
                            aggregateFunction,
                            accumulatorType.createSerializer(config.getSerializerConfig()));

            return buildWindowOperator(
                    stateDesc, new InternalSingleValueWindowFunction<>(windowFunction));
        }
    }

    public <ACC, V, R> OneInputStreamOperator<T, R> asyncAggregate(
            AggregateFunction<T, ACC, V> aggregateFunction,
            WindowFunction<V, R, K, W> windowFunction,
            TypeInformation<ACC> accumulatorType) {

        Preconditions.checkNotNull(aggregateFunction, "AggregateFunction cannot be null");
        Preconditions.checkNotNull(windowFunction, "WindowFunction cannot be null");

        if (aggregateFunction instanceof RichFunction) {
            throw new UnsupportedOperationException(
                    "This aggregate function cannot be a RichFunction.");
        }

        if (evictor != null) {
            return buildAsyncEvictingWindowOperator(
                    new InternalIterableAsyncWindowFunction<>(
                            new AggregateApplyWindowFunction<>(aggregateFunction, windowFunction)));
        } else {
            org.apache.flink.api.common.state.v2.AggregatingStateDescriptor<T, ACC, V> stateDesc =
                    new org.apache.flink.api.common.state.v2.AggregatingStateDescriptor<>(
                            WINDOW_STATE_NAME,
                            aggregateFunction,
                            accumulatorType.createSerializer(config.getSerializerConfig()));

            return buildAsyncWindowOperator(
                    stateDesc, new InternalSingleValueAsyncWindowFunction<>(windowFunction));
        }
    }

    public <ACC, V, R> OneInputStreamOperator<T, R> aggregate(
            AggregateFunction<T, ACC, V> aggregateFunction,
            ProcessWindowFunction<V, R, K, W> windowFunction,
            TypeInformation<ACC> accumulatorType) {

        Preconditions.checkNotNull(aggregateFunction, "AggregateFunction cannot be null");
        Preconditions.checkNotNull(windowFunction, "ProcessWindowFunction cannot be null");

        if (aggregateFunction instanceof RichFunction) {
            throw new UnsupportedOperationException(
                    "This aggregate function cannot be a RichFunction.");
        }

        if (evictor != null) {
            return buildEvictingWindowOperator(
                    new InternalAggregateProcessWindowFunction<>(
                            aggregateFunction, windowFunction));
        } else {
            AggregatingStateDescriptor<T, ACC, V> stateDesc =
                    new AggregatingStateDescriptor<>(
                            WINDOW_STATE_NAME,
                            aggregateFunction,
                            accumulatorType.createSerializer(config.getSerializerConfig()));

            return buildWindowOperator(
                    stateDesc, new InternalSingleValueProcessWindowFunction<>(windowFunction));
        }
    }

    public <ACC, V, R> OneInputStreamOperator<T, R> asyncAggregate(
            AggregateFunction<T, ACC, V> aggregateFunction,
            ProcessWindowFunction<V, R, K, W> windowFunction,
            TypeInformation<ACC> accumulatorType) {

        Preconditions.checkNotNull(aggregateFunction, "AggregateFunction cannot be null");
        Preconditions.checkNotNull(windowFunction, "ProcessWindowFunction cannot be null");

        if (aggregateFunction instanceof RichFunction) {
            throw new UnsupportedOperationException(
                    "This aggregate function cannot be a RichFunction.");
        }

        if (evictor != null) {
            return buildAsyncEvictingWindowOperator(
                    new InternalAggregateProcessAsyncWindowFunction<>(
                            aggregateFunction, windowFunction));
        } else {
            org.apache.flink.api.common.state.v2.AggregatingStateDescriptor<T, ACC, V> stateDesc =
                    new org.apache.flink.api.common.state.v2.AggregatingStateDescriptor<>(
                            WINDOW_STATE_NAME,
                            aggregateFunction,
                            accumulatorType.createSerializer(config.getSerializerConfig()));

            return buildAsyncWindowOperator(
                    stateDesc, new InternalSingleValueProcessAsyncWindowFunction<>(windowFunction));
        }
    }

    public <R> OneInputStreamOperator<T, R> apply(WindowFunction<T, R, K, W> function) {
        Preconditions.checkNotNull(function, "WindowFunction cannot be null");
        return apply(new InternalIterableWindowFunction<>(function));
    }

    public <R> OneInputStreamOperator<T, R> process(ProcessWindowFunction<T, R, K, W> function) {
        Preconditions.checkNotNull(function, "ProcessWindowFunction cannot be null");
        return apply(new InternalIterableProcessWindowFunction<>(function));
    }

    private <R> OneInputStreamOperator<T, R> apply(
            InternalWindowFunction<Iterable<T>, R, K, W> function) {
        if (evictor != null) {
            return buildEvictingWindowOperator(function);
        } else {
            ListStateDescriptor<T> stateDesc =
                    new ListStateDescriptor<>(
                            WINDOW_STATE_NAME,
                            inputType.createSerializer(config.getSerializerConfig()));

            return buildWindowOperator(stateDesc, function);
        }
    }

    public <R> OneInputStreamOperator<T, R> asyncApply(WindowFunction<T, R, K, W> function) {
        Preconditions.checkNotNull(function, "WindowFunction cannot be null");
        return asyncApply(new InternalIterableAsyncWindowFunction<>(function));
    }

    public <R> OneInputStreamOperator<T, R> asyncProcess(
            ProcessWindowFunction<T, R, K, W> function) {
        Preconditions.checkNotNull(function, "ProcessWindowFunction cannot be null");
        return asyncApply(new InternalIterableProcessAsyncWindowFunction<>(function));
    }

    private <R> OneInputStreamOperator<T, R> asyncApply(
            InternalAsyncWindowFunction<StateIterator<T>, R, K, W> function) {
        if (evictor != null) {
            return buildAsyncEvictingWindowOperator(function);
        } else {
            org.apache.flink.api.common.state.v2.ListStateDescriptor<T> stateDesc =
                    new org.apache.flink.api.common.state.v2.ListStateDescriptor<>(
                            WINDOW_STATE_NAME,
                            inputType.createSerializer(config.getSerializerConfig()));

            return buildAsyncWindowOperator(stateDesc, function);
        }
    }

    private <ACC, R> WindowOperator<K, T, ACC, R, W> buildWindowOperator(
            StateDescriptor<? extends AppendingState<T, ACC>, ?> stateDesc,
            InternalWindowFunction<ACC, R, K, W> function) {

        return new WindowOperator<>(
                windowAssigner,
                windowAssigner.getWindowSerializer(config),
                keySelector,
                keyType.createSerializer(config.getSerializerConfig()),
                stateDesc,
                function,
                trigger,
                allowedLateness,
                lateDataOutputTag);
    }

    private <R> WindowOperator<K, T, Iterable<T>, R, W> buildEvictingWindowOperator(
            InternalWindowFunction<Iterable<T>, R, K, W> function) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        TypeSerializer<StreamRecord<T>> streamRecordSerializer =
                (TypeSerializer<StreamRecord<T>>)
                        new StreamElementSerializer(
                                inputType.createSerializer(config.getSerializerConfig()));

        ListStateDescriptor<StreamRecord<T>> stateDesc =
                new ListStateDescriptor<>(WINDOW_STATE_NAME, streamRecordSerializer);

        return new EvictingWindowOperator<>(
                windowAssigner,
                windowAssigner.getWindowSerializer(config),
                keySelector,
                keyType.createSerializer(config.getSerializerConfig()),
                stateDesc,
                function,
                trigger,
                evictor,
                allowedLateness,
                lateDataOutputTag);
    }

    private <ACC, R> AsyncWindowOperator<K, T, ACC, R, W> buildAsyncWindowOperator(
            org.apache.flink.api.common.state.v2.StateDescriptor<?> stateDesc,
            InternalAsyncWindowFunction<ACC, R, K, W> function) {

        return new AsyncWindowOperator<>(
                windowAssigner,
                windowAssigner.getWindowSerializer(config),
                keySelector,
                keyType.createSerializer(config.getSerializerConfig()),
                stateDesc,
                function,
                asyncTrigger == null ? AsyncTriggerConverter.convertToAsync(trigger) : asyncTrigger,
                allowedLateness,
                lateDataOutputTag);
    }

    private <R> AsyncWindowOperator<K, T, StateIterator<T>, R, W> buildAsyncEvictingWindowOperator(
            InternalAsyncWindowFunction<StateIterator<T>, R, K, W> function) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        TypeSerializer<StreamRecord<T>> streamRecordSerializer =
                (TypeSerializer<StreamRecord<T>>)
                        new StreamElementSerializer(
                                inputType.createSerializer(config.getSerializerConfig()));

        org.apache.flink.api.common.state.v2.ListStateDescriptor<StreamRecord<T>> stateDesc =
                new org.apache.flink.api.common.state.v2.ListStateDescriptor<>(
                        WINDOW_STATE_NAME, streamRecordSerializer);

        return new AsyncEvictingWindowOperator<>(
                windowAssigner,
                windowAssigner.getWindowSerializer(config),
                keySelector,
                keyType.createSerializer(config.getSerializerConfig()),
                stateDesc,
                function,
                asyncTrigger == null ? AsyncTriggerConverter.convertToAsync(trigger) : asyncTrigger,
                evictor,
                allowedLateness,
                lateDataOutputTag);
    }

    protected static String generateFunctionName(Function function) {
        Class<? extends Function> functionClass = function.getClass();
        if (functionClass.isAnonymousClass()) {
            // getSimpleName returns an empty String for anonymous classes
            Type[] interfaces = functionClass.getInterfaces();
            if (interfaces.length == 0) {
                // extends an existing class (like RichMapFunction)
                Class<?> functionSuperClass = functionClass.getSuperclass();
                return functionSuperClass.getSimpleName()
                        + functionClass
                                .getName()
                                .substring(functionClass.getEnclosingClass().getName().length());
            } else {
                // implements a Function interface
                Class<?> functionInterface = functionClass.getInterfaces()[0];
                return functionInterface.getSimpleName()
                        + functionClass
                                .getName()
                                .substring(functionClass.getEnclosingClass().getName().length());
            }
        } else {
            return functionClass.getSimpleName();
        }
    }

    public String generateOperatorName() {
        return windowAssigner.getClass().getSimpleName();
    }

    public String generateOperatorDescription(Function function1, @Nullable Function function2) {
        return "Window("
                + windowAssigner
                + ", "
                + trigger.getClass().getSimpleName()
                + ", "
                + (evictor == null ? "" : (evictor.getClass().getSimpleName() + ", "))
                + generateFunctionName(function1)
                + (function2 == null ? "" : (", " + generateFunctionName(function2)))
                + ")";
    }

    @VisibleForTesting
    public long getAllowedLateness() {
        return allowedLateness;
    }

    private static class UserDefinedAsyncTrigger<T, W extends Window> extends AsyncTrigger<T, W> {
        private final Trigger<T, W> userDefinedTrigger;

        private UserDefinedAsyncTrigger(Trigger<T, W> userDefinedTrigger) {
            this.userDefinedTrigger = userDefinedTrigger;
        }

        @Override
        public StateFuture<TriggerResult> onElement(
                T element, long timestamp, W window, TriggerContext ctx) throws Exception {
            return StateFutureUtils.completedFuture(
                    userDefinedTrigger.onElement(
                            element, timestamp, window, AsyncTriggerContextConvertor.of(ctx)));
        }

        @Override
        public StateFuture<TriggerResult> onProcessingTime(long time, W window, TriggerContext ctx)
                throws Exception {
            return StateFutureUtils.completedFuture(
                    userDefinedTrigger.onProcessingTime(
                            time, window, AsyncTriggerContextConvertor.of(ctx)));
        }

        @Override
        public StateFuture<TriggerResult> onEventTime(long time, W window, TriggerContext ctx)
                throws Exception {
            return StateFutureUtils.completedFuture(
                    userDefinedTrigger.onEventTime(
                            time, window, AsyncTriggerContextConvertor.of(ctx)));
        }

        @Override
        public StateFuture<Void> clear(W window, TriggerContext ctx) throws Exception {
            userDefinedTrigger.clear(window, AsyncTriggerContextConvertor.of(ctx));
            return StateFutureUtils.completedVoidFuture();
        }

        @Override
        public boolean isEndOfStreamTrigger() {
            return userDefinedTrigger instanceof EndOfStreamTrigger;
        }

        public static <T, W extends Window> AsyncTrigger<T, W> of(
                Trigger<T, W> userDefinedTrigger) {
            return new UserDefinedAsyncTrigger<>(userDefinedTrigger);
        }
    }

    private static class AsyncTriggerConverter {

        @SuppressWarnings("unchecked")
        public static <T, W extends Window> AsyncTrigger<T, W> convertToAsync(
                Trigger<T, W> trigger) {
            if (trigger instanceof CountTrigger) {
                return (AsyncTrigger<T, W>)
                        AsyncCountTrigger.of(((CountTrigger<?>) trigger).getMaxCount());
            } else if (trigger instanceof EventTimeTrigger) {
                return (AsyncTrigger<T, W>) AsyncEventTimeTrigger.create();
            } else if (trigger instanceof ProcessingTimeTrigger) {
                return (AsyncTrigger<T, W>) AsyncProcessingTimeTrigger.create();
            } else if (trigger instanceof PurgingTrigger) {
                return (AsyncTrigger<T, W>)
                        AsyncPurgingTrigger.of(
                                convertToAsync(
                                        ((PurgingTrigger<?, ?>) trigger).getNestedTrigger()));
            } else {
                return UserDefinedAsyncTrigger.of(trigger);
            }
        }
    }

    /** A converter from {@link AsyncTrigger.TriggerContext} to {@link Trigger.TriggerContext}. */
    private static class AsyncTriggerContextConvertor implements TriggerContext {

        private final AsyncTrigger.TriggerContext asyncTriggerContext;

        private AsyncTriggerContextConvertor(AsyncTrigger.TriggerContext asyncTriggerContext) {
            this.asyncTriggerContext = asyncTriggerContext;
        }

        @Override
        public long getCurrentProcessingTime() {
            return asyncTriggerContext.getCurrentProcessingTime();
        }

        @Override
        public MetricGroup getMetricGroup() {
            return asyncTriggerContext.getMetricGroup();
        }

        @Override
        public long getCurrentWatermark() {
            return asyncTriggerContext.getCurrentWatermark();
        }

        @Override
        public void registerProcessingTimeTimer(long time) {
            asyncTriggerContext.registerProcessingTimeTimer(time);
        }

        @Override
        public void registerEventTimeTimer(long time) {
            asyncTriggerContext.registerEventTimeTimer(time);
        }

        @Override
        public void deleteProcessingTimeTimer(long time) {
            asyncTriggerContext.deleteProcessingTimeTimer(time);
        }

        @Override
        public void deleteEventTimeTimer(long time) {
            asyncTriggerContext.deleteEventTimeTimer(time);
        }

        @Override
        public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
            throw new UnsupportedOperationException(
                    "Trigger is for state V1 APIs, window operator with async state enabled only accept state V2 APIs.");
        }

        public static TriggerContext of(AsyncTrigger.TriggerContext asyncTriggerContext) {
            return new AsyncTriggerContextConvertor(asyncTriggerContext);
        }
    }
}
