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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * EvictingWindowOperator}.
 */
@Internal
public class EvictingWindowOperatorFactory<K, IN, OUT, W extends Window>
        extends WindowOperatorFactory<K, IN, Iterable<IN>, OUT, W> {

    private final Evictor<? super IN, ? super W> evictor;

    private final StateDescriptor<? extends ListState<StreamRecord<IN>>, ?>
            evictingWindowStateDescriptor;

    public EvictingWindowOperatorFactory(
            WindowAssigner<? super IN, W> windowAssigner,
            TypeSerializer<W> windowSerializer,
            KeySelector<IN, K> keySelector,
            TypeSerializer<K> keySerializer,
            StateDescriptor<? extends ListState<StreamRecord<IN>>, ?> windowStateDescriptor,
            InternalWindowFunction<Iterable<IN>, OUT, K, W> windowFunction,
            Trigger<? super IN, ? super W> trigger,
            Evictor<? super IN, ? super W> evictor,
            long allowedLateness,
            OutputTag<IN> lateDataOutputTag) {
        super(
                windowAssigner,
                windowSerializer,
                keySelector,
                keySerializer,
                null,
                windowFunction,
                trigger,
                allowedLateness,
                lateDataOutputTag);

        this.evictor = checkNotNull(evictor);
        this.evictingWindowStateDescriptor = checkNotNull(windowStateDescriptor);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        EvictingWindowOperator<K, IN, OUT, W> operator =
                new EvictingWindowOperator<>(
                        windowAssigner,
                        windowSerializer,
                        keySelector,
                        keySerializer,
                        evictingWindowStateDescriptor,
                        windowFunction,
                        trigger,
                        evictor,
                        allowedLateness,
                        lateDataOutputTag);
        operator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        operator.setProcessingTimeService(parameters.getProcessingTimeService());
        return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return EvictingWindowOperator.class;
    }

    @Override
    @VisibleForTesting
    @SuppressWarnings("unchecked, rawtypes")
    public StateDescriptor<? extends AppendingState<IN, Iterable<IN>>, ?> getStateDescriptor() {
        return (StateDescriptor<? extends AppendingState<IN, Iterable<IN>>, ?>)
                evictingWindowStateDescriptor;
    }

    @VisibleForTesting
    public Evictor<? super IN, ? super W> getEvictor() {
        return evictor;
    }
}
