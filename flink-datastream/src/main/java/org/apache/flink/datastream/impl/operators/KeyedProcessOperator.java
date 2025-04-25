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

package org.apache.flink.datastream.impl.operators;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.datastream.api.context.ProcessingTimeManager;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.impl.context.DefaultProcessingTimeManager;
import org.apache.flink.datastream.impl.extension.eventtime.functions.EventTimeWrappedOneInputStreamProcessFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/** Operator for {@link OneInputStreamProcessFunction} in {@link KeyedPartitionStream}. */
public class KeyedProcessOperator<KEY, IN, OUT> extends BaseKeyedProcessOperator<KEY, IN, OUT>
        implements Triggerable<KEY, VoidNamespace> {
    private transient InternalTimerService<VoidNamespace> timerService;

    public KeyedProcessOperator(OneInputStreamProcessFunction<IN, OUT> userFunction) {
        this(userFunction, null);
    }

    public KeyedProcessOperator(
            OneInputStreamProcessFunction<IN, OUT> userFunction,
            @Nullable KeySelector<OUT, KEY> outKeySelector) {
        super(userFunction, outKeySelector);
    }

    @Override
    public void open() throws Exception {
        this.timerService =
                getInternalTimerService("processing timer", VoidNamespaceSerializer.INSTANCE, this);
        super.open();
    }

    @Override
    public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        if (userFunction instanceof EventTimeWrappedOneInputStreamProcessFunction) {
            ((EventTimeWrappedOneInputStreamProcessFunction<IN, OUT>) userFunction)
                    .onEventTime(timer.getTimestamp(), getOutputCollector(), partitionedContext);
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        userFunction.onProcessingTimer(
                timer.getTimestamp(), getOutputCollector(), partitionedContext);
    }

    @Override
    protected ProcessingTimeManager getProcessingTimeManager() {
        return new DefaultProcessingTimeManager(timerService);
    }

    @Override
    protected InternalTimerService<VoidNamespace> getTimerService() {
        return timerService;
    }

    @Override
    protected Supplier<Long> getEventTimeSupplier() {
        return () -> timerService.currentWatermark();
    }
}
