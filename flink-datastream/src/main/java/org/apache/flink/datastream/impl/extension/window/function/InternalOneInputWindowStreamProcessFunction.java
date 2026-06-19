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

package org.apache.flink.datastream.impl.extension.window.function;

import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.window.function.OneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.function.WindowProcessFunction;
import org.apache.flink.datastream.api.extension.window.strategy.WindowStrategy;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Set;

/**
 * A class that wrap a {@link OneInputWindowStreamProcessFunction} to process function. This will be
 * translated to a window operator instead of vanilla process operator.
 *
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the output elements.
 * @param <W> Type of the window.
 */
public class InternalOneInputWindowStreamProcessFunction<IN, OUT, W extends Window>
        implements OneInputStreamProcessFunction<IN, OUT> {

    /** User-defined {@link WindowProcessFunction}. */
    private final OneInputWindowStreamProcessFunction<IN, OUT> windowProcessFunction;

    private final WindowAssigner<IN, W> assigner;

    private final Trigger<IN, W> trigger;

    /**
     * The allowed lateness for elements. This is used for:
     *
     * <ul>
     *   <li>Deciding if an element should be dropped from a window due to lateness.
     *   <li>Clearing the state of a window if the time out-of the {@code window.maxTimestamp +
     *       allowedLateness} landmark.
     * </ul>
     */
    private final long allowedLateness;

    private final WindowStrategy windowStrategy;

    public InternalOneInputWindowStreamProcessFunction(
            OneInputWindowStreamProcessFunction<IN, OUT> windowProcessFunction,
            WindowAssigner<IN, W> assigner,
            Trigger<IN, W> trigger,
            long allowedLateness,
            WindowStrategy windowStrategy) {
        this.windowProcessFunction = windowProcessFunction;
        this.assigner = assigner;
        this.trigger = trigger;
        this.allowedLateness = allowedLateness;
        this.windowStrategy = windowStrategy;
    }

    @Override
    public void processRecord(IN record, Collector<OUT> output, PartitionedContext<OUT> ctx)
            throws Exception {
        // Do nothing as this will translator to windowOperator instead of processOperator, and this
        // method will never be invoked.
    }

    public WindowAssigner<IN, W> getAssigner() {
        return assigner;
    }

    public Trigger<IN, W> getTrigger() {
        return trigger;
    }

    public long getAllowedLateness() {
        return allowedLateness;
    }

    public WindowStrategy getWindowStrategy() {
        return windowStrategy;
    }

    public OneInputWindowStreamProcessFunction<IN, OUT> getWindowProcessFunction() {
        return windowProcessFunction;
    }

    @Override
    public Set<StateDeclaration> usesStates() {
        return windowProcessFunction.usesStates();
    }
}
