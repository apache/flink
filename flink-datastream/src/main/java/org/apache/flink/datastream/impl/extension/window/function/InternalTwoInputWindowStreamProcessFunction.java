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
import org.apache.flink.datastream.api.extension.window.function.TwoInputNonBroadcastWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.function.WindowProcessFunction;
import org.apache.flink.datastream.api.extension.window.strategy.WindowStrategy;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.TaggedUnion;

import java.util.Set;

/**
 * A class that wrap a {@link TwoInputNonBroadcastWindowStreamProcessFunction} to process function.
 * This will be translated to a window operator instead of vanilla process operator.
 *
 * @param <IN1> Type of the first input elements.
 * @param <IN2> Type of the second input elements.
 * @param <OUT> Type of the output elements.
 * @param <W> Type of the window.
 */
public class InternalTwoInputWindowStreamProcessFunction<IN1, IN2, OUT, W extends Window>
        implements TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> {

    /** User-defined {@link WindowProcessFunction}. */
    private final TwoInputNonBroadcastWindowStreamProcessFunction<IN1, IN2, OUT>
            windowProcessFunction;

    private final WindowAssigner<TaggedUnion<IN1, IN2>, W> assigner;

    private final Trigger<TaggedUnion<IN1, IN2>, W> trigger;

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

    public InternalTwoInputWindowStreamProcessFunction(
            TwoInputNonBroadcastWindowStreamProcessFunction<IN1, IN2, OUT> windowProcessFunction,
            WindowAssigner<TaggedUnion<IN1, IN2>, W> assigner,
            Trigger<TaggedUnion<IN1, IN2>, W> trigger,
            long allowedLateness,
            WindowStrategy windowStrategy) {
        this.windowProcessFunction = windowProcessFunction;
        this.assigner = assigner;
        this.trigger = trigger;
        this.allowedLateness = allowedLateness;
        this.windowStrategy = windowStrategy;
    }

    @Override
    public void processRecordFromFirstInput(
            IN1 record, Collector<OUT> output, PartitionedContext<OUT> ctx) throws Exception {
        // Do nothing as this will translator to windowOperator instead of processOperator, and this
        // method will never be invoked.
    }

    @Override
    public void processRecordFromSecondInput(
            IN2 record, Collector<OUT> output, PartitionedContext<OUT> ctx) throws Exception {
        // Do nothing as this will translator to windowOperator instead of processOperator, and this
        // method will never be invoked.
    }

    public TwoInputNonBroadcastWindowStreamProcessFunction<IN1, IN2, OUT>
            getWindowProcessFunction() {
        return windowProcessFunction;
    }

    @Override
    public Set<StateDeclaration> usesStates() {
        return windowProcessFunction.usesStates();
    }

    public WindowAssigner<TaggedUnion<IN1, IN2>, W> getAssigner() {
        return assigner;
    }

    public Trigger<TaggedUnion<IN1, IN2>, W> getTrigger() {
        return trigger;
    }

    public long getAllowedLateness() {
        return allowedLateness;
    }

    public WindowStrategy getWindowStrategy() {
        return windowStrategy;
    }
}
