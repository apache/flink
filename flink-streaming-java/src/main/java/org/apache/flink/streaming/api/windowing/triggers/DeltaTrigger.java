/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.triggers;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * A {@link Trigger} that fires based on a {@link DeltaFunction} and a threshold.
 *
 * <p>This trigger calculates a delta between the data point which triggered last and the currently
 * arrived data point. It triggers if the delta is higher than a specified threshold.
 *
 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
 */
@PublicEvolving
public class DeltaTrigger<T, W extends Window> extends Trigger<T, W> {
    private static final long serialVersionUID = 1L;

    private final DeltaFunction<T> deltaFunction;
    private final double threshold;
    private final ValueStateDescriptor<T> stateDesc;

    private DeltaTrigger(
            double threshold, DeltaFunction<T> deltaFunction, TypeSerializer<T> stateSerializer) {
        this.deltaFunction = deltaFunction;
        this.threshold = threshold;
        stateDesc = new ValueStateDescriptor<>("last-element", stateSerializer);
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx)
            throws Exception {
        ValueState<T> lastElementState = ctx.getPartitionedState(stateDesc);
        if (lastElementState.value() == null) {
            lastElementState.update(element);
            return TriggerResult.CONTINUE;
        }
        if (deltaFunction.getDelta(lastElementState.value(), element) > this.threshold) {
            lastElementState.update(element);
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx)
            throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(stateDesc).clear();
    }

    @Override
    public String toString() {
        return "DeltaTrigger(" + deltaFunction + ", " + threshold + ")";
    }

    /**
     * Creates a delta trigger from the given threshold and {@code DeltaFunction}.
     *
     * @param threshold The threshold at which to trigger.
     * @param deltaFunction The delta function to use
     * @param stateSerializer TypeSerializer for the data elements.
     * @param <T> The type of elements on which this trigger can operate.
     * @param <W> The type of {@link Window Windows} on which this trigger can operate.
     */
    public static <T, W extends Window> DeltaTrigger<T, W> of(
            double threshold, DeltaFunction<T> deltaFunction, TypeSerializer<T> stateSerializer) {
        return new DeltaTrigger<>(threshold, deltaFunction, stateSerializer);
    }
}
