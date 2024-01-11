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

package org.apache.flink.table.runtime.operators.window.groupwindow.context;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.Trigger;

import java.time.ZoneId;

/**
 * A context object that is given to {@link Trigger} methods to allow them to register timer
 * callbacks and deal with state.
 */
public interface TriggerContext {

    /** Returns the current processing time. */
    long getCurrentProcessingTime();

    /**
     * Returns the metric group for this {@link Trigger}. This is the same metric group that would
     * be returned from {@link RuntimeContext#getMetricGroup()} in a user function.
     *
     * <p>You must not call methods that create metric objects (such as {@link
     * MetricGroup#counter(int)} multiple times but instead call once and store the metric object in
     * a field.
     */
    MetricGroup getMetricGroup();

    /** Returns the current watermark time. */
    long getCurrentWatermark();

    /**
     * Register a system time callback. When the current system time passes the specified time
     * {@link Trigger#onProcessingTime(long, Window)} is called with the time specified here.
     *
     * @param time The time at which to invoke {@link Trigger#onProcessingTime(long, Window)}
     */
    void registerProcessingTimeTimer(long time);

    /**
     * Register an event-time callback. When the current watermark passes the specified time {@link
     * Trigger#onEventTime(long, Window)} is called with the time specified here.
     *
     * @param time The watermark at which to invoke {@link Trigger#onEventTime(long, Window)}
     * @see org.apache.flink.streaming.api.watermark.Watermark
     */
    void registerEventTimeTimer(long time);

    /** Delete the processing time trigger for the given time. */
    void deleteProcessingTimeTimer(long time);

    /** Delete the event-time trigger for the given time. */
    void deleteEventTimeTimer(long time);

    /** Returns the shifted timezone. */
    ZoneId getShiftTimeZone();

    /**
     * Retrieves a {@link State} object that can be used to interact with fault-tolerant state that
     * is scoped to the window and key of the current trigger invocation.
     *
     * @param stateDescriptor The StateDescriptor that contains the name and type of the state that
     *     is being accessed.
     * @param <S> The type of the state.
     * @return The partitioned state object.
     * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
     *     function (function is not part os a KeyedStream).
     */
    <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor);
}
