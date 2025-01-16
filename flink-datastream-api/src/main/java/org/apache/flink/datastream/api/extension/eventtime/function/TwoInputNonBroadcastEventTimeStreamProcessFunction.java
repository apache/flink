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

package org.apache.flink.datastream.api.extension.eventtime.function;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;

/** The {@link TwoInputNonBroadcastStreamProcessFunction} that extends with event time support. */
@Experimental
public interface TwoInputNonBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT>
        extends EventTimeProcessFunction, TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> {

    /**
     * The {@code #onEventTimeWatermark} method signifies that the EventTimeProcessFunction has
     * received an EventTimeWatermark. Other types of watermarks will be processed by the {@code
     * ProcessFunction#onWatermark} method.
     */
    default void onEventTimeWatermark(
            long watermarkTimestamp, Collector<OUT> output, NonPartitionedContext<OUT> ctx)
            throws Exception {}

    /**
     * Invoked when an event-time timer fires. Note that it is only used in {@link
     * KeyedPartitionStream}.
     */
    default void onEventTimer(long timestamp, Collector<OUT> output, PartitionedContext<OUT> ctx) {}
}
