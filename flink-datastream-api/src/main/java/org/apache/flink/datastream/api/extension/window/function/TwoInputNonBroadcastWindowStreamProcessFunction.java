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

package org.apache.flink.datastream.api.extension.window.function;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.window.context.TwoInputWindowContext;

/**
 * A type of {@link WindowProcessFunction} for two input window processing, such as window-join.
 *
 * @param <IN1> The type of the input1 value.
 * @param <IN2> The type of the input2 value.
 * @param <OUT> The type of the output value.
 */
@Experimental
public interface TwoInputNonBroadcastWindowStreamProcessFunction<IN1, IN2, OUT>
        extends WindowProcessFunction {

    /**
     * This method will be invoked when a record is received from input1. Its default behaviors to
     * store data in built-in window state by {@link TwoInputWindowContext#putRecord1}. If the user
     * overrides this method, they have to take care of the input data themselves.
     */
    default void onRecord1(
            IN1 record,
            Collector<OUT> output,
            PartitionedContext<OUT> ctx,
            TwoInputWindowContext<IN1, IN2> windowContext)
            throws Exception {
        windowContext.putRecord1(record);
    }

    /**
     * This method will be invoked when a record is received from input2. Its default behaviors to
     * store data in built-in window state by {@link TwoInputWindowContext#putRecord2}. If the user
     * overrides this method, they have to take care of the input data themselves.
     */
    default void onRecord2(
            IN2 record,
            Collector<OUT> output,
            PartitionedContext<OUT> ctx,
            TwoInputWindowContext<IN1, IN2> windowContext)
            throws Exception {
        windowContext.putRecord2(record);
    }

    /**
     * This method will be invoked when the Window is triggered, you can obtain all the input
     * records in the Window by {@link TwoInputWindowContext#getAllRecords1()} and {@link
     * TwoInputWindowContext#getAllRecords2()}.
     */
    void onTrigger(
            Collector<OUT> output,
            PartitionedContext<OUT> ctx,
            TwoInputWindowContext<IN1, IN2> windowContext)
            throws Exception;

    /**
     * Callback when a window is about to be cleaned up. It is the time to deletes any state in the
     * {@code windowContext} when the Window expires (the event time or processing time passes its
     * {@code maxTimestamp} + {@code allowedLateness}).
     */
    default void onClear(
            Collector<OUT> output,
            PartitionedContext<OUT> ctx,
            TwoInputWindowContext<IN1, IN2> windowContext)
            throws Exception {}

    /**
     * This method will be invoked when a record is received from input1 after the window has been
     * cleaned.
     */
    default void onLateRecord1(IN1 record, Collector<OUT> output, PartitionedContext<OUT> ctx)
            throws Exception {}

    /**
     * This method will be invoked when a record is received from input2 after the window has been
     * cleaned.
     */
    default void onLateRecord2(IN2 record, Collector<OUT> output, PartitionedContext<OUT> ctx)
            throws Exception {}
}
