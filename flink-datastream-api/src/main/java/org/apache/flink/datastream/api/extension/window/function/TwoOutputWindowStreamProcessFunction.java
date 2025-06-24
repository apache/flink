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
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.extension.window.context.OneInputWindowContext;

/**
 * A type of {@link WindowProcessFunction} for two-output window processing.
 *
 * @param <IN> The type of the input value.
 * @param <OUT1> The type of the output value to the first output.
 * @param <OUT2> The type of the output value to the second output.
 */
@Experimental
public interface TwoOutputWindowStreamProcessFunction<IN, OUT1, OUT2>
        extends WindowProcessFunction {

    /**
     * This method will be invoked when a record is received. Its default behaviors to store data in
     * built-in window state by {@link OneInputWindowContext#putRecord}. If the user overrides this
     * method, they have to take care of the input data themselves.
     */
    default void onRecord(
            IN record,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext<OUT1, OUT2> ctx,
            OneInputWindowContext<IN> windowContext)
            throws Exception {
        windowContext.putRecord(record);
    }

    /**
     * This method will be invoked when the Window is triggered, you can obtain all the input
     * records in the Window by {@link OneInputWindowContext#getAllRecords()}.
     */
    void onTrigger(
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext<OUT1, OUT2> ctx,
            OneInputWindowContext<IN> windowContext)
            throws Exception;

    /**
     * Callback when a window is about to be cleaned up. It is the time to deletes any state in the
     * {@code windowContext} when the Window expires (the event time or processing time passes its
     * {@code maxTimestamp} + {@code allowedLateness}).
     */
    default void onClear(
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext<OUT1, OUT2> ctx,
            OneInputWindowContext<IN> windowContext)
            throws Exception {}

    /** This method will be invoked when a record is received after the window has been cleaned. */
    default void onLateRecord(
            IN record,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext<OUT1, OUT2> ctx)
            throws Exception {}
}
