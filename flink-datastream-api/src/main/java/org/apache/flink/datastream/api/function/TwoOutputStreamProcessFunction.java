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

package org.apache.flink.datastream.api.function;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;

/** This contains all logical related to process and emit records to two output streams. */
@Experimental
public interface TwoOutputStreamProcessFunction<IN, OUT1, OUT2> extends ProcessFunction {
    /**
     * Process and emit record to the first/second output through {@link Collector}s.
     *
     * @param record to process.
     * @param output1 to emit processed records to the first output.
     * @param output2 to emit processed records to the second output.
     * @param ctx runtime context in which this function is executed.
     */
    void processRecord(
            IN record,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext ctx)
            throws Exception;

    /**
     * This is a life-cycle method indicates that this function will no longer receive any input
     * data.
     *
     * @param ctx the context in which this function is executed.
     */
    default void endInput(TwoOutputNonPartitionedContext<OUT1, OUT2> ctx) {}

    /**
     * Callback for processing timer.
     *
     * @param timestamp when this callback is triggered.
     * @param output1 to emit record.
     * @param output2 to emit record.
     * @param ctx runtime context in which this function is executed.
     */
    default void onProcessingTimer(
            long timestamp,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext ctx) {}
}
