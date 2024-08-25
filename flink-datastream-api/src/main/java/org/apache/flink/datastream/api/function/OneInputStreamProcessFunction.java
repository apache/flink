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
import org.apache.flink.api.watermark.GeneralizedWatermark;
import org.apache.flink.api.watermark.WatermarkPolicy;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;

/** This contains all logical related to process records from single input. */
@Experimental
public interface OneInputStreamProcessFunction<IN, OUT> extends ProcessFunction {
    /**
     * Process record and emit data through {@link Collector}.
     *
     * @param record to process.
     * @param output to emit processed records.
     * @param ctx runtime context in which this function is executed.
     */
    void processRecord(IN record, Collector<OUT> output, PartitionedContext ctx) throws Exception;

    /**
     * This is a life-cycle method indicates that this function will no longer receive any data from
     * the input.
     *
     * @param ctx the context in which this function is executed.
     */
    default void endInput(NonPartitionedContext<OUT> ctx) {}

    /**
     * Callback for processing timer.
     *
     * @param timestamp when this callback is triggered.
     * @param output to emit record.
     * @param ctx runtime context in which this function is executed.
     */
    default void onProcessingTimer(long timestamp, Collector<OUT> output, PartitionedContext ctx) {}

    default void onEventTimer(long timestamp, Collector<OUT> output, PartitionedContext ctx) {}

    default WatermarkPolicy onWatermark(
            GeneralizedWatermark watermark, Collector<OUT> output, NonPartitionedContext<OUT> ctx) {
        return WatermarkPolicy.PEEK;
    }
}
