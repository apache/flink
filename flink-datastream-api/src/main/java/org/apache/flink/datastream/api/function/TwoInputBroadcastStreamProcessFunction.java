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
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.RuntimeContext;

/**
 * This contains all logical related to process records from a broadcast stream and a non-broadcast
 * stream.
 */
@Experimental
public interface TwoInputBroadcastStreamProcessFunction<IN1, IN2, OUT> extends ProcessFunction {
    /**
     * Process record from non-broadcast input and emit data through {@link Collector}.
     *
     * @param record to process.
     * @param output to emit processed records.
     * @param ctx runtime context in which this function is executed.
     */
    void processRecordFromNonBroadcastInput(IN1 record, Collector<OUT> output, RuntimeContext ctx)
            throws Exception;

    /**
     * Process record from broadcast input. In general, the broadcast side is not allowed to
     * manipulate state and output data because it corresponds to all partitions instead of a single
     * partition. But you could use broadcast context to process all the partitions at once.
     *
     * @param record to process.
     * @param ctx the context in which this function is executed.
     */
    void processRecordFromBroadcastInput(IN2 record, NonPartitionedContext<OUT> ctx)
            throws Exception;

    /**
     * This is a life-cycle method indicates that this function will no longer receive any data from
     * the non-broadcast input.
     *
     * @param ctx the context in which this function is executed.
     */
    default void endNonBroadcastInput(NonPartitionedContext<OUT> ctx) {}

    /**
     * This is a life-cycle method indicates that this function will no longer receive any data from
     * the broadcast input.
     *
     * @param ctx the context in which this function is executed.
     */
    default void endBroadcastInput(NonPartitionedContext<OUT> ctx) {}
}
