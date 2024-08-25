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
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.watermark.AbstractPartitionedContext;

/** A function to be applied to all partitions with two outputs. */
@FunctionalInterface
@Experimental
public interface TwoOutputApplyPartitionFunction<OUT1, OUT2> extends Function {
    /**
     * The actual method to be applied to each partition.
     *
     * @param firstOutput to emit record to first output.
     * @param secondOutput to emit record to second output.
     * @param ctx runtime context in which this function is executed.
     */
    void apply(
            Collector<OUT1> firstOutput,
            Collector<OUT2> secondOutput,
            AbstractPartitionedContext ctx)
            throws Exception;
}
