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

package org.apache.flink.datastream.impl.extension.join.operators;

import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.join.JoinFunction;
import org.apache.flink.datastream.api.extension.join.JoinType;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;

/**
 * Wrap the user-defined {@link JoinFunction} as {@link TwoInputNonBroadcastStreamProcessFunction}
 * to execute the Join operation within Join extension.
 */
public class TwoInputNonBroadcastJoinProcessFunction<IN1, IN2, OUT>
        implements TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> {

    private final JoinFunction<IN1, IN2, OUT> joinFunction;

    private final JoinType joinType;

    public TwoInputNonBroadcastJoinProcessFunction(
            JoinFunction<IN1, IN2, OUT> joinFunction, JoinType joinType) {
        this.joinFunction = joinFunction;
        this.joinType = joinType;
    }

    @Override
    public void processRecordFromFirstInput(
            IN1 record, Collector<OUT> output, PartitionedContext<OUT> ctx) throws Exception {}

    @Override
    public void processRecordFromSecondInput(
            IN2 record, Collector<OUT> output, PartitionedContext<OUT> ctx) throws Exception {}

    public JoinFunction<IN1, IN2, OUT> getJoinFunction() {
        return joinFunction;
    }

    public JoinType getJoinType() {
        return joinType;
    }
}
