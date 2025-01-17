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

import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.datastream.api.extension.join.JoinType;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.impl.operators.KeyedTwoInputNonBroadcastProcessOperator;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Operator for executing the Join operation in Join extension. Note that this should be executed
 * with two {@link KeyedPartitionStream}.
 */
public class TwoInputNonBroadcastJoinProcessOperator<K, IN1, IN2, OUT>
        extends KeyedTwoInputNonBroadcastProcessOperator<K, IN1, IN2, OUT> {

    private final TwoInputNonBroadcastJoinProcessFunction<IN1, IN2, OUT> joinProcessFunction;

    private final ListStateDescriptor<IN1> leftStateDescriptor;

    private final ListStateDescriptor<IN2> rightStateDescriptor;

    /** The state that stores the left input records. */
    private transient ListState<IN1> leftState;

    /** The state that stores the right input records. */
    private transient ListState<IN2> rightState;

    public TwoInputNonBroadcastJoinProcessOperator(
            TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> userFunction,
            ListStateDescriptor<IN1> leftStateDescriptor,
            ListStateDescriptor<IN2> rightStateDescriptor) {
        super(userFunction);
        this.joinProcessFunction =
                (TwoInputNonBroadcastJoinProcessFunction<IN1, IN2, OUT>) userFunction;
        checkArgument(
                joinProcessFunction.getJoinType() == JoinType.INNER,
                "Currently only support INNER join.");
        this.leftStateDescriptor = leftStateDescriptor;
        this.rightStateDescriptor = rightStateDescriptor;
    }

    @Override
    public void open() throws Exception {
        super.open();
        leftState =
                getOrCreateKeyedState(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        leftStateDescriptor);
        rightState =
                getOrCreateKeyedState(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        rightStateDescriptor);
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        collector.setTimestampFromStreamRecord(element);
        IN1 leftRecord = element.getValue();
        Iterable<IN2> rightRecords = rightState.get();
        if (rightRecords != null) {
            for (IN2 rightRecord : rightRecords) {
                joinProcessFunction
                        .getJoinFunction()
                        .processRecord(leftRecord, rightRecord, collector, partitionedContext);
            }
        }
        leftState.add(leftRecord);
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        collector.setTimestampFromStreamRecord(element);
        Iterable<IN1> leftRecords = leftState.get();
        IN2 rightRecord = element.getValue();
        if (leftRecords != null) {
            for (IN1 leftRecord : leftState.get()) {
                joinProcessFunction
                        .getJoinFunction()
                        .processRecord(leftRecord, rightRecord, collector, partitionedContext);
            }
        }
        rightState.add(rightRecord);
    }
}
