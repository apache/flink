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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TwoInputStreamOperator} for executing {@link KeyedBroadcastProcessFunction
 * KeyedBroadcastProcessFunctions} in {@link org.apache.flink.api.common.RuntimeExecutionMode#BATCH}
 * execution mode.
 *
 * <p>Compared to {@link CoBroadcastWithKeyedOperator} this does an additional sanity check on the
 * input processing order requirement.
 *
 * @param <KS> The key type of the input keyed stream.
 * @param <IN1> The input type of the keyed (non-broadcast) side.
 * @param <IN2> The input type of the broadcast side.
 * @param <OUT> The output type of the operator.
 */
@Internal
public class BatchCoBroadcastWithKeyedOperator<KS, IN1, IN2, OUT>
        extends CoBroadcastWithKeyedOperator<KS, IN1, IN2, OUT> implements BoundedMultiInput {

    private static final long serialVersionUID = 5926499536290284870L;

    private transient volatile boolean isBroadcastSideDone = false;

    public BatchCoBroadcastWithKeyedOperator(
            final KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT> function,
            final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors) {
        super(function, broadcastStateDescriptors);
    }

    @Override
    public void endInput(int inputId) throws Exception {
        if (inputId == 2) {
            // finished with the broadcast side
            isBroadcastSideDone = true;
        }
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        checkState(
                isBroadcastSideDone,
                "Should not process regular input before broadcast side is done.");

        super.processElement1(element);
    }
}
