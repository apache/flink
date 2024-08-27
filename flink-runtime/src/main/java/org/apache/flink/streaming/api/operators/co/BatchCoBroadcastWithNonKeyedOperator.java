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
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TwoInputStreamOperator} for executing {@link BroadcastProcessFunction
 * BroadcastProcessFunctions} in {@link org.apache.flink.api.common.RuntimeExecutionMode#BATCH}
 * execution mode.
 *
 * <p>Compared to {@link CoBroadcastWithNonKeyedOperator} this uses {@link BoundedMultiInput} and
 * {@link InputSelectable} to enforce the requirement that the broadcast side is processed before
 * the regular input.
 *
 * @param <IN1> The input type of the regular (non-broadcast) side.
 * @param <IN2> The input type of the broadcast side.
 * @param <OUT> The output type of the operator.
 */
@Internal
public class BatchCoBroadcastWithNonKeyedOperator<IN1, IN2, OUT>
        extends CoBroadcastWithNonKeyedOperator<IN1, IN2, OUT>
        implements BoundedMultiInput, InputSelectable {

    private static final long serialVersionUID = -1869740381935471752L;

    private transient volatile boolean isBroadcastSideDone = false;

    public BatchCoBroadcastWithNonKeyedOperator(
            final BroadcastProcessFunction<IN1, IN2, OUT> function,
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
    public InputSelection nextSelection() {
        if (!isBroadcastSideDone) {
            return InputSelection.SECOND;
        } else {
            return InputSelection.FIRST;
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
