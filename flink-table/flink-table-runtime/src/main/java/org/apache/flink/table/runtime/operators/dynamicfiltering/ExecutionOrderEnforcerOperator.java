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

package org.apache.flink.table.runtime.operators.dynamicfiltering;

import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Arrays;
import java.util.List;

/**
 * ExecutionOrderEnforcerOperator has two inputs, one of which is a source, and the other is the
 * dependent upstream. It enforces that the input source is executed after the dependent input is
 * finished. Everything passed from the inputs is forwarded to the output, though typically the
 * dependent input should not send anything.
 *
 * <p>The operator must be chained with the source, which is generally ensured by the {@link
 * ExecutionOrderEnforcerOperatorFactory}. If chaining is explicitly disabled, the enforcer can not
 * work as expected.
 *
 * <p>The operator is used only for dynamic filtering at present.
 */
public class ExecutionOrderEnforcerOperator<IN> extends AbstractStreamOperatorV2<IN>
        implements MultipleInputStreamOperator<IN> {

    public ExecutionOrderEnforcerOperator(StreamOperatorParameters<IN> parameters) {
        super(parameters, 2);
    }

    @Override
    public List<Input> getInputs() {
        return Arrays.asList(
                new ForwardingInput<>(this, 1, output), new ForwardingInput<>(this, 2, output));
    }

    private static class ForwardingInput<IN> extends AbstractInput<IN, IN> {
        private final Output<StreamRecord<IN>> output;

        public ForwardingInput(
                AbstractStreamOperatorV2<IN> owner, int inputId, Output<StreamRecord<IN>> output) {
            super(owner, inputId);
            this.output = output;
        }

        @Override
        public void processElement(StreamRecord<IN> element) throws Exception {
            output.collect(element);
        }
    }
}
