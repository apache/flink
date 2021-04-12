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

package org.apache.flink.streaming.util;

import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Arrays;
import java.util.List;

/** A test operator class implementing {@link BoundedMultiInput}. */
public class TestBoundedMultipleInputOperator extends AbstractStreamOperatorV2<String>
        implements MultipleInputStreamOperator<String>, BoundedMultiInput {

    private static final long serialVersionUID = 1L;

    private final String name;

    public TestBoundedMultipleInputOperator(
            String name, StreamOperatorParameters<String> parameters) {
        super(parameters, 3);
        this.name = name;
    }

    @Override
    public List<Input> getInputs() {
        return Arrays.asList(
                new TestInput(this, 1), new TestInput(this, 2), new TestInput(this, 3));
    }

    @Override
    public void endInput(int inputId) {
        output.collect(new StreamRecord<>("[" + name + "-" + inputId + "]: End of input"));
    }

    @Override
    public void close() throws Exception {
        output.collect(new StreamRecord<>("[" + name + "]: Bye"));
        super.close();
    }

    class TestInput extends AbstractInput<String, String> {
        public TestInput(AbstractStreamOperatorV2<String> owner, int inputId) {
            super(owner, inputId);
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            output.collect(
                    element.replace("[" + name + "-" + inputId + "]: " + element.getValue()));
        }
    }
}
