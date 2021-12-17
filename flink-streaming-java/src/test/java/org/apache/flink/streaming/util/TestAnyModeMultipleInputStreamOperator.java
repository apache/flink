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
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Arrays;
import java.util.List;

/** A test operator class for any mode reading. */
public class TestAnyModeMultipleInputStreamOperator extends AbstractStreamOperatorV2<String>
        implements MultipleInputStreamOperator<String>, InputSelectable {

    public TestAnyModeMultipleInputStreamOperator(StreamOperatorParameters<String> parameters) {
        super(parameters, 2);
    }

    @Override
    public InputSelection nextSelection() {
        return InputSelection.ALL;
    }

    @Override
    public List<Input> getInputs() {
        return Arrays.asList(new ToStringInput(this, 1), new ToStringInput(this, 2));
    }

    /** Factory to construct {@link TestAnyModeMultipleInputStreamOperator}. */
    public static class Factory extends AbstractStreamOperatorFactory<String> {

        @Override
        public <T extends StreamOperator<String>> T createStreamOperator(
                StreamOperatorParameters<String> parameters) {
            return (T) new TestAnyModeMultipleInputStreamOperator(parameters);
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return TestAnyModeMultipleInputStreamOperator.class;
        }
    }

    /** {@link AbstractInput} that converts argument to string pre-pending input id. */
    public static class ToStringInput<T> extends AbstractInput<T, String> {
        public ToStringInput(AbstractStreamOperatorV2<String> owner, int inputId) {
            super(owner, inputId);
        }

        @Override
        public void processElement(StreamRecord<T> element) {
            output.collect(element.replace(String.format("[%d]: %s", inputId, element.getValue())));
        }
    }
}
