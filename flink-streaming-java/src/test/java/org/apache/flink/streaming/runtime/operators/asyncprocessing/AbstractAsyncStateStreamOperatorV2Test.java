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

package org.apache.flink.streaming.runtime.operators.asyncprocessing;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Basic tests for {@link AbstractAsyncStateStreamOperatorV2}. */
public class AbstractAsyncStateStreamOperatorV2Test {

    protected KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
            createTestHarness(int maxParalelism, int numSubtasks, int subtaskIndex)
                    throws Exception {
        return new KeyedOneInputStreamOperatorV2TestHarness<>(
                new TestOperatorFactory(),
                new AbstractAsyncStateStreamOperatorTest.TestKeySelector(),
                BasicTypeInfo.INT_TYPE_INFO,
                maxParalelism,
                numSubtasks,
                subtaskIndex);
    }

    @Test
    public void testCreateAsyncExecutionController() throws Exception {
        try (KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0)) {
            testHarness.open();
            assertThat(testHarness.getBaseOperator())
                    .isInstanceOf(AbstractAsyncStateStreamOperatorV2.class);
            assertThat(
                            ((AbstractAsyncStateStreamOperatorV2) testHarness.getBaseOperator())
                                    .getAsyncExecutionController())
                    .isNotNull();
        }
    }

    private static class KeyedOneInputStreamOperatorV2TestHarness<K, IN, OUT>
            extends KeyedOneInputStreamOperatorTestHarness<K, IN, OUT> {
        public KeyedOneInputStreamOperatorV2TestHarness(
                StreamOperatorFactory<OUT> operatorFactory,
                final KeySelector<IN, K> keySelector,
                TypeInformation<K> keyType,
                int maxParallelism,
                int numSubtasks,
                int subtaskIndex)
                throws Exception {
            super(operatorFactory, keySelector, keyType, maxParallelism, numSubtasks, subtaskIndex);
        }

        public StreamOperator<OUT> getBaseOperator() {
            return operator;
        }
    }

    private static class TestOperatorFactory extends AbstractStreamOperatorFactory<String> {
        @Override
        public <T extends StreamOperator<String>> T createStreamOperator(
                StreamOperatorParameters<String> parameters) {
            return (T) new SingleInputTestOperator(parameters);
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return SingleInputTestOperator.class;
        }
    }

    /**
     * Testing operator that can respond to commands by either setting/deleting state, emitting
     * state or setting timers.
     */
    private static class SingleInputTestOperator extends AbstractAsyncStateStreamOperatorV2<String>
            implements MultipleInputStreamOperator<String>, Triggerable<Integer, VoidNamespace> {

        private static final long serialVersionUID = 1L;

        public SingleInputTestOperator(StreamOperatorParameters<String> parameters) {
            super(parameters, 1);
        }

        @Override
        public void open() throws Exception {
            super.open();
        }

        @Override
        public List<Input> getInputs() {
            return Collections.singletonList(
                    new AbstractInput<Tuple2<Integer, String>, String>(this, 1) {
                        @Override
                        public void processElement(StreamRecord<Tuple2<Integer, String>> element)
                                throws Exception {}
                    });
        }

        @Override
        public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {}

        @Override
        public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer)
                throws Exception {}
    }
}
