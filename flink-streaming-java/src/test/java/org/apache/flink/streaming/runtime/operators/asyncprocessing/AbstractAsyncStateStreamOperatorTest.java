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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Basic tests for {@link AbstractAsyncStateStreamOperator}. */
public class AbstractAsyncStateStreamOperatorTest {

    protected KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
            createTestHarness(int maxParalelism, int numSubtasks, int subtaskIndex)
                    throws Exception {
        TestOperator testOperator = new TestOperator();
        return new KeyedOneInputStreamOperatorTestHarness<>(
                testOperator,
                new TestKeySelector(),
                BasicTypeInfo.INT_TYPE_INFO,
                maxParalelism,
                numSubtasks,
                subtaskIndex);
    }

    @Test
    public void testCreateAsyncExecutionController() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0)) {
            testHarness.open();
            assertThat(testHarness.getOperator())
                    .isInstanceOf(AbstractAsyncStateStreamOperator.class);
            assertThat(
                            ((AbstractAsyncStateStreamOperator) testHarness.getOperator())
                                    .getAsyncExecutionController())
                    .isNotNull();
        }
    }

    /** A simple testing operator. */
    private static class TestOperator extends AbstractAsyncStateStreamOperator<String>
            implements OneInputStreamOperator<Tuple2<Integer, String>, String>,
                    Triggerable<Integer, VoidNamespace> {

        private static final long serialVersionUID = 1L;

        @Override
        public void open() throws Exception {
            super.open();
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, String>> element)
                throws Exception {}

        @Override
        public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {}

        @Override
        public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer)
                throws Exception {}
    }

    /** {@link KeySelector} for tests. */
    static class TestKeySelector implements KeySelector<Tuple2<Integer, String>, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(Tuple2<Integer, String> value) {
            return value.f0;
        }
    }
}
