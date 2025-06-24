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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link StreamTaskTestHarness}. */
class StreamTaskTestHarnessTest {

    @Test
    void testMultipleSetupsThrowsException() {

        StreamTaskTestHarness<String> harness =
                new StreamTaskTestHarness<>(
                        OneInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO);
        harness.setupOutputForSingletonOperatorChain();

        assertThatThrownBy(harness::setupOutputForSingletonOperatorChain)
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> harness.setupOperatorChain(new OperatorID(), new TestOperator()))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(
                        () ->
                                harness.setupOperatorChain(
                                        new OperatorID(), new TwoInputTestOperator()))
                .isInstanceOf(IllegalStateException.class);

        StreamTaskTestHarness<String> harness1 =
                new StreamTaskTestHarness<>(
                        OneInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO);
        harness1.setupOperatorChain(new OperatorID(), new TestOperator())
                .chain(
                        new OperatorID(),
                        new TestOperator(),
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                new SerializerConfigImpl()));

        assertThatThrownBy(harness1::setupOutputForSingletonOperatorChain)
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> harness1.setupOperatorChain(new OperatorID(), new TestOperator()))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(
                        () ->
                                harness1.setupOperatorChain(
                                        new OperatorID(), new TwoInputTestOperator()))
                .isInstanceOf(IllegalStateException.class);

        StreamTaskTestHarness<String> harness2 =
                new StreamTaskTestHarness<>(
                        TwoInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO);
        harness2.setupOperatorChain(new OperatorID(), new TwoInputTestOperator())
                .chain(
                        new OperatorID(),
                        new TestOperator(),
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                new SerializerConfigImpl()));

        assertThatThrownBy(harness2::setupOutputForSingletonOperatorChain)
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> harness2.setupOperatorChain(new OperatorID(), new TestOperator()))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(
                        () ->
                                harness2.setupOperatorChain(
                                        new OperatorID(), new TwoInputTestOperator()))
                .isInstanceOf(IllegalStateException.class);
    }

    private static class TestOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {
        @Override
        public void processElement(StreamRecord<String> element) throws Exception {}
    }

    private static class TwoInputTestOperator extends AbstractStreamOperator<String>
            implements TwoInputStreamOperator<String, String, String> {
        @Override
        public void processElement1(StreamRecord<String> element) throws Exception {}

        @Override
        public void processElement2(StreamRecord<String> element) throws Exception {}
    }
}
