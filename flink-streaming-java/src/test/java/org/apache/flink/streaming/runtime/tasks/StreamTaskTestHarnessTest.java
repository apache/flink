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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Assert;
import org.junit.Test;

/** Tests for the {@link StreamTaskTestHarness}. */
public class StreamTaskTestHarnessTest {

    @Test
    public void testMultipleSetupsThrowsException() {
        StreamTaskTestHarness<String> harness;

        harness =
                new StreamTaskTestHarness<>(
                        OneInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO);
        harness.setupOutputForSingletonOperatorChain();

        try {
            harness.setupOutputForSingletonOperatorChain();
            Assert.fail();
        } catch (IllegalStateException expected) {
            // expected
        }
        try {
            harness.setupOperatorChain(new OperatorID(), new TestOperator());
            Assert.fail();
        } catch (IllegalStateException expected) {
            // expected
        }
        try {
            harness.setupOperatorChain(new OperatorID(), new TwoInputTestOperator());
            Assert.fail();
        } catch (IllegalStateException expected) {
            // expected
        }

        harness =
                new StreamTaskTestHarness<>(
                        OneInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO);
        harness.setupOperatorChain(new OperatorID(), new TestOperator())
                .chain(
                        new OperatorID(),
                        new TestOperator(),
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()));

        try {
            harness.setupOutputForSingletonOperatorChain();
            Assert.fail();
        } catch (IllegalStateException expected) {
            // expected
        }
        try {
            harness.setupOperatorChain(new OperatorID(), new TestOperator());
            Assert.fail();
        } catch (IllegalStateException expected) {
            // expected
        }
        try {
            harness.setupOperatorChain(new OperatorID(), new TwoInputTestOperator());
            Assert.fail();
        } catch (IllegalStateException expected) {
            // expected
        }

        harness =
                new StreamTaskTestHarness<>(
                        TwoInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO);
        harness.setupOperatorChain(new OperatorID(), new TwoInputTestOperator())
                .chain(
                        new OperatorID(),
                        new TestOperator(),
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()));

        try {
            harness.setupOutputForSingletonOperatorChain();
            Assert.fail();
        } catch (IllegalStateException expected) {
            // expected
        }
        try {
            harness.setupOperatorChain(new OperatorID(), new TestOperator());
            Assert.fail();
        } catch (IllegalStateException expected) {
            // expected
        }
        try {
            harness.setupOperatorChain(new OperatorID(), new TwoInputTestOperator());
            Assert.fail();
        } catch (IllegalStateException expected) {
            // expected
        }
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
