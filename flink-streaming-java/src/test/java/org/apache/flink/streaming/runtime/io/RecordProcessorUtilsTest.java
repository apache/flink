/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.KeyContextHandler;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskITCase;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RecordProcessorUtils}. */
class RecordProcessorUtilsTest {

    @Test
    void testGetRecordProcessor() throws Exception {
        TestOperator input1 = new TestOperator();
        TestOperator input2 = new TestKeyContextHandlerOperator(true);
        TestOperator input3 = new TestKeyContextHandlerOperator(false);

        RecordProcessorUtils.getRecordProcessor(input1).accept(new StreamRecord<>("test"));
        assertThat(input1.setKeyContextElementCalled).isTrue();
        assertThat(input1.processElementCalled).isTrue();

        RecordProcessorUtils.getRecordProcessor(input2).accept(new StreamRecord<>("test"));
        assertThat(input2.setKeyContextElementCalled).isTrue();
        assertThat(input2.processElementCalled).isTrue();

        RecordProcessorUtils.getRecordProcessor(input3).accept(new StreamRecord<>("test"));
        assertThat(input3.setKeyContextElementCalled).isFalse();
        assertThat(input3.processElementCalled).isTrue();
    }

    @Test
    void testGetRecordProcessor1() throws Exception {
        TestOperator operator1 = new TestOperator();
        TestOperator operator2 = new TestKeyContextHandlerOperator(true, true);
        TestOperator operator3 = new TestKeyContextHandlerOperator(false, true);

        RecordProcessorUtils.getRecordProcessor1(operator1).accept(new StreamRecord<>("test"));
        assertThat(operator1.setKeyContextElement1Called).isTrue();
        assertThat(operator1.processElement1Called).isTrue();

        RecordProcessorUtils.getRecordProcessor1(operator2).accept(new StreamRecord<>("test"));
        assertThat(operator2.setKeyContextElement1Called).isTrue();
        assertThat(operator2.processElement1Called).isTrue();

        RecordProcessorUtils.getRecordProcessor1(operator3).accept(new StreamRecord<>("test"));
        assertThat(operator3.setKeyContextElement1Called).isFalse();
        assertThat(operator3.processElement1Called).isTrue();
    }

    @Test
    void testGetRecordProcessor2() throws Exception {
        TestOperator operator1 = new TestOperator();
        TestOperator operator2 = new TestKeyContextHandlerOperator(true, true);
        TestOperator operator3 = new TestKeyContextHandlerOperator(true, false);

        RecordProcessorUtils.getRecordProcessor2(operator1).accept(new StreamRecord<>("test"));
        assertThat(operator1.setKeyContextElement2Called).isTrue();
        assertThat(operator1.processElement2Called).isTrue();

        RecordProcessorUtils.getRecordProcessor2(operator2).accept(new StreamRecord<>("test"));
        assertThat(operator2.setKeyContextElement2Called).isTrue();
        assertThat(operator2.processElement2Called).isTrue();

        RecordProcessorUtils.getRecordProcessor2(operator3).accept(new StreamRecord<>("test"));
        assertThat(operator3.setKeyContextElement2Called).isFalse();
        assertThat(operator3.processElement2Called).isTrue();
    }

    @Test
    void testOverrideSetKeyContextElementForOneInputStreamOperator() throws Exception {
        // test no override
        NoOverrideOneInputStreamOperator noOverride = new NoOverrideOneInputStreamOperator();
        RecordProcessorUtils.getRecordProcessor(noOverride).accept(new StreamRecord<>("test"));
        assertThat(noOverride.setCurrentKeyCalled).isFalse();

        // test override "SetKeyContextElement"
        OverrideSetKeyContextOneInputStreamOperator overrideSetKeyContext =
                new OverrideSetKeyContextOneInputStreamOperator();
        RecordProcessorUtils.getRecordProcessor(overrideSetKeyContext)
                .accept(new StreamRecord<>("test"));
        assertThat(overrideSetKeyContext.setKeyContextElementCalled).isTrue();

        // test override "SetKeyContextElement1"
        OverrideSetKeyContext1OneInputStreamOperator overrideSetKeyContext1 =
                new OverrideSetKeyContext1OneInputStreamOperator();
        RecordProcessorUtils.getRecordProcessor(overrideSetKeyContext1)
                .accept(new StreamRecord<>("test"));
        assertThat(overrideSetKeyContext1.setKeyContextElement1Called).isTrue();
    }

    @Test
    void testOverrideSetKeyContextElementForTwoInputStreamOperator() throws Exception {
        // test no override
        NoOverrideTwoInputStreamOperator noOverride = new NoOverrideTwoInputStreamOperator();
        RecordProcessorUtils.getRecordProcessor1(noOverride).accept(new StreamRecord<>("test"));
        RecordProcessorUtils.getRecordProcessor2(noOverride).accept(new StreamRecord<>("test"));
        assertThat(noOverride.setCurrentKeyCalled).isFalse();

        // test override "SetKeyContextElement1" and "SetKeyContextElement2"
        OverrideSetKeyContext1AndSetKeyContext2TwoInputStreamOperator override =
                new OverrideSetKeyContext1AndSetKeyContext2TwoInputStreamOperator();
        RecordProcessorUtils.getRecordProcessor1(override).accept(new StreamRecord<>("test"));
        RecordProcessorUtils.getRecordProcessor2(override).accept(new StreamRecord<>("test"));
        assertThat(override.setKeyContextElement1Called).isTrue();
        assertThat(override.setKeyContextElement2Called).isTrue();
    }

    private static class NoOverrideOperator extends AbstractStreamOperator<String> {

        boolean setCurrentKeyCalled = false;

        NoOverrideOperator() throws Exception {
            super();
            // For case that "SetKeyContextElement" has not been overridden,
            // we can determine whether the "SetKeyContextElement" is called through
            // "setCurrentKey". According to the implementation, we need to make the
            // "stateKeySelector1/stateKeySelector2" not null. Besides, we override the
            // "hasKeyContext1" and "hasKeyContext2" to avoid "stateKeySelector1/stateKeySelector2"
            // from affecting the return value
            Configuration configuration = new Configuration();
            KeySelector keySelector = x -> x;
            InstantiationUtil.writeObjectToConfig(keySelector, configuration, "statePartitioner0");
            InstantiationUtil.writeObjectToConfig(keySelector, configuration, "statePartitioner1");
            setup(
                    new StreamTaskITCase.NoOpStreamTask<>(new DummyEnvironment()),
                    new MockStreamConfig(configuration, 1),
                    new MockOutput<>(new ArrayList<>()));
        }

        @Override
        public boolean hasKeyContext1() {
            return false;
        }

        @Override
        public boolean hasKeyContext2() {
            return false;
        }

        @Override
        public void setCurrentKey(Object key) {
            setCurrentKeyCalled = true;
        }
    }

    private static class NoOverrideOneInputStreamOperator extends NoOverrideOperator
            implements OneInputStreamOperator<String, String> {

        NoOverrideOneInputStreamOperator() throws Exception {
            super();
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {}
    }

    private static class OverrideSetKeyContextOneInputStreamOperator
            extends NoOverrideOneInputStreamOperator {
        boolean setKeyContextElementCalled = false;

        OverrideSetKeyContextOneInputStreamOperator() throws Exception {
            super();
        }

        @Override
        public void setKeyContextElement(StreamRecord<String> record) throws Exception {
            setKeyContextElementCalled = true;
        }
    }

    private static class OverrideSetKeyContext1OneInputStreamOperator
            extends NoOverrideOneInputStreamOperator {
        boolean setKeyContextElement1Called = false;

        OverrideSetKeyContext1OneInputStreamOperator() throws Exception {
            super();
        }

        @Override
        public void setKeyContextElement1(StreamRecord record) throws Exception {
            setKeyContextElement1Called = true;
        }
    }

    private static class NoOverrideTwoInputStreamOperator extends NoOverrideOperator
            implements TwoInputStreamOperator<String, String, String> {

        NoOverrideTwoInputStreamOperator() throws Exception {
            super();
        }

        @Override
        public void processElement1(StreamRecord<String> element) throws Exception {}

        @Override
        public void processElement2(StreamRecord<String> element) throws Exception {}
    }

    private static class OverrideSetKeyContext1AndSetKeyContext2TwoInputStreamOperator
            extends NoOverrideTwoInputStreamOperator {

        boolean setKeyContextElement1Called = false;

        boolean setKeyContextElement2Called = false;

        OverrideSetKeyContext1AndSetKeyContext2TwoInputStreamOperator() throws Exception {
            super();
        }

        @Override
        public void setKeyContextElement1(StreamRecord record) throws Exception {
            setKeyContextElement1Called = true;
        }

        @Override
        public void setKeyContextElement2(StreamRecord record) throws Exception {
            setKeyContextElement2Called = true;
        }
    }

    private static class TestOperator
            implements Input<String>, TwoInputStreamOperator<String, String, String> {
        boolean setKeyContextElementCalled = false;
        boolean processElementCalled = false;

        boolean setKeyContextElement1Called = false;
        boolean processElement1Called = false;

        boolean setKeyContextElement2Called = false;
        boolean processElement2Called = false;

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            processElementCalled = true;
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {}

        @Override
        public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {}

        @Override
        public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {}

        @Override
        public void setKeyContextElement(StreamRecord<String> record) throws Exception {
            setKeyContextElementCalled = true;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {}

        @Override
        public void setCurrentKey(Object key) {}

        @Override
        public Object getCurrentKey() {
            return null;
        }

        @Override
        public void open() throws Exception {}

        @Override
        public void finish() throws Exception {}

        @Override
        public void close() throws Exception {}

        @Override
        public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {}

        @Override
        public OperatorSnapshotFutures snapshotState(
                long checkpointId,
                long timestamp,
                CheckpointOptions checkpointOptions,
                CheckpointStreamFactory storageLocation)
                throws Exception {
            return null;
        }

        @Override
        public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
                throws Exception {}

        @Override
        public OperatorMetricGroup getMetricGroup() {
            return null;
        }

        @Override
        public OperatorID getOperatorID() {
            return null;
        }

        @Override
        public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
            setKeyContextElement1Called = true;
        }

        @Override
        public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
            setKeyContextElement2Called = true;
        }

        @Override
        public void processElement1(StreamRecord<String> element) throws Exception {
            processElement1Called = true;
        }

        @Override
        public void processElement2(StreamRecord<String> element) throws Exception {
            processElement2Called = true;
        }

        @Override
        public void processWatermark1(Watermark mark) throws Exception {}

        @Override
        public void processWatermark2(Watermark mark) throws Exception {}

        @Override
        public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {}

        @Override
        public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {}

        @Override
        public void processWatermarkStatus1(WatermarkStatus watermarkStatus) throws Exception {}

        @Override
        public void processWatermarkStatus2(WatermarkStatus watermarkStatus) throws Exception {}
    }

    private static class TestKeyContextHandlerOperator extends TestOperator
            implements KeyContextHandler {
        private final boolean hasKeyContext1;
        private final boolean hasKeyContext2;

        TestKeyContextHandlerOperator(boolean hasKeyContext) {
            this.hasKeyContext1 = hasKeyContext;
            this.hasKeyContext2 = true;
        }

        TestKeyContextHandlerOperator(boolean hasKeyContext1, boolean hasKeyContext2) {
            this.hasKeyContext1 = hasKeyContext1;
            this.hasKeyContext2 = hasKeyContext2;
        }

        @Override
        public boolean hasKeyContext() {
            return hasKeyContext1;
        }

        @Override
        public boolean hasKeyContext1() {
            return hasKeyContext1;
        }

        @Override
        public boolean hasKeyContext2() {
            return hasKeyContext2;
        }
    }
}
