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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import java.util.Collections;
import java.util.List;

/** Tests for the facilities provided by {@link AbstractStreamOperatorV2}. */
public class AbstractStreamOperatorV2Test extends AbstractStreamOperatorTest {
    @Override
    protected KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
            createTestHarness(int maxParalelism, int numSubtasks, int subtaskIndex)
                    throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new TestOperatorFactory(),
                new TestKeySelector(),
                BasicTypeInfo.INT_TYPE_INFO,
                maxParalelism,
                numSubtasks,
                subtaskIndex);
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
    private static class SingleInputTestOperator extends AbstractStreamOperatorV2<String>
            implements MultipleInputStreamOperator<String>, Triggerable<Integer, VoidNamespace> {

        private static final long serialVersionUID = 1L;

        private transient InternalTimerService<VoidNamespace> timerService;

        private final ValueStateDescriptor<String> stateDescriptor =
                new ValueStateDescriptor<>("state", StringSerializer.INSTANCE);

        public SingleInputTestOperator(StreamOperatorParameters<String> parameters) {
            super(parameters, 1);
        }

        @Override
        public void open() throws Exception {
            super.open();

            this.timerService =
                    getInternalTimerService("test-timers", VoidNamespaceSerializer.INSTANCE, this);
        }

        @Override
        public List<Input> getInputs() {
            return Collections.singletonList(
                    new AbstractInput<Tuple2<Integer, String>, String>(this, 1) {
                        @Override
                        public void processElement(StreamRecord<Tuple2<Integer, String>> element)
                                throws Exception {
                            String[] command = element.getValue().f1.split(":");
                            switch (command[0]) {
                                case "SET_STATE":
                                    getPartitionedState(stateDescriptor).update(command[1]);
                                    break;
                                case "DELETE_STATE":
                                    getPartitionedState(stateDescriptor).clear();
                                    break;
                                case "SET_EVENT_TIME_TIMER":
                                    timerService.registerEventTimeTimer(
                                            VoidNamespace.INSTANCE, Long.parseLong(command[1]));
                                    break;
                                case "SET_PROC_TIME_TIMER":
                                    timerService.registerProcessingTimeTimer(
                                            VoidNamespace.INSTANCE, Long.parseLong(command[1]));
                                    break;
                                case "EMIT_STATE":
                                    String stateValue =
                                            getPartitionedState(stateDescriptor).value();
                                    output.collect(
                                            new StreamRecord<>(
                                                    "ON_ELEMENT:"
                                                            + element.getValue().f0
                                                            + ":"
                                                            + stateValue));
                                    break;
                                default:
                                    throw new IllegalArgumentException();
                            }
                        }
                    });
        }

        @Override
        public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
            String stateValue = getPartitionedState(stateDescriptor).value();
            output.collect(new StreamRecord<>("ON_EVENT_TIME:" + stateValue));
        }

        @Override
        public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
            String stateValue = getPartitionedState(stateDescriptor).value();
            output.collect(new StreamRecord<>("ON_PROC_TIME:" + stateValue));
        }
    }
}
