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

package org.apache.flink.streaming.api.operators.sorted.state;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The single-key-at-a-time {@link BatchExecutionKeyedStateBackend} only moves forward through the
 * sorted keys. Switching back to an earlier key would silently fire its timers early and lose its
 * state, so it must fail loudly.
 */
class BatchExecutionKeyOrderTest {

    @Test
    void testInputPathMovesThroughKeys() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> harness =
                createBatchHarness(new KeyRevisitingOperator(false))) {
            harness.open();
            for (int key = 1; key <= 3; key++) {
                harness.processElement(new StreamRecord<>(key));
                harness.processElement(new StreamRecord<>(key));
            }
            assertThat(harness.extractOutputValues()).containsExactly(1, 1, 2, 2, 3, 3);
        }
    }

    @Test
    void testOperatorSwitchingBackToPreviousKeyFails() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> harness =
                createBatchHarness(new KeyRevisitingOperator(true))) {
            harness.open();
            harness.processElement(new StreamRecord<>(1));
            assertThatThrownBy(() -> harness.processElement(new StreamRecord<>(2)))
                    .hasStackTraceContaining("Cannot switch the current key from 2 to 1");
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer>
            createBatchHarness(OneInputStreamOperator<Integer, Integer> operator) throws Exception {
        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, (KeySelector<Integer, Integer>) value -> value, Types.INT);
        harness.setStateBackend(new BatchExecutionStateBackend());
        // As in BATCH execution, where keyed inputs are sorted by key.
        StreamConfig config = harness.getStreamConfig();
        config.setInputs(
                new StreamConfig.NetworkInputConfig(
                        IntSerializer.INSTANCE, 0, StreamConfig.InputRequirement.SORTED));
        config.serializeAllConfigs();
        return harness;
    }

    /**
     * Emits each record. With {@code revisit}, it first switches back to the previous record's key
     * (as an operator flushing a cross-key buffer would) and then to the current one.
     */
    private static class KeyRevisitingOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        private final boolean revisit;
        private Integer previousKey;

        KeyRevisitingOperator(boolean revisit) {
            this.revisit = revisit;
        }

        @Override
        public void processElement(StreamRecord<Integer> element) {
            Integer key = (Integer) getCurrentKey();
            // Same-key calls are always fine.
            setCurrentKey(key);
            if (revisit && previousKey != null && !previousKey.equals(key)) {
                setCurrentKey(previousKey);
                setCurrentKey(key);
            }
            previousKey = key;
            output.collect(element);
        }
    }
}
