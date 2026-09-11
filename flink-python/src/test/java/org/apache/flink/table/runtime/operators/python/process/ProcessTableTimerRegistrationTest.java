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

package org.apache.flink.table.runtime.operators.python.process;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.operators.process.WritableInternalTimeContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.StringDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.setCurrentKeyForStreaming;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ProcessTableTimerRegistration}. */
class ProcessTableTimerRegistrationTest {

    private static final RowType KEY_TYPE =
            RowType.of(new LogicalType[] {new VarCharType()}, new String[] {"key"});
    private static final RowType TIMER_DATA_TYPE =
            RowType.of(
                    new LogicalType[] {
                        new TinyIntType(false),
                        new BigIntType(true),
                        new VarCharType(true, VarCharType.MAX_LENGTH),
                        KEY_TYPE,
                        new BigIntType(true),
                        new BigIntType(true)
                    },
                    new String[] {
                        "operation",
                        "timestamp",
                        "name",
                        "key",
                        "table_watermark",
                        "current_watermark"
                    });

    @Test
    void testNamedAnonymousDeleteAndClearOperations() throws Exception {
        final TestTimerOperator operator = new TestTimerOperator();
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(operator, null)) {
            operator.setTimer(ProcessTableTimerRegistration.REGISTER_NAMED, 10L, "replace", "A");
            operator.setTimer(ProcessTableTimerRegistration.REGISTER_NAMED, 20L, "replace", "A");
            operator.setTimer(ProcessTableTimerRegistration.REGISTER_ANONYMOUS, 15L, null, "A");
            operator.setTimer(ProcessTableTimerRegistration.DELETE_ANONYMOUS, 15L, null, "A");
            operator.setTimer(ProcessTableTimerRegistration.REGISTER_ANONYMOUS, 18L, null, "A");

            harness.processWatermark(new Watermark(19L));
            assertThat(operator.firedTimers).containsExactly("A:<anonymous>:18");

            harness.processWatermark(new Watermark(20L));
            assertThat(operator.firedTimers).containsExactly("A:<anonymous>:18", "A:replace:20");
        }
    }

    @Test
    void testDeleteAndClearAllAreScopedToCurrentKey() throws Exception {
        final TestTimerOperator operator = new TestTimerOperator();
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(operator, null)) {
            operator.setTimer(ProcessTableTimerRegistration.REGISTER_NAMED, 10L, "delete", "A");
            operator.setTimer(ProcessTableTimerRegistration.DELETE_NAMED, null, "delete", "A");
            operator.setTimer(ProcessTableTimerRegistration.REGISTER_NAMED, 11L, "clear", "A");
            operator.setTimer(ProcessTableTimerRegistration.REGISTER_ANONYMOUS, 12L, null, "A");
            operator.setTimer(ProcessTableTimerRegistration.CLEAR_ALL, null, null, "A");
            operator.setTimer(ProcessTableTimerRegistration.REGISTER_ANONYMOUS, 13L, null, "A");

            harness.processWatermark(new Watermark(13L));

            assertThat(operator.firedTimers).containsExactly("A:<anonymous>:13");
        }
    }

    @Test
    void testTimersAreRestoredFromCheckpoint() throws Exception {
        final OperatorSubtaskState snapshot;
        final TestTimerOperator firstOperator = new TestTimerOperator();
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(firstOperator, null)) {
            firstOperator.setTimer(
                    ProcessTableTimerRegistration.REGISTER_NAMED, 50L, "restored", "A");
            firstOperator.setTimer(
                    ProcessTableTimerRegistration.REGISTER_ANONYMOUS, 51L, null, "A");
            snapshot = harness.snapshot(1L, 1L);
        }

        final TestTimerOperator restoredOperator = new TestTimerOperator();
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(restoredOperator, snapshot)) {
            harness.processWatermark(new Watermark(51L));

            assertThat(restoredOperator.firedTimers)
                    .containsExactlyInAnyOrder("A:restored:50", "A:<anonymous>:51");
        }
    }

    @Test
    void testRejectsUnknownTimerOperation() throws Exception {
        final TestTimerOperator operator = new TestTimerOperator();
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> ignored =
                createHarness(operator, null)) {
            assertThatThrownBy(() -> operator.setTimer((byte) 99, null, null, "A"))
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasMessageContaining("Failed to apply a Python PTF timer command")
                    .hasRootCauseMessage("Unknown PTF timer operation: 99");
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> createHarness(
            TestTimerOperator operator, OperatorSubtaskState initialState) throws Exception {
        final KeySelector<RowData, RowData> keySelector =
                value -> GenericRowData.of(value.getString(0));
        final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, keySelector, InternalTypeInfo.of(KEY_TYPE), 1, 1, 0);
        harness.setup(new RowDataSerializer(KEY_TYPE));
        if (initialState != null) {
            harness.initializeState(initialState);
        }
        harness.open();
        return harness;
    }

    private static final class TestTimerOperator extends AbstractStreamOperator<RowData>
            implements OneInputStreamOperator<RowData, RowData>, Triggerable<RowData, Object> {

        private static final long serialVersionUID = 1L;

        private final List<String> firedTimers = new ArrayList<>();
        private transient MapState<StringData, Long> namedTimers;
        private transient ProcessTableTimerRegistration registration;
        private transient RowDataSerializer timerDataSerializer;
        private transient Object keyForTimerService;

        @Override
        public void open() throws Exception {
            super.open();
            timerDataSerializer = new RowDataSerializer(TIMER_DATA_TYPE);
            namedTimers =
                    getKeyedStateStore()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "test-named-timers",
                                            StringDataSerializer.INSTANCE,
                                            LongSerializer.INSTANCE));
            final InternalTimerService<StringData> namedTimerService =
                    getInternalTimerService(
                            "test-named-timer-service",
                            StringDataSerializer.INSTANCE,
                            (Triggerable) this);
            final InternalTimerService<VoidNamespace> anonymousTimerService =
                    getInternalTimerService(
                            "test-anonymous-timer-service",
                            VoidNamespaceSerializer.INSTANCE,
                            (Triggerable) this);
            registration =
                    new ProcessTableTimerRegistration(
                            this,
                            getKeyedStateBackend(),
                            new WritableInternalTimeContext(
                                    namedTimers, namedTimerService, anonymousTimerService),
                            timerDataSerializer,
                            1);
        }

        @Override
        public void processElement(StreamRecord<RowData> element) {}

        @Override
        public void setCurrentKey(Object key) {
            keyForTimerService = key;
        }

        @Override
        public Object getCurrentKey() {
            return keyForTimerService;
        }

        @Override
        public void onEventTime(InternalTimer<RowData, Object> timer) throws Exception {
            setCurrentKey(timer.getKey());
            setCurrentKeyForStreaming(
                    (org.apache.flink.runtime.state.KeyedStateBackend<RowData>)
                            (org.apache.flink.runtime.state.KeyedStateBackend<?>)
                                    getKeyedStateBackend(),
                    timer.getKey());
            final String timerName;
            if (timer.getNamespace() == VoidNamespace.INSTANCE) {
                timerName = "<anonymous>";
            } else {
                final StringData name = (StringData) timer.getNamespace();
                namedTimers.remove(name);
                timerName = name.toString();
            }
            firedTimers.add(
                    timer.getKey().getString(0) + ":" + timerName + ":" + timer.getTimestamp());
        }

        @Override
        public void onProcessingTime(InternalTimer<RowData, Object> timer) {}

        private void setTimer(byte operation, Long timestamp, String name, String key)
                throws Exception {
            final GenericRowData command = new GenericRowData(6);
            command.setField(0, operation);
            command.setField(1, timestamp);
            command.setField(2, name == null ? null : StringData.fromString(name));
            command.setField(3, GenericRowData.of(StringData.fromString(key)));
            command.setField(4, null);
            command.setField(5, null);

            final ByteArrayOutputStreamWithPos output = new ByteArrayOutputStreamWithPos();
            timerDataSerializer.serialize(command, new DataOutputViewStreamWrapper(output));
            registration.setTimer(output.toByteArray());
        }
    }
}
