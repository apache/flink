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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.ProcessTableRunner;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RuntimeChangelogMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Harness tests for {@link ProcessSetTableOperator}.
 *
 * <p>The tests use hand-written {@link ProcessTableRunner}s instead of generated code. This allows
 * for full control over the order of input rows, watermarks, and state access.
 */
class ProcessSetTableOperatorTest {

    private static final DataType MAIN_TYPE =
            DataTypes.ROW(DataTypes.FIELD("k", DataTypes.BIGINT()));

    // Output layout is driven by PassPartitionKeysCollector / PassAllCollector in
    // AbstractProcessTableOperator: [partition-key, PTF-emitted label].
    private static final DataType OUTPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("k", DataTypes.BIGINT()),
                    DataTypes.FIELD("label", DataTypes.STRING()));

    private static final InternalTypeInfo<RowData> MAIN_TYPE_INFO =
            InternalTypeInfo.of(MAIN_TYPE.getLogicalType());

    private static final InternalTypeInfo<RowData> OUTPUT_TYPE_INFO =
            InternalTypeInfo.of(OUTPUT_TYPE.getLogicalType());

    private static final RowDataKeySelector KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {0}, MAIN_TYPE_INFO.toRowFieldTypes());

    private static final RuntimeChangelogMode INSERT_ONLY =
            RuntimeChangelogMode.serialize(ChangelogMode.insertOnly());

    @Test
    void testNotifyStatefulSets() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> harness =
                createBroadcastHarness(new NotifyStatefulSetsOperatorCreator(true, 0))) {
            processMainRows(harness, 1L, 2L, 1L);
            // Notifies sets 1 and 2
            processBroadcastRow(harness, "a");
            // Notifies sets 1 and 2, which clear their state while iterating over the keys
            processBroadcastRow(harness, "clear");
            // No stateful sets left
            processBroadcastRow(harness, "b");
            // Fires timers registered during notification
            processWatermark(harness, TIMER_TIME, 2);

            final List<String> output = describeOutput(harness);
            assertThat(output).hasSize(9);
            assertThat(output.subList(0, 3)).containsExactly("1:count-1", "2:count-1", "1:count-2");
            // The order of sets is defined by the state backend
            assertThat(output.subList(3, 5))
                    .containsExactlyInAnyOrder(
                            "1:notified-2-a-read-only", "2:notified-1-a-read-only");
            assertThat(output.subList(5, 7))
                    .containsExactlyInAnyOrder(
                            "1:notified-2-clear-read-only", "2:notified-1-clear-read-only");
            assertThat(output.subList(7, 9)).containsExactlyInAnyOrder("1:fired", "2:fired");
        }
    }

    @Test
    void testWithoutNotifyStatefulSets() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> harness =
                createBroadcastHarness(new NotifyStatefulSetsOperatorCreator(false, 0))) {
            processMainRows(harness, 1L, 2L, 1L);
            processBroadcastRow(harness, "a");
            processWatermark(harness, TIMER_TIME, 2);

            assertThat(describeOutput(harness))
                    .containsExactly("1:count-1", "2:count-1", "1:count-2");
        }
    }

    @Test
    void testNonBinaryKey() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> harness =
                createHarness(new SingleInputOperatorCreator(), new GenericKeySelector())) {
            assertThatThrownBy(() -> processMainRows(harness, 1L))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(
                            "Partition keys must be BinaryRowData but was: "
                                    + GenericRowData.class.getName());
        }
    }

    @Test
    void testNotifyStatefulSetsWithExpiredState() throws Exception {
        try (KeyedMultiInputStreamOperatorTestHarness<RowData, RowData> harness =
                createTtlBroadcastHarness(new NotifyStatefulSetsOperatorCreator(true, 10))) {
            harness.setStateTtlProcessingTime(0);
            processMainRows(harness, 1L);
            harness.setStateTtlProcessingTime(12);
            // Notifies set 1 with expired state, reading it cleans it up in the heap backend
            processBroadcastRow(harness, "a");
            // No stateful sets left
            processBroadcastRow(harness, "b");

            processMainRows(harness, 2L);
            harness.setStateTtlProcessingTime(24);
            // Notifies set 2 with expired state, writing it keeps the set alive
            processBroadcastRow(harness, "reset");
            harness.setStateTtlProcessingTime(30);
            processBroadcastRow(harness, "c");

            assertThat(describeOutput(harness.getOutput()))
                    .containsExactly(
                            "1:count-1",
                            "1:notified-null-a-read-only",
                            "2:count-1",
                            "2:notified-null-reset-read-only",
                            "2:notified-0-c-read-only");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    /** Creates a {@link ProcessSetTableOperator} for the given operator parameters. */
    private interface OperatorCreator extends Serializable {
        ProcessSetTableOperator create(StreamOperatorParameters<RowData> parameters);
    }

    private static StreamTaskMailboxTestHarness<RowData> createHarness(
            OperatorCreator operatorCreator, InternalTypeInfo<?>... sideInputTypes)
            throws Exception {
        return createHarness(operatorCreator, KEY_SELECTOR, sideInputTypes);
    }

    private static StreamTaskMailboxTestHarness<RowData> createHarness(
            OperatorCreator operatorCreator,
            RowDataKeySelector keySelector,
            InternalTypeInfo<?>... sideInputTypes)
            throws Exception {
        final StreamTaskMailboxTestHarnessBuilder<RowData> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, OUTPUT_TYPE_INFO)
                        .setKeyType(keySelector.getProducedType())
                        .addInput(MAIN_TYPE_INFO, 1, keySelector);
        for (InternalTypeInfo<?> sideInputType : sideInputTypes) {
            builder.addInput(sideInputType, 1, null);
        }
        return builder.setupOperatorChain(new TestOperatorFactory(operatorCreator))
                .name("ptf")
                .finishForSingletonOperatorChain(
                        OUTPUT_TYPE_INFO.createSerializer(new SerializerConfigImpl()))
                .build();
    }

    private static ProcessSetTableOperator createOperator(
            StreamOperatorParameters<RowData> parameters,
            List<RuntimeTableSemantics> tableSemantics,
            List<RuntimeStateInfo> stateInfos,
            ProcessTableRunner runner) {
        final int inputCount = tableSemantics.size();
        final int stateCount = stateInfos.size();
        return new ProcessSetTableOperator(
                parameters,
                tableSemantics,
                stateInfos,
                new RecordComparator[inputCount],
                runner,
                new HashFunction[stateCount],
                new RecordEqualiser[stateCount],
                INSERT_ONLY,
                List.of());
    }

    /** Table with set semantics that is partitioned by its first column. */
    private static RuntimeTableSemantics mainTableSemantics() {
        return new RuntimeTableSemantics(
                "main",
                0,
                MAIN_TYPE,
                new int[] {0},
                new int[0],
                new RuntimeTableSemantics.SortDirection[0],
                INSERT_ONLY,
                /* passColumnsThrough */ false,
                /* hasSetSemantics */ true,
                /* hasBroadcastSemantics */ false,
                /* notifiesStatefulSets */ false,
                /* timeColumn */ -1,
                /* upsertKeyColumns */ List.of());
    }

    private static void processMainRows(StreamTaskMailboxTestHarness<RowData> harness, long... keys)
            throws Exception {
        for (long key : keys) {
            harness.processElement(new StreamRecord<>(GenericRowData.of(key)), 0);
        }
    }

    private static void processMainRows(
            KeyedMultiInputStreamOperatorTestHarness<RowData, RowData> harness, long... keys)
            throws Exception {
        for (long key : keys) {
            harness.processElement(0, new StreamRecord<>(GenericRowData.of(key)));
        }
    }

    /** Advances the watermark of all inputs. */
    private static void processWatermark(
            StreamTaskMailboxTestHarness<RowData> harness, long watermark, int inputCount)
            throws Exception {
        for (int input = 0; input < inputCount; input++) {
            harness.processElement(new Watermark(watermark), input);
        }
        harness.processAll();
    }

    /** Describes emitted rows as "key:label". */
    private static List<String> describeOutput(StreamTaskMailboxTestHarness<RowData> harness) {
        return describeOutput(harness.getOutput());
    }

    private static List<String> describeOutput(Collection<Object> output) {
        return output.stream()
                .filter(StreamRecord.class::isInstance)
                .map(r -> (RowData) ((StreamRecord<?>) r).getValue())
                .map(r -> r.getLong(0) + ":" + r.getString(1))
                .collect(Collectors.toList());
    }

    /** Key selector that violates the requirement of binary keys. */
    private static class GenericKeySelector implements RowDataKeySelector {
        private static final long serialVersionUID = 1L;

        @Override
        public RowData getKey(RowData value) {
            return GenericRowData.of(value.getLong(0));
        }

        @Override
        public InternalTypeInfo<RowData> getProducedType() {
            return KEY_SELECTOR.getProducedType();
        }

        @Override
        public RowDataKeySelector copy() {
            return new GenericKeySelector();
        }
    }

    /** Operator creator for a single main table that never evaluates rows. */
    private static class SingleInputOperatorCreator implements OperatorCreator {
        private static final long serialVersionUID = 1L;

        @Override
        public ProcessSetTableOperator create(StreamOperatorParameters<RowData> parameters) {
            return createOperator(
                    parameters,
                    List.of(mainTableSemantics()),
                    List.of(),
                    new ProcessTableRunner() {
                        @Override
                        public void callEval() {}

                        @Override
                        public void callOnTimer() {}
                    });
        }
    }

    private static class TestOperatorFactory extends AbstractStreamOperatorFactory<RowData> {
        private static final long serialVersionUID = 1L;

        private final OperatorCreator operatorCreator;

        TestOperatorFactory(OperatorCreator operatorCreator) {
            this.operatorCreator = operatorCreator;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<RowData>> T createStreamOperator(
                StreamOperatorParameters<RowData> parameters) {
            return (T) operatorCreator.create(parameters);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return ProcessSetTableOperator.class;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Broadcast
    // --------------------------------------------------------------------------------------------

    private static final DataType BROADCAST_TYPE =
            DataTypes.ROW(DataTypes.FIELD("rule", DataTypes.STRING()));

    private static final InternalTypeInfo<RowData> BROADCAST_TYPE_INFO =
            InternalTypeInfo.of(BROADCAST_TYPE.getLogicalType());

    private static final long TIMER_TIME = 100L;

    private static StreamTaskMailboxTestHarness<RowData> createBroadcastHarness(
            OperatorCreator operatorCreator) throws Exception {
        return createHarness(operatorCreator, BROADCAST_TYPE_INFO);
    }

    /**
     * Creates an operator-level harness instead of a task-level harness, because only the former
     * allows controlling the time of state TTL.
     */
    private static KeyedMultiInputStreamOperatorTestHarness<RowData, RowData>
            createTtlBroadcastHarness(OperatorCreator operatorCreator) throws Exception {
        final KeyedMultiInputStreamOperatorTestHarness<RowData, RowData> harness =
                new KeyedMultiInputStreamOperatorTestHarness<>(
                        new TestOperatorFactory(operatorCreator), KEY_SELECTOR.getProducedType());
        harness.setKeySelector(0, KEY_SELECTOR);
        harness.open();
        return harness;
    }

    private static RuntimeTableSemantics broadcastTableSemantics(boolean notifyStatefulSets) {
        return new RuntimeTableSemantics(
                "broadcast",
                1,
                BROADCAST_TYPE,
                new int[0],
                new int[0],
                new RuntimeTableSemantics.SortDirection[0],
                INSERT_ONLY,
                /* passColumnsThrough */ false,
                /* hasSetSemantics */ false,
                /* hasBroadcastSemantics */ true,
                notifyStatefulSets,
                /* timeColumn */ -1,
                /* upsertKeyColumns */ List.of());
    }

    private static void processBroadcastRow(
            StreamTaskMailboxTestHarness<RowData> harness, String rule) throws Exception {
        harness.processElement(
                new StreamRecord<>(GenericRowData.of(StringData.fromString(rule))), 1);
    }

    private static void processBroadcastRow(
            KeyedMultiInputStreamOperatorTestHarness<RowData, RowData> harness, String rule)
            throws Exception {
        harness.processElement(
                1, new StreamRecord<>(GenericRowData.of(StringData.fromString(rule))));
    }

    /** Operator for {@link NotifyStatefulSetsRunner} with a keyed count and a broadcast rule. */
    private static class NotifyStatefulSetsOperatorCreator implements OperatorCreator {
        private static final long serialVersionUID = 1L;

        private final boolean notifyStatefulSets;
        private final long countTimeToLive;

        NotifyStatefulSetsOperatorCreator(boolean notifyStatefulSets, long countTimeToLive) {
            this.notifyStatefulSets = notifyStatefulSets;
            this.countTimeToLive = countTimeToLive;
        }

        @Override
        public ProcessSetTableOperator create(StreamOperatorParameters<RowData> parameters) {
            return createOperator(
                    parameters,
                    List.of(mainTableSemantics(), broadcastTableSemantics(notifyStatefulSets)),
                    List.of(
                            new RuntimeStateInfo(
                                    "count",
                                    ValueView.newValueViewDataType(DataTypes.INT()),
                                    countTimeToLive,
                                    false),
                            new RuntimeStateInfo(
                                    "rule",
                                    ValueView.newValueViewDataType(DataTypes.STRING()),
                                    0,
                                    true)),
                    new NotifyStatefulSetsRunner());
        }
    }

    /**
     * {@link ProcessTableRunner} that counts rows of the main table per set. A broadcast row
     * updates a broadcast rule. If notified, every stateful set emits its count together with the
     * rule and registers a timer. The rule "clear" clears the set's state, the rule "reset" resets
     * the set's count. State handles are accessed directly instead of converting state like
     * generated code does.
     */
    private static class NotifyStatefulSetsRunner extends ProcessTableRunner {

        @Override
        @SuppressWarnings("unchecked")
        public void callEval() throws Exception {
            final ValueState<Integer> count = (ValueState<Integer>) stateHandles[0].getState();
            final ValueState<String> rule = (ValueState<String>) stateHandles[1].getState();
            if (inputIndex == 0) {
                final int c = Optional.ofNullable(count.value()).orElse(0) + 1;
                count.update(c);
                emit("count-" + c);
            } else if (isProcessingBroadcast()) {
                rule.update(inputRow.getString(0).toString());
            } else {
                String label = "notified-" + count.value() + "-" + rule.value();
                try {
                    rule.update("invalid");
                } catch (TableRuntimeException e) {
                    label += "-read-only";
                }
                emit(label);
                runnerContext.timeContext(Long.class).registerOnTime(TIMER_TIME);
                if (rule.value().equals("clear")) {
                    count.clear();
                } else if (rule.value().equals("reset")) {
                    count.update(0);
                }
            }
        }

        @Override
        public void callOnTimer() {
            onTimerCollector.collect(GenericRowData.of(StringData.fromString("fired")));
        }

        private void emit(String label) {
            evalCollector.collect(GenericRowData.of(StringData.fromString(label)));
        }
    }
}
