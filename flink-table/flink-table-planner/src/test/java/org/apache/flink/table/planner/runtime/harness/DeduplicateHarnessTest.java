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

package org.apache.flink.table.planner.runtime.harness;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Harness tests for the processing-time state retention of the built-in DEDUPLICATE PTF, which need
 * control over the time that state TTL observes.
 */
class DeduplicateHarnessTest {

    private static final Function<RowData, String> PAYMENT =
            row -> row.getString(0) + ", " + row.getInt(1);

    private static final Function<RowData, String> CUSTOMER_CHANGE =
            row -> row.getString(1) + ", " + row.getLong(2);

    private static Stream<Arguments> resetTtlOnDuplicate() {
        return Stream.of(
                Arguments.of(true, List.of("+I[p1, 50]@10:00", "+I[p1, 50]@10:20")),
                Arguments.of(
                        false,
                        List.of("+I[p1, 50]@10:00", "+I[p1, 50]@10:07", "+I[p1, 50]@10:20")));
    }

    @ParameterizedTest(name = "reset_ttl_on_duplicate={0}")
    @MethodSource("resetTtlOnDuplicate")
    void testTimeBoundedDeduplication(
            final boolean resetTtlOnDuplicate, final List<String> expected) throws Exception {
        try (KeyedMultiInputStreamOperatorTestHarness<RowData, ?> harness =
                createHarness(
                        "SELECT * FROM DEDUPLICATE("
                                + "input => TABLE payments PARTITION BY payment_id, "
                                + "state_ttl => INTERVAL '5' MINUTES, "
                                + "reset_ttl_on_duplicate => "
                                + resetTtlOnDuplicate
                                + ")")) {
            final List<String> output = new ArrayList<>();
            for (final String time : List.of("10:00", "10:04", "10:07", "10:20")) {
                output.addAll(process(harness, time, PAYMENT, payment(50)));
            }

            assertThat(output).isEqualTo(expected);
        }
    }

    @Test
    void testRecordAfterExpiryIsFirstRecord() throws Exception {
        try (KeyedMultiInputStreamOperatorTestHarness<RowData, ?> harness =
                createHarness(
                        "SELECT * FROM DEDUPLICATE("
                                + "input => TABLE customer_changes PARTITION BY customer_id, "
                                + "keep => 'last', "
                                + "sequence_by => DESCRIPTOR(lsn), "
                                + "state_ttl => INTERVAL '5' MINUTES)")) {
            final List<String> output = new ArrayList<>();
            output.addAll(
                    process(harness, "10:00", CUSTOMER_CHANGE, customerChange("Berlin", 120)));
            output.addAll(
                    process(harness, "10:02", CUSTOMER_CHANGE, customerChange("Munich", 100)));
            output.addAll(
                    process(harness, "10:20", CUSTOMER_CHANGE, customerChange("Hamburg", 90)));

            assertThat(output).containsExactly("+I[Berlin, 120]@10:00", "+I[Hamburg, 90]@10:20");
        }
    }

    @Test
    void testReplacingRecordRefreshesTtl() throws Exception {
        try (KeyedMultiInputStreamOperatorTestHarness<RowData, ?> harness =
                createHarness(
                        "SELECT * FROM DEDUPLICATE("
                                + "input => TABLE payments PARTITION BY payment_id, "
                                + "keep => 'last', "
                                + "state_ttl => INTERVAL '5' MINUTES, "
                                + "reset_ttl_on_duplicate => false)")) {
            final List<String> output = new ArrayList<>();
            output.addAll(process(harness, "10:00", PAYMENT, payment(50)));
            output.addAll(process(harness, "10:04", PAYMENT, payment(60)));
            output.addAll(process(harness, "10:07", PAYMENT, payment(60)));
            output.addAll(process(harness, "10:08", PAYMENT, payment(70)));
            output.addAll(process(harness, "10:14", PAYMENT, payment(70)));

            assertThat(output)
                    .containsExactly(
                            "+I[p1, 50]@10:00",
                            "-U[p1, 50]@10:04",
                            "+U[p1, 60]@10:04",
                            "-U[p1, 60]@10:08",
                            "+U[p1, 70]@10:08",
                            "+I[p1, 70]@10:14");
        }
    }

    @Test
    void testIdenticalRecordRefreshesTtl() throws Exception {
        try (KeyedMultiInputStreamOperatorTestHarness<RowData, ?> harness =
                createHarness(
                        "SELECT * FROM DEDUPLICATE("
                                + "input => TABLE payments PARTITION BY payment_id, "
                                + "keep => 'last', "
                                + "state_ttl => INTERVAL '5' MINUTES)")) {
            final List<String> output = new ArrayList<>();
            for (final String time : List.of("10:00", "10:04", "10:07")) {
                output.addAll(process(harness, time, PAYMENT, payment(50)));
            }

            assertThat(output).containsExactly("+I[p1, 50]@10:00");
        }
    }

    private static List<String> process(
            final KeyedMultiInputStreamOperatorTestHarness<RowData, ?> harness,
            final String time,
            final Function<RowData, String> describe,
            final RowData input)
            throws Exception {
        harness.setStateTtlProcessingTime(toMillis(time));
        harness.processElement(0, new StreamRecord<>(input));
        final List<String> output = new ArrayList<>();
        for (final Object record : harness.getOutput()) {
            final RowData row = (RowData) ((StreamRecord<?>) record).getValue();
            output.add(
                    String.format(
                            "%s[%s]@%s",
                            row.getRowKind().shortString(), describe.apply(row), time));
        }
        harness.getOutput().clear();
        return output;
    }

    private static RowData payment(final int amount) {
        return GenericRowData.of(StringData.fromString("p1"), amount);
    }

    private static RowData customerChange(final String city, final long lsn) {
        return GenericRowData.of(StringData.fromString("c1"), StringData.fromString(city), lsn);
    }

    private static KeyedMultiInputStreamOperatorTestHarness<RowData, ?> createHarness(
            final String query) throws Exception {
        final StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        tEnv.executeSql(
                "CREATE TABLE payments (payment_id STRING, amount INT) "
                        + "WITH ('connector' = 'values')");
        tEnv.executeSql(
                "CREATE TABLE customer_changes (customer_id STRING, city STRING, lsn BIGINT) "
                        + "WITH ('connector' = 'values')");
        final KeyedMultipleInputTransformation<?> transformation =
                findPtf(tEnv.toChangelogStream(tEnv.sqlQuery(query)).getTransformation())
                        .orElseThrow(() -> new AssertionError("No PTF transformation found."));
        return openHarness(
                transformation.getOperatorFactory(),
                (RowDataKeySelector) transformation.getStateKeySelectors().get(0));
    }

    private static <OUT> KeyedMultiInputStreamOperatorTestHarness<RowData, OUT> openHarness(
            final StreamOperatorFactory<OUT> operatorFactory, final RowDataKeySelector keySelector)
            throws Exception {
        final KeyedMultiInputStreamOperatorTestHarness<RowData, OUT> harness =
                new KeyedMultiInputStreamOperatorTestHarness<>(
                        operatorFactory, keySelector.getProducedType());
        harness.setKeySelector(0, keySelector);
        harness.open();
        return harness;
    }

    private static Optional<KeyedMultipleInputTransformation<?>> findPtf(
            final Transformation<?> transformation) {
        if (transformation instanceof KeyedMultipleInputTransformation) {
            return Optional.of((KeyedMultipleInputTransformation<?>) transformation);
        }
        return transformation.getInputs().stream()
                .map(DeduplicateHarnessTest::findPtf)
                .flatMap(Optional::stream)
                .findFirst();
    }

    private static long toMillis(final String time) {
        final String[] parts = time.split(":");
        return Duration.ofHours(Long.parseLong(parts[0]))
                .plusMinutes(Long.parseLong(parts[1]))
                .toMillis();
    }
}
