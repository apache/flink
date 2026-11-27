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
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Harness tests for the state TTL of the built-in DEDUPLICATE PTF. */
class DeduplicateHarnessTest {

    private static final Function<RowData, String> PAYMENT =
            row -> row.getString(0) + ", " + row.getInt(1);

    private static final Function<RowData, String> VISIT =
            row -> row.getString(0) + ", " + row.getString(1);

    @ParameterizedTest(name = "keep={0}")
    @ValueSource(strings = {"first", "last"})
    void testDuplicateResetsTtl(final String keep) throws Exception {
        try (KeyedMultiInputStreamOperatorTestHarness<RowData, ?> harness =
                createHarness(
                        "SELECT * FROM DEDUPLICATE("
                                + "input => TABLE payments PARTITION BY payment_id, "
                                + "keep => '"
                                + keep
                                + "', "
                                + "state_ttl => INTERVAL '5' MINUTES)")) {
            final List<String> output = new ArrayList<>();
            for (final String time : List.of("10:00", "10:04", "10:07", "10:20")) {
                output.addAll(process(harness, time, PAYMENT, payment(50)));
            }

            assertThat(output).containsExactly("+I[p1, 50]@10:00", "+I[p1, 50]@10:20");
        }
    }

    @Test
    void testLiveRowsForgetKeptRowWithItsEntry() throws Exception {
        try (KeyedMultiInputStreamOperatorTestHarness<RowData, ?> harness =
                createHarness(
                        "SELECT * FROM DEDUPLICATE("
                                + "input => TABLE visits PARTITION BY user_id, "
                                + "keep => 'first', "
                                + "state_ttl => INTERVAL '1' HOUR)")) {
            final List<String> output = new ArrayList<>();
            output.addAll(process(harness, "10:00", VISIT, visit(RowKind.INSERT, "a1")));
            output.addAll(process(harness, "10:40", VISIT, visit(RowKind.INSERT, "a2")));
            // a1 expired at 11:00: the kept row is chosen among the remembered rows
            output.addAll(process(harness, "11:10", VISIT, visit(RowKind.INSERT, "a3")));
            // a2 expired at 11:40, but a delete of the kept row still retracts it
            output.addAll(process(harness, "11:50", VISIT, visit(RowKind.DELETE, "a2")));

            assertThat(output)
                    .containsExactly(
                            "+I[u1, a1]@10:00",
                            "-U[u1, a1]@11:10",
                            "+U[u1, a2]@11:10",
                            "-U[u1, a2]@11:50",
                            "+U[u1, a3]@11:50");
        }
    }

    @Test
    void testLiveRowsContinueArrivalAfterCounterExpires() throws Exception {
        try (KeyedMultiInputStreamOperatorTestHarness<RowData, ?> harness =
                createHarness(
                        "SELECT * FROM DEDUPLICATE("
                                + "input => TABLE visits PARTITION BY user_id, "
                                + "keep => 'last', "
                                + "state_ttl => INTERVAL '1' HOUR)")) {
            final List<String> output = new ArrayList<>();
            output.addAll(process(harness, "10:00", VISIT, visit(RowKind.INSERT, "a")));
            output.addAll(process(harness, "10:05", VISIT, visit(RowKind.INSERT, "a")));
            output.addAll(process(harness, "10:10", VISIT, visit(RowKind.INSERT, "b")));
            output.addAll(process(harness, "10:50", VISIT, visit(RowKind.DELETE, "a")));
            output.addAll(process(harness, "11:00", VISIT, visit(RowKind.DELETE, "b")));
            // the arrival counter expired at 11:10, while a is still remembered
            output.addAll(process(harness, "11:20", VISIT, visit(RowKind.INSERT, "c")));

            assertThat(output)
                    .containsExactly(
                            "+I[u1, a]@10:00",
                            "-U[u1, a]@10:10",
                            "+U[u1, b]@10:10",
                            "-U[u1, b]@11:00",
                            "+U[u1, a]@11:00",
                            "-U[u1, a]@11:20",
                            "+U[u1, c]@11:20");
        }
    }

    @Test
    void testLiveRowsRetractionKeepsKeptRowAlive() throws Exception {
        try (KeyedMultiInputStreamOperatorTestHarness<RowData, ?> harness =
                createHarness(
                        "SELECT * FROM DEDUPLICATE("
                                + "input => TABLE visits PARTITION BY user_id, "
                                + "keep => 'first', "
                                + "state_ttl => INTERVAL '1' HOUR)")) {
            final List<String> output = new ArrayList<>();
            output.addAll(process(harness, "10:00", VISIT, visit(RowKind.INSERT, "x")));
            output.addAll(process(harness, "10:00", VISIT, visit(RowKind.INSERT, "y")));
            output.addAll(process(harness, "10:00", VISIT, visit(RowKind.INSERT, "y")));
            output.addAll(process(harness, "10:50", VISIT, visit(RowKind.DELETE, "y")));
            output.addAll(process(harness, "11:30", VISIT, visit(RowKind.INSERT, "z")));

            assertThat(output)
                    .containsExactly("+I[u1, x]@10:00", "-U[u1, x]@11:30", "+U[u1, y]@11:30");
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

    private static RowData visit(final RowKind kind, final String page) {
        return GenericRowData.ofKind(
                kind, StringData.fromString("u1"), StringData.fromString(page));
    }

    private static KeyedMultiInputStreamOperatorTestHarness<RowData, ?> createHarness(
            final String query) throws Exception {
        final StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        tEnv.executeSql(
                "CREATE TABLE payments (payment_id STRING, amount INT) "
                        + "WITH ('connector' = 'values')");
        tEnv.executeSql(
                "CREATE TABLE visits (user_id STRING, page STRING) "
                        + "WITH ('connector' = 'values', 'changelog-mode' = 'I,UB,UA,D')");
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
