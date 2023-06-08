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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.IDLE_STATE_RETENTION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.configuredSerdeContext;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.testJsonRoundTrip;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test json serialization/deserialization and util methods for {@link
 * org.apache.flink.table.planner.plan.nodes.exec.StateMetadata}.
 */
@Execution(ExecutionMode.CONCURRENT)
public class StateMetadataTest {

    private TableConfig tableConfig;

    @BeforeEach
    public void beforeEach() {
        tableConfig = TableConfig.getDefault();
    }

    @CsvSource({"0,0hour,fooState", "1,600000ms,barState", "2,10minute,meowState"})
    @ParameterizedTest
    public void testStateMetadataSerde(int index, String ttl, String name) throws IOException {
        testJsonRoundTrip(new StateMetadata(index, ttl, name), StateMetadata.class);
    }

    @CsvSource(
            value = {
                "{\"index\":0,\"name\":\"fooState\"}|state ttl should not be null",
                "{\"index\":-1,\"ttl\":\"3600000ms\",\"name\":\"barState\"}|state index should start from 0",
                "{\"ttl\":\"3600000ms\",\"index\":1}|state name should not be null"
            },
            delimiterString = "|")
    @ParameterizedTest
    public void testDeserializeFromMalformedJson(String malformedJson, String expectedMsg) {
        assertThatThrownBy(
                        () ->
                                toObject(
                                        configuredSerdeContext(),
                                        malformedJson,
                                        StateMetadata.class))
                .hasMessageContaining(expectedMsg);
    }

    @MethodSource("provideConfigForOneInput")
    @ParameterizedTest
    public void testGetOneInputOperatorDefaultMeta(
            Consumer<TableConfig> configModifier,
            String expectedStateName,
            long expectedTtlMillis) {
        configModifier.accept(tableConfig);
        List<StateMetadata> stateMetadataList =
                StateMetadata.getOneInputOperatorDefaultMeta(tableConfig, expectedStateName);
        assertThat(stateMetadataList).hasSize(1);
        assertThat(stateMetadataList.get(0))
                .isEqualTo(
                        new StateMetadata(
                                0, Duration.ofMillis(expectedTtlMillis), expectedStateName));
    }

    @MethodSource("provideConfigForMultiInput")
    @ParameterizedTest
    public void testGetMultiInputOperatorDefaultMeta(
            Consumer<TableConfig> configModifier,
            List<String> expectedStateNameList,
            List<Long> expectedTtlMillisList) {
        configModifier.accept(tableConfig);
        List<StateMetadata> stateMetadataList =
                StateMetadata.getMultiInputOperatorDefaultMeta(
                        tableConfig, expectedStateNameList.toArray(new String[0]));
        assertThat(stateMetadataList).hasSameSizeAs(expectedStateNameList);
        IntStream.range(0, stateMetadataList.size())
                .forEach(
                        i ->
                                assertThat(stateMetadataList.get(i))
                                        .isEqualTo(
                                                new StateMetadata(
                                                        i,
                                                        Duration.ofMillis(
                                                                expectedTtlMillisList.get(i)),
                                                        expectedStateNameList.get(i))));
    }

    @MethodSource("provideStateMetaForOneInput")
    @ParameterizedTest
    public void testGetStateTtlForOneInputOperator(
            Function<TableConfig, ExecNodeConfig> configModifier,
            @Nullable List<StateMetadata> stateMetadataList,
            long expectedStateTtl) {
        ExecNodeConfig nodeConfig = configModifier.apply(tableConfig);
        assertThat(StateMetadata.getStateTtlForOneInputOperator(nodeConfig, stateMetadataList))
                .isEqualTo(expectedStateTtl);
    }

    @MethodSource("provideStateMetaForMultiInput")
    @ParameterizedTest
    public void testGetStateTtlForMultiInputOperator(
            Function<TableConfig, ExecNodeConfig> configModifier,
            @Nullable List<StateMetadata> stateMetadataList,
            List<Long> expectedStateTtlList) {
        ExecNodeConfig nodeConfig = configModifier.apply(tableConfig);
        assertThat(
                        StateMetadata.getStateTtlForMultiInputOperator(
                                nodeConfig, expectedStateTtlList.size(), stateMetadataList))
                .containsExactlyElementsOf(expectedStateTtlList);
    }

    @MethodSource("provideMalformedStateMeta")
    @ParameterizedTest
    public void testGetStateTtlFromInvalidStateMeta(
            int expectedInputNumOfOperator,
            List<StateMetadata> malformedStateMetadataList,
            String expectedMessage) {
        assertThatThrownBy(
                        () ->
                                StateMetadata.getStateTtlForMultiInputOperator(
                                        ExecNodeConfig.ofTableConfig(tableConfig, true),
                                        expectedInputNumOfOperator,
                                        malformedStateMetadataList))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expectedMessage);
    }

    public static Stream<Arguments> provideConfigForOneInput() {
        return Stream.of(
                Arguments.of((Consumer<TableConfig>) config -> {}, "fooState", 0L),
                Arguments.of(
                        (Consumer<TableConfig>)
                                config -> config.set(IDLE_STATE_RETENTION, Duration.ofMinutes(10)),
                        "barState",
                        600000L));
    }

    public static Stream<Arguments> provideConfigForMultiInput() {
        return Stream.of(
                Arguments.of(
                        (Consumer<TableConfig>) config -> {},
                        Arrays.asList("fooState", "barState"),
                        Stream.generate(() -> 0L).limit(2).collect(Collectors.toList())),
                Arguments.of(
                        (Consumer<TableConfig>)
                                config -> config.set(IDLE_STATE_RETENTION, Duration.ofDays(1)),
                        Arrays.asList("firstState", "secondState", "thirdState", "fourthState"),
                        Stream.generate(() -> 86400000L).limit(4).collect(Collectors.toList())));
    }

    public static Stream<Arguments> provideStateMetaForOneInput() {
        return Stream.of(
                Arguments.of(
                        (Function<TableConfig, ExecNodeConfig>)
                                (config) -> ExecNodeConfig.ofTableConfig(config, true),
                        null,
                        0L),
                Arguments.of(
                        (Function<TableConfig, ExecNodeConfig>)
                                (config) -> {
                                    config.set(IDLE_STATE_RETENTION, Duration.ofDays(1));
                                    return ExecNodeConfig.ofTableConfig(config, true);
                                },
                        Collections.emptyList(),
                        86400000L),
                Arguments.of(
                        (Function<TableConfig, ExecNodeConfig>)
                                (config) -> ExecNodeConfig.ofTableConfig(config, true),
                        Collections.singletonList(
                                new StateMetadata(0, Duration.ofMillis(3600000L), "fooState")),
                        3600000L),
                Arguments.of(
                        (Function<TableConfig, ExecNodeConfig>)
                                (config) -> {
                                    config.set(IDLE_STATE_RETENTION, Duration.ofDays(1));
                                    return ExecNodeConfig.ofTableConfig(config, true);
                                },
                        Collections.singletonList(
                                new StateMetadata(0, Duration.ofMillis(172800000L), "barState")),
                        172800000L));
    }

    public static Stream<Arguments> provideStateMetaForMultiInput() {
        return Stream.of(
                Arguments.of(
                        (Function<TableConfig, ExecNodeConfig>)
                                (config) -> ExecNodeConfig.ofTableConfig(config, true),
                        null,
                        Stream.generate(() -> 0L).limit(2).collect(Collectors.toList())),
                Arguments.of(
                        (Function<TableConfig, ExecNodeConfig>)
                                (config) -> {
                                    config.set(IDLE_STATE_RETENTION, Duration.ofDays(1));
                                    return ExecNodeConfig.ofTableConfig(config, true);
                                },
                        Collections.emptyList(),
                        Stream.generate(() -> 86400000L).limit(3).collect(Collectors.toList())),
                Arguments.of(
                        (Function<TableConfig, ExecNodeConfig>)
                                (config) -> ExecNodeConfig.ofTableConfig(config, true),
                        Arrays.asList(
                                new StateMetadata(1, Duration.ofMillis(86400000L), "fooState"),
                                new StateMetadata(0, Duration.ofMillis(3600000L), "barState")),
                        Arrays.asList(3600000L, 86400000L)),
                Arguments.of(
                        (Function<TableConfig, ExecNodeConfig>)
                                (config) -> {
                                    config.set(IDLE_STATE_RETENTION, Duration.ofMinutes(30));
                                    return ExecNodeConfig.ofTableConfig(config, true);
                                },
                        Arrays.asList(
                                new StateMetadata(1, Duration.ofMillis(86400000L), "fooState"),
                                new StateMetadata(0, Duration.ofMillis(3600000L), "barState"),
                                new StateMetadata(2, Duration.ofMillis(3600000L), "meowState")),
                        Arrays.asList(3600000L, 86400000L, 3600000L)));
    }

    public static Stream<Arguments> provideMalformedStateMeta() {
        return Stream.of(
                Arguments.of(
                        1,
                        Arrays.asList(
                                new StateMetadata(1, Duration.ofMillis(60000L), "fooState"),
                                new StateMetadata(3, Duration.ofMillis(3600000L), "barState")),
                        "Received 2 state meta for a OneInputStreamOperator."),
                Arguments.of(
                        2,
                        Collections.singletonList(
                                new StateMetadata(1, Duration.ofMillis(60000L), "fooState")),
                        "Received 1 state meta for a TwoInputStreamOperator."),
                Arguments.of(
                        3,
                        Collections.singletonList(
                                new StateMetadata(0, Duration.ofMillis(60000L), "fooState")),
                        "Received 1 state meta for a MultipleInputStreamOperator."),
                Arguments.of(
                        2,
                        Arrays.asList(
                                new StateMetadata(0, Duration.ofMillis(60000L), "fooState"),
                                new StateMetadata(0, Duration.ofMillis(3600000L), "barState")),
                        "The state index should not contain duplicates and start from 0 (inclusive) and monotonically increase to the "
                                + "input size (exclusive) of the operator."),
                Arguments.of(
                        2,
                        Arrays.asList(
                                new StateMetadata(1, Duration.ofMillis(3600000L), "barState"),
                                new StateMetadata(3, Duration.ofMillis(3600000L), "barState")),
                        "The state index should not contain duplicates and start from 0 (inclusive) and monotonically increase to the "
                                + "input size (exclusive) of the operator."));
    }
}
