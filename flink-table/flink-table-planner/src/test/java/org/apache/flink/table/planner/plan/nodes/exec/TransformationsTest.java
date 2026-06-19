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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.WithBoundedness;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.CompiledPlanUtils;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.table.planner.factories.TableFactoryHarness.ScanSourceBase;
import org.apache.flink.table.planner.lineage.TableSourceLineageVertex;
import org.apache.flink.table.planner.utils.JsonTestUtils;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_UID_FORMAT;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.UidGeneration.ALWAYS;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.UidGeneration.DISABLED;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.UidGeneration.PLAN_ONLY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

/**
 * Various tests to check {@link Transformation}s that have been generated from {@link ExecNode}s.
 */
@Execution(ExecutionMode.CONCURRENT)
class TransformationsTest {

    @Test
    void testLegacyBatchSource() {
        final StreamTableEnvironment env =
                StreamTableEnvironment.create(
                        StreamExecutionEnvironment.getExecutionEnvironment(),
                        EnvironmentSettings.newInstance().inBatchMode().build());

        final Table table =
                env.from(
                        TableDescriptor.forConnector("values")
                                .option("bounded", "true")
                                .schema(dummySchema())
                                .build());

        final LegacySourceTransformation<?> sourceTransform =
                toLegacySourceTransformation(env, table);

        assertBoundedness(Boundedness.BOUNDED, sourceTransform);
        assertThat(sourceTransform.getOperator().emitsProgressiveWatermarks()).isFalse();

        assertThat(sourceTransform.getLineageVertex()).isNotNull();
        assertThat(((TableSourceLineageVertex) sourceTransform.getLineageVertex()).boundedness())
                .isEqualTo(Boundedness.BOUNDED);

        List<LineageDataset> datasets = sourceTransform.getLineageVertex().datasets();
        assertThat(datasets.size()).isEqualTo(1);
        assertThat(datasets.get(0).name()).contains("*anonymous_values$");
        assertThat(datasets.get(0).namespace()).isEqualTo("values://FromElementsFunction");
    }

    @Test
    void testLegacyStreamSource() {
        final StreamTableEnvironment env =
                StreamTableEnvironment.create(
                        StreamExecutionEnvironment.getExecutionEnvironment(),
                        EnvironmentSettings.newInstance().inStreamingMode().build());

        final Table table =
                env.from(
                        TableDescriptor.forConnector("values")
                                .option("bounded", "false")
                                .schema(dummySchema())
                                .build());

        final LegacySourceTransformation<?> sourceTransform =
                toLegacySourceTransformation(env, table);

        assertBoundedness(Boundedness.CONTINUOUS_UNBOUNDED, sourceTransform);
        assertThat(sourceTransform.getOperator().emitsProgressiveWatermarks()).isTrue();

        assertThat(sourceTransform.getLineageVertex()).isNotNull();
        assertThat(((TableSourceLineageVertex) sourceTransform.getLineageVertex()).boundedness())
                .isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);

        List<LineageDataset> datasets = sourceTransform.getLineageVertex().datasets();
        assertThat(datasets.size()).isEqualTo(1);
        assertThat(datasets.get(0).name()).contains("*anonymous_values$");
        assertThat(datasets.get(0).namespace()).isEqualTo("values://FromElementsFunction");
    }

    @Test
    void testLegacyBatchValues() {
        final StreamTableEnvironment env =
                StreamTableEnvironment.create(
                        StreamExecutionEnvironment.getExecutionEnvironment(),
                        EnvironmentSettings.newInstance().inBatchMode().build());

        final Table table = env.fromValues(1, 2, 3);

        final LegacySourceTransformation<?> sourceTransform =
                toLegacySourceTransformation(env, table);

        assertBoundedness(Boundedness.BOUNDED, sourceTransform);
    }

    @Test
    void testUidGeneration() {
        testUidGeneration(c -> c.set(TABLE_EXEC_UID_GENERATION, PLAN_ONLY), true, false);
        testUidGeneration(c -> c.set(TABLE_EXEC_UID_GENERATION, ALWAYS), true, true);
        testUidGeneration(c -> c.set(TABLE_EXEC_UID_GENERATION, DISABLED), false, false);
    }

    @Test
    void testUidDefaults() throws IOException {
        assertTransformations(
                config -> {},
                json -> {},
                env -> planFromCurrentFlinkValues(env).asJsonString(),
                "\\d+_sink",
                "\\d+_constraint-validator",
                "\\d+_values");
    }

    @Test
    void testUidFlink1_15() throws IOException {
        assertTransformations(
                config ->
                        config.set(TABLE_EXEC_UID_FORMAT, "<id>_<type>_<version>_<transformation>"),
                json -> {},
                env -> planFromFlink1_15Values(env).asJsonString(),
                "\\d+_stream-exec-sink_1_sink",
                "\\d+_stream-exec-sink_1_constraint-validator",
                "\\d+_stream-exec-values_1_values");
    }

    @Test
    void testUidFlink1_18() throws IOException {
        assertTransformations(
                config ->
                        config.set(TABLE_EXEC_UID_FORMAT, "<id>_<type>_<version>_<transformation>"),
                json -> {},
                env -> planFromCurrentFlinkValues(env).asJsonString(),
                "\\d+_stream-exec-sink_2_sink",
                "\\d+_stream-exec-sink_2_constraint-validator",
                "\\d+_stream-exec-values_1_values");
    }

    @Test
    void testPerNodeCustomUid() throws IOException {
        assertTransformations(
                config -> {},
                json ->
                        JsonTestUtils.setExecNodeConfig(
                                json,
                                "stream-exec-sink_2",
                                TABLE_EXEC_UID_FORMAT.key(),
                                "my_custom_<transformation>_<id>"),
                env -> planFromCurrentFlinkValues(env).asJsonString(),
                "my_custom_sink_\\d+",
                "my_custom_constraint-validator_\\d+",
                "\\d+_values");
    }

    @Test
    void testMultiTransformSources() throws IOException {
        assertTransformations(
                config -> {},
                json -> {},
                env -> planFromCurrentFlinkMultiTransformSource(env).asJsonString(),
                TransformationSummary.of(
                        "\\d+_sink",
                        ".*anonymous_blackhole.*",
                        ".*Sink\\(table=\\[.*anonymous_blackhole.*\\], fields=\\[i\\]\\)"),
                // In StreamTableScan v2, the uid, name, and description of the intermediate
                // transformation are implementation-specific.
                TransformationSummary.of("\\d+_my-transform", "MyTransform", "MyDescription"),
                // In StreamTableScan v2, the uid, name, and description of the leaf
                // transformation are implementation-specific.
                TransformationSummary.of(
                        "\\d+_my-source",
                        "T\\[\\d+\\]",
                        "\\[\\d+\\]\\:TableSourceScan\\(table=\\[\\[default_catalog, default_database, T\\]\\], fields=\\[i\\]\\)"));
    }

    @Test
    void testMultiTransformSourcesV1() throws IOException {
        assertTransformations(
                config -> {},
                json -> {},
                env -> planFromFlink2_2MultiTransformSource(env).asJsonString(),
                TransformationSummary.of(
                        "\\d+_sink",
                        ".*anonymous_blackhole.*",
                        ".*Sink\\(table=\\[.*anonymous_blackhole.*\\], fields=\\[i\\]\\)"),
                // In StreamTableScan v1, the uid, name, and description of the intermediate
                // transformation are set by the planner.
                TransformationSummary.of(
                        "\\d+_source",
                        "T\\[\\d+\\]",
                        "\\[\\d+\\]\\:TableSourceScan\\(table=\\[\\[default_catalog, default_database, T\\]\\], fields=\\[i\\]\\)"),
                // In StreamTableScan v1, the uid, name, and description of the leaf
                // transformation are implementation-specific.
                TransformationSummary.of(
                        "\\d+_my-source",
                        "T\\[\\d+\\]",
                        "\\[\\d+\\]\\:TableSourceScan\\(table=\\[\\[default_catalog, default_database, T\\]\\], fields=\\[i\\]\\)"));
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private static class TransformationSummary {
        public final String uid;
        public final String name;
        public final String description;

        static TransformationSummary of(String uid, String name, String description) {
            return new TransformationSummary(uid, name, description);
        }

        private TransformationSummary(String uid, String name, String description) {
            this.uid = uid;
            this.name = name;
            this.description = description;
        }
    }

    private static void testUidGeneration(
            Consumer<TableConfig> config,
            boolean expectUidWithCompilation,
            boolean expectUidWithoutCompilation) {
        final StreamTableEnvironment env =
                StreamTableEnvironment.create(
                        StreamExecutionEnvironment.getExecutionEnvironment(),
                        EnvironmentSettings.inStreamingMode());
        config.accept(env.getConfig());

        env.createTemporaryTable(
                "source_table",
                TableDescriptor.forConnector("values")
                        .option("bounded", "true")
                        .schema(dummySchema())
                        .build());
        env.createTemporaryTable(
                "sink_table", TableDescriptor.forConnector("values").schema(dummySchema()).build());

        // There should be 3 transformations: sink -> calc -> source
        final Table table = env.from("source_table").select($("i").abs());

        // Uses in-memory ExecNodes
        final CompiledPlan memoryPlan = table.insertInto("sink_table").compilePlan();
        final List<String> memoryUids =
                CompiledPlanUtils.toTransformations(env, memoryPlan)
                        .get(0)
                        .getTransitivePredecessors()
                        .stream()
                        .map(Transformation::getUid)
                        .collect(Collectors.toList());
        assertThat(memoryUids).hasSize(3);
        if (expectUidWithCompilation) {
            assertThat(memoryUids).allSatisfy(u -> assertThat(u).isNotNull());
        } else {
            assertThat(memoryUids).allSatisfy(u -> assertThat(u).isNull());
        }

        // Uses deserialized ExecNodes
        final String jsonPlan = table.insertInto("sink_table").compilePlan().asJsonString();
        final List<String> jsonUids =
                CompiledPlanUtils.toTransformations(
                                env, env.loadPlan(PlanReference.fromJsonString(jsonPlan)))
                        .get(0)
                        .getTransitivePredecessors()
                        .stream()
                        .map(Transformation::getUid)
                        .collect(Collectors.toList());
        assertThat(jsonUids).hasSize(3);
        if (expectUidWithCompilation) {
            assertThat(jsonUids).allSatisfy(u -> assertThat(u).isNotNull());
        } else {
            assertThat(jsonUids).allSatisfy(u -> assertThat(u).isNull());
        }

        final List<String> inlineUids =
                env
                        .toChangelogStream(table)
                        .getTransformation()
                        .getTransitivePredecessors()
                        .stream()
                        .map(Transformation::getUid)
                        .collect(Collectors.toList());
        assertThat(inlineUids).hasSize(3);
        if (expectUidWithoutCompilation) {
            assertThat(inlineUids).allSatisfy(u -> assertThat(u).isNotNull());
        } else {
            assertThat(inlineUids).allSatisfy(u -> assertThat(u).isNull());
        }
    }

    private static void assertTransformations(
            Consumer<TableConfig> configModifier,
            Consumer<JsonNode> jsonModifier,
            Function<TableEnvironment, String> planGenerator,
            String... expectedUids)
            throws IOException {
        assertTransformations(
                configModifier,
                jsonModifier,
                planGenerator,
                Arrays.stream(expectedUids)
                        .map(uid -> TransformationSummary.of(uid, null, null))
                        .toArray(TransformationSummary[]::new));
    }

    private static void assertTransformations(
            Consumer<TableConfig> configModifier,
            Consumer<JsonNode> jsonModifier,
            Function<TableEnvironment, String> planGenerator,
            TransformationSummary... expectedTransformations)
            throws IOException {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        configModifier.accept(env.getConfig());
        final JsonNode json = JsonTestUtils.readFromString(planGenerator.apply(env));
        jsonModifier.accept(json);
        final List<TransformationSummary> actualTransformations =
                CompiledPlanUtils.toTransformations(
                                env, env.loadPlan(PlanReference.fromJsonString(json.toString())))
                        .get(0)
                        .getTransitivePredecessors()
                        .stream()
                        .map(
                                t ->
                                        TransformationSummary.of(
                                                t.getUid(), t.getName(), t.getDescription()))
                        .collect(Collectors.toList());
        assertThat(actualTransformations).hasSize(expectedTransformations.length);
        IntStream.range(0, expectedTransformations.length)
                .forEach(
                        i -> {
                            final TransformationSummary expected = expectedTransformations[i];
                            final TransformationSummary actual = actualTransformations.get(i);
                            if (expected.uid != null) {
                                assertThat(actual.uid).as("uid").matches(expected.uid);
                            }
                            if (expected.name != null) {
                                assertThat(actual.name).as("name").matches(expected.name);
                            }
                            if (expected.description != null) {
                                assertThat(actual.description)
                                        .as("description")
                                        .matches(expected.description);
                            }
                        });
    }

    private static CompiledPlan planFromCurrentFlinkValues(TableEnvironment env) {
        return env.fromValues(1, 2, 3)
                .insertInto(TableDescriptor.forConnector("blackhole").build())
                .compilePlan();
    }

    private static CompiledPlan planFromFlink1_15Values(TableEnvironment env) {
        // plan content is compiled using release-1.15 with exec node version 1
        return env.loadPlan(PlanReference.fromResource("/jsonplan/testUidFlink1_15.out"));
    }

    private static CompiledPlan planFromFlink2_2MultiTransformSource(TableEnvironment env) {
        createMultiTransformSource(env, "stream-exec-table-source-scan_1");
        // plan content is compiled from
        // planFromCurrentFlinkMultiTransformSource() using Flink release-2.2
        return env.loadPlan(
                PlanReference.fromResource("/jsonplan/testMultiTransformSourceUidsFlink2_2.out"));
    }

    private static CompiledPlan planFromCurrentFlinkMultiTransformSource(TableEnvironment env) {
        createMultiTransformSource(env, "stream-exec-table-source-scan_2");
        return env.from("T")
                .insertInto(TableDescriptor.forConnector("blackhole").build())
                .compilePlan();
    }

    private static void createMultiTransformSource(
            TableEnvironment env, String expectedSourceExecNode) {
        final DataStreamScanProvider scanProvider =
                new DataStreamScanProvider() {
                    @Override
                    public boolean isBounded() {
                        return false;
                    }

                    @Override
                    public DataStream<RowData> produceDataStream(
                            ProviderContext providerContext, StreamExecutionEnvironment execEnv) {

                        assertThat(providerContext.getContainerNodeType())
                                .isEqualTo(expectedSourceExecNode);

                        // UID 1
                        final SingleOutputStreamOperator<Integer> ints = execEnv.fromData(1, 2, 3);
                        providerContext.generateUid("my-source").ifPresent(ints::uid);
                        ints.name(providerContext.getName());
                        ints.setDescription(providerContext.getDescription());

                        // UID 2
                        final SingleOutputStreamOperator<RowData> rows =
                                ints.process(
                                        new ProcessFunction<>() {
                                            @Override
                                            public void processElement(
                                                    Integer value,
                                                    Context ctx,
                                                    Collector<RowData> out) {
                                                throw new IllegalStateException(
                                                        "Should not be called.");
                                            }
                                        });
                        providerContext.generateUid("my-transform").ifPresent(rows::uid);
                        rows.name("MyTransform");
                        rows.setDescription("MyDescription");

                        return rows;
                    }
                };

        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(dummySchema())
                        .source(
                                new ScanSourceBase() {
                                    @Override
                                    public ScanRuntimeProvider getScanRuntimeProvider(
                                            ScanContext runtimeProviderContext) {
                                        return scanProvider;
                                    }
                                })
                        .build();

        env.createTemporaryTable("T", sourceDescriptor);
    }

    private static LegacySourceTransformation<?> toLegacySourceTransformation(
            StreamTableEnvironment env, Table table) {
        Transformation<?> transform = env.toChangelogStream(table).getTransformation();
        while (transform.getInputs().size() == 1) {
            transform = transform.getInputs().get(0);
        }
        assertThat(transform).isInstanceOf(LegacySourceTransformation.class);
        return (LegacySourceTransformation<?>) transform;
    }

    private static void assertBoundedness(Boundedness boundedness, Transformation<?> transform) {
        assertThat(transform)
                .asInstanceOf(type(WithBoundedness.class))
                .extracting(WithBoundedness::getBoundedness)
                .isEqualTo(boundedness);
    }

    private static Schema dummySchema() {
        return Schema.newBuilder().column("i", DataTypes.INT()).build();
    }
}
