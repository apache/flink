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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
import org.apache.flink.table.planner.utils.JsonTestUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_LEGACY_TRANSFORMATION_UIDS;
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
    public void testLegacyBatchSource() {
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
    }

    @Test
    public void testLegacyStreamSource() {
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
    }

    @Test
    public void testLegacyBatchValues() {
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
    public void testUidGeneration() {
        checkUids(c -> c.set(TABLE_EXEC_UID_GENERATION, PLAN_ONLY), true, false);
        checkUids(c -> c.set(TABLE_EXEC_UID_GENERATION, ALWAYS), true, true);
        checkUids(c -> c.set(TABLE_EXEC_UID_GENERATION, DISABLED), false, false);
        checkUids(
                c -> {
                    c.set(TABLE_EXEC_UID_GENERATION, PLAN_ONLY);
                    c.set(TABLE_EXEC_LEGACY_TRANSFORMATION_UIDS, true);
                },
                false,
                false);
    }

    private static void checkUids(
            Consumer<TableConfig> config,
            boolean expectUidWithCompilation,
            boolean expectUidWithoutCompilation) {
        final StreamTableEnvironment env =
                StreamTableEnvironment.create(
                        StreamExecutionEnvironment.getExecutionEnvironment(),
                        EnvironmentSettings.newInstance().inStreamingMode().build());
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
                CompiledPlanUtils.toTransformations(env, memoryPlan).get(0)
                        .getTransitivePredecessors().stream()
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
                        .get(0).getTransitivePredecessors().stream()
                        .map(Transformation::getUid)
                        .collect(Collectors.toList());
        assertThat(jsonUids).hasSize(3);
        if (expectUidWithCompilation) {
            assertThat(jsonUids).allSatisfy(u -> assertThat(u).isNotNull());
        } else {
            assertThat(jsonUids).allSatisfy(u -> assertThat(u).isNull());
        }

        final List<String> inlineUids =
                env.toChangelogStream(table).getTransformation().getTransitivePredecessors()
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

    @Test
    public void testUidDefaults() throws IOException {
        checkUidModification(
                config -> {},
                json -> {},
                env -> planFromCurrentFlinkVersion(env).asJsonString(),
                "\\d+_sink",
                "\\d+_constraint-validator",
                "\\d+_values");
    }

    @Test
    public void testUidFlink1_15() throws IOException {
        checkUidModification(
                config ->
                        config.set(TABLE_EXEC_UID_FORMAT, "<id>_<type>_<version>_<transformation>"),
                json -> {},
                env -> planFromFlink1_15(env).asJsonString(),
                "\\d+_stream-exec-sink_1_sink",
                "\\d+_stream-exec-sink_1_constraint-validator",
                "\\d+_stream-exec-values_1_values");
    }

    @Test
    public void testUidFlink1_18() throws IOException {
        checkUidModification(
                config ->
                        config.set(TABLE_EXEC_UID_FORMAT, "<id>_<type>_<version>_<transformation>"),
                json -> {},
                env -> planFromCurrentFlinkVersion(env).asJsonString(),
                "\\d+_stream-exec-sink_1_sink",
                "\\d+_stream-exec-sink_1_constraint-validator",
                "\\d+_stream-exec-values_1_values");
    }

    @Test
    public void testPerNodeCustomUid() throws IOException {
        checkUidModification(
                config -> {},
                json ->
                        JsonTestUtils.setExecNodeConfig(
                                json,
                                "stream-exec-sink_1",
                                TABLE_EXEC_UID_FORMAT.key(),
                                "my_custom_<transformation>_<id>"),
                env -> planFromCurrentFlinkVersion(env).asJsonString(),
                "my_custom_sink_\\d+",
                "my_custom_constraint-validator_\\d+",
                "\\d+_values");
    }

    private static void checkUidModification(
            Consumer<TableConfig> configModifier,
            Consumer<JsonNode> jsonModifier,
            Function<TableEnvironment, String> planGenerator,
            String... expectedUidPatterns)
            throws IOException {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        configModifier.accept(env.getConfig());
        final JsonNode json = JsonTestUtils.readFromString(planGenerator.apply(env));
        jsonModifier.accept(json);
        final List<String> planUids =
                CompiledPlanUtils.toTransformations(
                                env, env.loadPlan(PlanReference.fromJsonString(json.toString())))
                        .get(0).getTransitivePredecessors().stream()
                        .map(Transformation::getUid)
                        .collect(Collectors.toList());
        assertThat(planUids).hasSize(expectedUidPatterns.length);
        IntStream.range(0, expectedUidPatterns.length)
                .forEach(i -> assertThat(planUids.get(i)).matches(expectedUidPatterns[i]));
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private static CompiledPlan planFromCurrentFlinkVersion(TableEnvironment env) {
        return env.fromValues(1, 2, 3)
                .insertInto(TableDescriptor.forConnector("blackhole").build())
                .compilePlan();
    }

    private static CompiledPlan planFromFlink1_15(TableEnvironment env) {
        // plan content is compiled using release-1.15 with exec node version 1
        return env.loadPlan(PlanReference.fromResource("/jsonplan/testUidFlink1_15.out"));
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
