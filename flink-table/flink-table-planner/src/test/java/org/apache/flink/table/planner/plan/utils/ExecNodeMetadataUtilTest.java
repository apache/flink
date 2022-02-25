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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

import org.assertj.core.api.Condition;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil.TABLE_CONFIG_OPTIONS;
import static org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil.UNSUPPORTED_JSON_SERDE_CLASSES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ExecNodeMetadataUtil}. */
public class ExecNodeMetadataUtilTest {

    @Test
    public void testNoJsonCreator() {
        assertThatThrownBy(() -> ExecNodeMetadataUtil.addTestNode(DummyNodeNoJsonCreator.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "ExecNode: org.apache.flink.table.planner.plan.utils."
                                + "ExecNodeMetadataUtilTest.DummyNodeNoJsonCreator does not "
                                + "implement @JsonCreator annotation on constructor.");
    }

    @Test
    public void testNoAnnotation() {
        assertThatThrownBy(() -> ExecNodeMetadataUtil.addTestNode(DummyNodeNoAnnotation.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "ExecNode: org.apache.flink.table.planner.plan.utils."
                                + "ExecNodeMetadataUtilTest.DummyNodeNoAnnotation is missing "
                                + "ExecNodeMetadata annotation.");
    }

    @Test
    public void testBothAnnotations() {
        assertThatThrownBy(() -> ExecNodeMetadataUtil.addTestNode(DummyNodeBothAnnotations.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "ExecNode: org.apache.flink.table.planner.plan.utils."
                                + "ExecNodeMetadataUtilTest.DummyNodeBothAnnotations is annotated "
                                + "both with interface org.apache.flink.table.planner.plan.nodes."
                                + "exec.ExecNodeMetadata and interface org.apache.flink.table."
                                + "planner.plan.nodes.exec.MultipleExecNodeMetadata. Please use "
                                + "only interface org.apache.flink.table.planner.plan.nodes.exec."
                                + "MultipleExecNodeMetadata or multiple interface org.apache.flink."
                                + "table.planner.plan.nodes.exec.ExecNodeMetadata");
    }

    @Test
    public void testMultipleAnnotations() {
        // Using MultipleExecNodeMetadata annotation
        ExecNodeMetadataUtil.addTestNode(DummyNode.class);
        assertThat(ExecNodeMetadataUtil.retrieveExecNode("dummy-node", 1))
                .isSameAs(DummyNode.class);
        assertThat(ExecNodeMetadataUtil.retrieveExecNode("dummy-node", 2))
                .isSameAs(DummyNode.class);
        assertThat(ExecNodeMetadataUtil.retrieveExecNode("dummy-node", 3))
                .isSameAs(DummyNode.class);
        assertThat(ExecNodeMetadataUtil.latestAnnotation(DummyNode.class))
                .has(new Condition<>(m -> m.version() == 3, "version"))
                .has(
                        new Condition<>(
                                m -> m.minPlanVersion() == FlinkVersion.v1_15, "minPlanVersion"))
                .has(
                        new Condition<>(
                                m -> m.minPlanVersion() == FlinkVersion.v1_15, "minStateVersion"));

        ReadableConfig config =
                new Configuration()
                        .set(ExecutionConfigOptions.IDLE_STATE_RETENTION, Duration.ofSeconds(60))
                        .set(
                                ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER,
                                ExecutionConfigOptions.NotNullEnforcer.DROP)
                        .set(
                                ExecutionConfigOptions
                                        .TABLE_EXEC_DEDUPLICATE_MINIBATCH_COMPACT_CHANGES_ENABLED,
                                true);

        ReadableConfig persistedConfig =
                ExecNodeContext.newPersistedConfig(DummyNode.class, config);
        assertThat(persistedConfig.get(ExecutionConfigOptions.IDLE_STATE_RETENTION))
                .isEqualTo(Duration.ofSeconds(60));
        assertThat(persistedConfig.get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER))
                .isEqualTo(ExecutionConfigOptions.NotNullEnforcer.DROP);
        assertThat(
                        persistedConfig.get(
                                ExecutionConfigOptions
                                        .TABLE_EXEC_DEDUPLICATE_MINIBATCH_COMPACT_CHANGES_ENABLED))
                .isEqualTo(true);

        // Using multiple individual ExecNodeMetadata annotations
        ExecNodeMetadataUtil.addTestNode(DummyNodeMultipleAnnotations.class);
        assertThat(ExecNodeMetadataUtil.retrieveExecNode("dummy-node-multiple-annotations", 1))
                .isSameAs(DummyNodeMultipleAnnotations.class);
        assertThat(ExecNodeMetadataUtil.retrieveExecNode("dummy-node-multiple-annotations", 2))
                .isSameAs(DummyNodeMultipleAnnotations.class);
        assertThat(ExecNodeMetadataUtil.retrieveExecNode("dummy-node-multiple-annotations", 3))
                .isSameAs(DummyNodeMultipleAnnotations.class);
        assertThat(ExecNodeMetadataUtil.latestAnnotation(DummyNodeMultipleAnnotations.class))
                .has(new Condition<>(m -> m.version() == 3, "version"))
                .has(
                        new Condition<>(
                                m -> m.minPlanVersion() == FlinkVersion.v1_15, "minPlanVersion"))
                .has(
                        new Condition<>(
                                m -> m.minPlanVersion() == FlinkVersion.v1_15, "minStateVersion"));

        assertThatThrownBy(
                        () ->
                                ExecNodeContext.newPersistedConfig(
                                        DummyNodeMultipleAnnotations.class, config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "ExecNode: org.apache.flink.table.planner.plan.utils."
                                + "ExecNodeMetadataUtilTest.DummyNodeMultipleAnnotations, "
                                + "consumedOption: option111 not listed in [TableConfigOptions, "
                                + "ExecutionConfigOptions].");
    }

    @Test
    public void testDuplicateConsumedOptions() {
        ExecNodeMetadataUtil.addTestNode(DummyNodeDuplicateConsumedOptions.class);
        TABLE_CONFIG_OPTIONS.add(
                ConfigOptions.key("option1")
                        .stringType()
                        .defaultValue("1")
                        .withDescription("option1"));
        TABLE_CONFIG_OPTIONS.add(
                ConfigOptions.key("option2")
                        .stringType()
                        .defaultValue("1")
                        .withDescription("option2"));
        TABLE_CONFIG_OPTIONS.add(
                ConfigOptions.key("option3")
                        .stringType()
                        .defaultValue("1")
                        .withDescription("option3"));
        assertThatThrownBy(
                        () ->
                                ExecNodeContext.newPersistedConfig(
                                        DummyNodeDuplicateConsumedOptions.class,
                                        new Configuration()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "ExecNode: org.apache.flink.table.planner.plan.utils."
                                + "ExecNodeMetadataUtilTest."
                                + "DummyNodeDuplicateConsumedOptions, consumedOption: "
                                + "option2 is listed multiple times in consumedOptions, "
                                + "potentially also with fallback/deprecated key.");
    }

    @Test
    public void testDuplicateDeprecatedKeysConsumedOptions() {
        ExecNodeMetadataUtil.addTestNode(DummyNodeDuplicateDeprecatedKeysConsumedOptions.class);
        TABLE_CONFIG_OPTIONS.add(
                ConfigOptions.key("option11")
                        .stringType()
                        .defaultValue("1")
                        .withDeprecatedKeys("option1")
                        .withDescription("option1"));
        TABLE_CONFIG_OPTIONS.add(
                ConfigOptions.key("option2")
                        .stringType()
                        .defaultValue("1")
                        .withDescription("option2"));
        TABLE_CONFIG_OPTIONS.add(
                ConfigOptions.key("option3")
                        .stringType()
                        .defaultValue("1")
                        .withDescription("option3"));
        assertThatThrownBy(
                        () ->
                                ExecNodeContext.newPersistedConfig(
                                        DummyNodeDuplicateDeprecatedKeysConsumedOptions.class,
                                        new Configuration()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "ExecNode: org.apache.flink.table.planner.plan.utils."
                                + "ExecNodeMetadataUtilTest."
                                + "DummyNodeDuplicateDeprecatedKeysConsumedOptions, "
                                + "consumedOption: option11 is listed multiple times in "
                                + "consumedOptions, potentially also with fallback/deprecated "
                                + "key.");
    }

    @Test
    public void testDuplicateFallbackKeysConsumedOptions() {
        ExecNodeMetadataUtil.addTestNode(DummyNodeDuplicateFallbackKeysConsumedOptions.class);
        TABLE_CONFIG_OPTIONS.add(
                ConfigOptions.key("option1")
                        .stringType()
                        .defaultValue("1")
                        .withFallbackKeys("option11")
                        .withDescription("option1"));
        TABLE_CONFIG_OPTIONS.add(
                ConfigOptions.key("option2")
                        .stringType()
                        .defaultValue("1")
                        .withDescription("option2"));
        TABLE_CONFIG_OPTIONS.add(
                ConfigOptions.key("option3")
                        .stringType()
                        .defaultValue("1")
                        .withDescription("option3"));
        assertThatThrownBy(
                        () ->
                                ExecNodeContext.newPersistedConfig(
                                        DummyNodeDuplicateFallbackKeysConsumedOptions.class,
                                        new Configuration()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "ExecNode: org.apache.flink.table.planner.plan.utils."
                                + "ExecNodeMetadataUtilTest."
                                + "DummyNodeDuplicateFallbackKeysConsumedOptions, "
                                + "consumedOption: option11 is listed multiple times in "
                                + "consumedOptions, potentially also with fallback/deprecated "
                                + "key.");
    }

    @Test
    public void testNewContext() {
        assertThatThrownBy(() -> ExecNodeContext.newContext(DummyNodeNoAnnotation.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "ExecNode: org.apache.flink.table.planner.plan.utils."
                                + "ExecNodeMetadataUtilTest.DummyNodeNoAnnotation is not "
                                + "listed in the unsupported classes since it is not annotated "
                                + "with: ExecNodeMetadata.");

        assertThatThrownBy(() -> ExecNodeContext.newContext(DummyNode.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "ExecNode: org.apache.flink.table.planner.plan.utils."
                                + "ExecNodeMetadataUtilTest.DummyNode is not listed in the "
                                + "supported classes and yet is annotated with: ExecNodeMetadata.");
    }

    @Test
    public void testStreamExecNodeJsonSerdeCoverage() {
        Set<Class<? extends ExecNode<?>>> subClasses = ExecNodeMetadataUtil.execNodes();
        List<Class<? extends ExecNode<?>>> classesWithoutJsonCreator = new ArrayList<>();
        List<Class<? extends ExecNode<?>>> classesWithJsonCreatorInUnsupportedList =
                new ArrayList<>();
        for (Class<? extends ExecNode<?>> clazz : subClasses) {
            boolean hasJsonCreator = JsonSerdeUtil.hasJsonCreatorAnnotation(clazz);
            if (hasJsonCreator && UNSUPPORTED_JSON_SERDE_CLASSES.contains(clazz)) {
                classesWithJsonCreatorInUnsupportedList.add(clazz);
            }
            if (!hasJsonCreator && !UNSUPPORTED_JSON_SERDE_CLASSES.contains(clazz)) {
                classesWithoutJsonCreator.add(clazz);
            }
        }

        assertThat(classesWithoutJsonCreator)
                .as(
                        "%s do not support json serialization/deserialization, "
                                + "please refer the implementation of the other StreamExecNodes.",
                        classesWithoutJsonCreator.stream()
                                .map(Class::getSimpleName)
                                .collect(Collectors.joining(",")))
                .isEmpty();
        assertThat(classesWithJsonCreatorInUnsupportedList)
                .as(
                        "%s have support for json serialization/deserialization, "
                                + "but still in UNSUPPORTED_JSON_SERDE_CLASSES list. "
                                + "please move them from UNSUPPORTED_JSON_SERDE_CLASSES.",
                        classesWithJsonCreatorInUnsupportedList.stream()
                                .map(Class::getSimpleName)
                                .collect(Collectors.joining(",")))
                .isEmpty();
    }

    @MultipleExecNodeMetadata({
        @ExecNodeMetadata(
                name = "dummy-node",
                version = 1,
                consumedOptions = {"option1", "option2"},
                minPlanVersion = FlinkVersion.v1_13,
                minStateVersion = FlinkVersion.v1_13),
        @ExecNodeMetadata(
                name = "dummy-node",
                version = 2,
                consumedOptions = {"option11", "option22"},
                minPlanVersion = FlinkVersion.v1_14,
                minStateVersion = FlinkVersion.v1_14),
        @ExecNodeMetadata(
                name = "dummy-node",
                version = 3,
                consumedOptions = {
                    "table.exec.state.ttl",
                    "table.exec.sink.not-null-enforcer",
                    "table.exec.deduplicate.mini-batch.compact-changes.enabled" // deprecated key
                },
                minPlanVersion = FlinkVersion.v1_15,
                minStateVersion = FlinkVersion.v1_15)
    })
    private static class DummyNode extends ExecNodeBase<RowData> {

        @JsonCreator
        protected DummyNode(
                ExecNodeContext context,
                ReadableConfig persistedConfig,
                List<InputProperty> properties,
                LogicalType outputType,
                String description) {
            super(10, context, persistedConfig, properties, outputType, description);
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(
                PlannerBase planner, ExecNodeConfig config) {
            return null;
        }
    }

    @ExecNodeMetadata(
            name = "dummy-node-multiple-annotations",
            version = 1,
            consumedOptions = {"option1", "option2"},
            minPlanVersion = FlinkVersion.v1_13,
            minStateVersion = FlinkVersion.v1_13)
    @ExecNodeMetadata(
            name = "dummy-node-multiple-annotations",
            version = 2,
            consumedOptions = {"option11", "option22"},
            minPlanVersion = FlinkVersion.v1_14,
            minStateVersion = FlinkVersion.v1_14)
    @ExecNodeMetadata(
            name = "dummy-node-multiple-annotations",
            version = 3,
            consumedOptions = {"option111", "option222"},
            minPlanVersion = FlinkVersion.v1_15,
            minStateVersion = FlinkVersion.v1_15)
    private static class DummyNodeMultipleAnnotations extends ExecNodeBase<RowData> {

        @JsonCreator
        protected DummyNodeMultipleAnnotations(
                ExecNodeContext context,
                ReadableConfig persistedConfig,
                List<InputProperty> properties,
                LogicalType outputType,
                String description) {
            super(10, context, persistedConfig, properties, outputType, description);
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(
                PlannerBase planner, ExecNodeConfig config) {
            return null;
        }
    }

    private static class DummyNodeNoJsonCreator extends ExecNodeBase<RowData> {

        protected DummyNodeNoJsonCreator(
                ExecNodeContext context,
                ReadableConfig persistedConfig,
                List<InputProperty> properties,
                LogicalType outputType,
                String description) {
            super(10, context, persistedConfig, properties, outputType, description);
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(
                PlannerBase planner, ExecNodeConfig config) {
            return null;
        }
    }

    private static class DummyNodeNoAnnotation extends ExecNodeBase<RowData>
            implements StreamExecNode<RowData> {

        @JsonCreator
        protected DummyNodeNoAnnotation(
                ExecNodeContext context,
                ReadableConfig persistedConfig,
                List<InputProperty> properties,
                LogicalType outputType,
                String description) {
            super(10, context, persistedConfig, properties, outputType, description);
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(
                PlannerBase planner, ExecNodeConfig config) {
            return null;
        }
    }

    @MultipleExecNodeMetadata({
        @ExecNodeMetadata(
                name = "dummy-node",
                version = 1,
                minPlanVersion = FlinkVersion.v1_14,
                minStateVersion = FlinkVersion.v1_14),
        @ExecNodeMetadata(
                name = "dummy-node",
                version = 2,
                minPlanVersion = FlinkVersion.v1_15,
                minStateVersion = FlinkVersion.v1_15)
    })
    @ExecNodeMetadata(
            name = "dummy-node",
            version = 3,
            minPlanVersion = FlinkVersion.v1_15,
            minStateVersion = FlinkVersion.v1_15)
    private static class DummyNodeBothAnnotations extends ExecNodeBase<RowData> {

        @JsonCreator
        protected DummyNodeBothAnnotations(
                ExecNodeContext context,
                ReadableConfig persistedConfig,
                List<InputProperty> properties,
                LogicalType outputType,
                String description) {
            super(10, context, persistedConfig, properties, outputType, description);
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(
                PlannerBase planner, ExecNodeConfig config) {
            return null;
        }
    }

    @ExecNodeMetadata(
            name = "dummy-node-duplicate-consumedOptions",
            version = 3,
            consumedOptions = {"option1", "option2", "option3", "option2"},
            minPlanVersion = FlinkVersion.v1_15,
            minStateVersion = FlinkVersion.v1_15)
    private static class DummyNodeDuplicateConsumedOptions extends ExecNodeBase<RowData> {

        @JsonCreator
        protected DummyNodeDuplicateConsumedOptions(
                ExecNodeContext context,
                ReadableConfig persistedConfig,
                List<InputProperty> properties,
                LogicalType outputType,
                String description) {
            super(10, context, persistedConfig, properties, outputType, description);
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(
                PlannerBase planner, ExecNodeConfig config) {
            return null;
        }
    }

    @ExecNodeMetadata(
            name = "dummy-node-duplicate-deprecated-keys-consumedOptions",
            version = 3,
            consumedOptions = {"option1", "option2", "option3", "option11"},
            minPlanVersion = FlinkVersion.v1_15,
            minStateVersion = FlinkVersion.v1_15)
    private static class DummyNodeDuplicateDeprecatedKeysConsumedOptions
            extends ExecNodeBase<RowData> {

        @JsonCreator
        protected DummyNodeDuplicateDeprecatedKeysConsumedOptions(
                ExecNodeContext context,
                ReadableConfig persistedConfig,
                List<InputProperty> properties,
                LogicalType outputType,
                String description) {
            super(10, context, persistedConfig, properties, outputType, description);
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(
                PlannerBase planner, ExecNodeConfig config) {
            return null;
        }
    }

    @ExecNodeMetadata(
            name = "dummy-node-duplicate-fallback-keys-consumedOptions",
            version = 3,
            consumedOptions = {"option1", "option2", "option3", "option11"},
            minPlanVersion = FlinkVersion.v1_15,
            minStateVersion = FlinkVersion.v1_15)
    private static class DummyNodeDuplicateFallbackKeysConsumedOptions
            extends ExecNodeBase<RowData> {

        @JsonCreator
        protected DummyNodeDuplicateFallbackKeysConsumedOptions(
                ExecNodeContext context,
                ReadableConfig persistedConfig,
                List<InputProperty> properties,
                LogicalType outputType,
                String description) {
            super(10, context, persistedConfig, properties, outputType, description);
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(
                PlannerBase planner, ExecNodeConfig config) {
            return null;
        }
    }
}
