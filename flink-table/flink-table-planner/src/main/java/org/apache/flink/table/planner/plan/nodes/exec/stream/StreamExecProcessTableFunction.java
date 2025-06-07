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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.HashCodeGenerator;
import org.apache.flink.table.planner.codegen.ProcessTableRunnerGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalProcessTableFunction;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedProcessTableRunner;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.process.ProcessTableOperatorFactory;
import org.apache.flink.table.runtime.operators.process.RuntimeChangelogMode;
import org.apache.flink.table.runtime.operators.process.RuntimeStateInfo;
import org.apache.flink.table.runtime.operators.process.RuntimeTableSemantics;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInferenceUtil.StateInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.codegen.ProcessTableRunnerGenerator.GeneratedRunnerResult;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldCount;

/**
 * {@link StreamExecNode} for {@link ProcessTableFunction}.
 *
 * <p>A process table function (PTF) maps zero, one, or multiple tables to zero, one, or multiple
 * rows. PTFs enable implementing user-defined operators that can be as feature-rich as built-in
 * operations. PTFs have access to Flink's managed state, event-time and timer services, underlying
 * table changelogs, and can take multiple partitioned tables to produce a new table.
 */
@ExecNodeMetadata(
        name = "stream-exec-process-table-function",
        version = 1,
        producedTransformations = StreamExecProcessTableFunction.PROCESS_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_1,
        minStateVersion = FlinkVersion.v2_1)
public class StreamExecProcessTableFunction extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String PROCESS_TRANSFORMATION = "process";

    public static final String FIELD_NAME_UID = "uid";
    public static final String FIELD_NAME_FUNCTION_CALL = "functionCall";
    public static final String FIELD_NAME_INPUT_CHANGELOG_MODES = "inputChangelogModes";
    public static final String FIELD_NAME_OUTPUT_CHANGELOG_MODE = "outputChangelogMode";

    @JsonProperty(FIELD_NAME_UID)
    private final @Nullable String uid;

    @JsonProperty(FIELD_NAME_FUNCTION_CALL)
    private final RexCall invocation;

    @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODES)
    private final List<ChangelogMode> inputChangelogModes;

    @JsonProperty(FIELD_NAME_OUTPUT_CHANGELOG_MODE)
    private final ChangelogMode outputChangelogMode;

    public StreamExecProcessTableFunction(
            ReadableConfig tableConfig,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description,
            @Nullable String uid,
            RexCall invocation,
            List<ChangelogMode> inputChangelogModes,
            ChangelogMode outputChangelogMode) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecProcessTableFunction.class),
                ExecNodeContext.newPersistedConfig(
                        StreamExecProcessTableFunction.class, tableConfig),
                inputProperties,
                outputType,
                description,
                uid,
                invocation,
                inputChangelogModes,
                outputChangelogMode);
    }

    @JsonCreator
    public StreamExecProcessTableFunction(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description,
            @JsonProperty(FIELD_NAME_UID) @Nullable String uid,
            @JsonProperty(FIELD_NAME_FUNCTION_CALL) RexNode invocation,
            @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODES) List<ChangelogMode> inputChangelogModes,
            @JsonProperty(FIELD_NAME_OUTPUT_CHANGELOG_MODE) ChangelogMode outputChangelogMode) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.uid = uid;
        this.invocation = (RexCall) invocation;
        this.inputChangelogModes = inputChangelogModes;
        this.outputChangelogMode = outputChangelogMode;
    }

    public @Nullable String getUid() {
        return uid;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final List<Transformation<RowData>> inputTransforms =
                getInputEdges().stream()
                        .map(e -> (Transformation<RowData>) e.translateToPlan(planner))
                        .collect(Collectors.toList());

        final List<Ord<StaticArgument>> providedInputArgs =
                StreamPhysicalProcessTableFunction.getProvidedInputArgs(invocation);
        final List<RexNode> operands = invocation.getOperands();
        final List<Integer> inputTimeColumns =
                StreamPhysicalProcessTableFunction.toInputTimeColumns(invocation);
        final List<RuntimeTableSemantics> runtimeTableSemantics =
                providedInputArgs.stream()
                        .map(
                                providedInputArg -> {
                                    final RexTableArgCall tableArgCall =
                                            (RexTableArgCall) operands.get(providedInputArg.i);
                                    final StaticArgument tableArg = providedInputArg.e;
                                    return createRuntimeTableSemantics(
                                            tableArg, tableArgCall, inputTimeColumns);
                                })
                        .collect(Collectors.toList());

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader());

        final RexCall udfCall = StreamPhysicalProcessTableFunction.toUdfCall(invocation);
        final GeneratedRunnerResult generated =
                ProcessTableRunnerGenerator.generate(
                        ctx, udfCall, inputTimeColumns, inputChangelogModes, outputChangelogMode);
        final GeneratedProcessTableRunner generatedRunner = generated.runner();
        final LinkedHashMap<String, StateInfo> stateInfos = generated.stateInfos();

        final List<RuntimeStateInfo> runtimeStateInfos =
                stateInfos.entrySet().stream()
                        .map(
                                stateInfo ->
                                        createRuntimeStateInfo(
                                                stateInfo.getKey(), stateInfo.getValue(), config))
                        .collect(Collectors.toList());
        final GeneratedHashFunction[] stateHashCode =
                runtimeStateInfos.stream()
                        .map(RuntimeStateInfo::getDataType)
                        .map(DataType::getLogicalType)
                        .map(
                                t ->
                                        HashCodeGenerator.generateRowHash(
                                                ctx,
                                                t,
                                                "StateHashCode",
                                                IntStream.range(0, getFieldCount(t)).toArray()))
                        .toArray(GeneratedHashFunction[]::new);
        final GeneratedRecordEqualiser[] stateEquals =
                runtimeStateInfos.stream()
                        .map(RuntimeStateInfo::getDataType)
                        .map(DataType::getLogicalType)
                        .map(t -> EqualiserCodeGenerator.generateRowEquals(ctx, t, "StateEquals"))
                        .toArray(GeneratedRecordEqualiser[]::new);

        final RuntimeChangelogMode producedChangelogMode =
                RuntimeChangelogMode.serialize(outputChangelogMode);

        final ProcessTableOperatorFactory operatorFactory =
                new ProcessTableOperatorFactory(
                        runtimeTableSemantics,
                        runtimeStateInfos,
                        generatedRunner,
                        stateHashCode,
                        stateEquals,
                        producedChangelogMode);

        final String effectiveUid =
                uid != null ? uid : createTransformationUid(PROCESS_TRANSFORMATION, config);

        final TransformationMetadata metadata =
                new TransformationMetadata(
                        effectiveUid,
                        createTransformationName(config),
                        createTransformationDescription(config));

        final Transformation<RowData> transform;
        if (runtimeTableSemantics.stream().anyMatch(RuntimeTableSemantics::hasSetSemantics)) {
            transform =
                    createKeyedTransformation(
                            inputTransforms,
                            metadata,
                            operatorFactory,
                            planner,
                            runtimeTableSemantics);
        } else {
            transform = createNonKeyedTransformation(inputTransforms, metadata, operatorFactory);
        }

        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }

        return transform;
    }

    private RuntimeTableSemantics createRuntimeTableSemantics(
            StaticArgument tableArg, RexTableArgCall tableArgCall, List<Integer> inputTimeColumns) {
        final RuntimeChangelogMode consumedChangelogMode =
                RuntimeChangelogMode.serialize(
                        inputChangelogModes.get(tableArgCall.getInputIndex()));
        final DataType dataType;
        if (tableArg.getDataType().isPresent()) {
            dataType = tableArg.getDataType().get();
        } else {
            dataType = DataTypes.of(FlinkTypeFactory.toLogicalRowType(tableArgCall.type));
        }

        final int timeColumn = inputTimeColumns.get(tableArgCall.getInputIndex());

        return new RuntimeTableSemantics(
                tableArg.getName(),
                tableArgCall.getInputIndex(),
                dataType,
                tableArgCall.getPartitionKeys(),
                consumedChangelogMode,
                tableArg.is(StaticArgumentTrait.PASS_COLUMNS_THROUGH),
                tableArg.is(StaticArgumentTrait.TABLE_AS_SET),
                timeColumn);
    }

    private Transformation<RowData> createKeyedTransformation(
            List<Transformation<RowData>> inputTransforms,
            TransformationMetadata metadata,
            ProcessTableOperatorFactory operatorFactory,
            PlannerBase planner,
            List<RuntimeTableSemantics> runtimeTableSemantics) {
        assert runtimeTableSemantics.size() == inputTransforms.size();

        final List<KeySelector<RowData, RowData>> keySelectors =
                runtimeTableSemantics.stream()
                        .map(
                                inputSemantics ->
                                        KeySelectorUtil.getRowDataSelector(
                                                planner.getFlinkContext().getClassLoader(),
                                                inputSemantics.partitionByColumns(),
                                                (InternalTypeInfo<RowData>)
                                                        inputTransforms
                                                                .get(inputSemantics.getInputIndex())
                                                                .getOutputType()))
                        .collect(Collectors.toList());

        final KeyedMultipleInputTransformation<RowData> transform =
                ExecNodeUtil.createKeyedMultiInputTransformation(
                        inputTransforms,
                        keySelectors,
                        ((RowDataKeySelector) keySelectors.get(0)).getProducedType(),
                        metadata,
                        operatorFactory,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransforms.get(0).getParallelism(),
                        false);

        transform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);

        return transform;
    }

    private Transformation<RowData> createNonKeyedTransformation(
            List<Transformation<RowData>> inputTransforms,
            TransformationMetadata metadata,
            ProcessTableOperatorFactory operatorFactory) {
        final Transformation<RowData> inputTransform = inputTransforms.get(0);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                metadata,
                operatorFactory,
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism(),
                false);
    }

    private static RuntimeStateInfo createRuntimeStateInfo(
            String name, StateInfo stateInfo, ExecNodeConfig config) {
        return new RuntimeStateInfo(
                name,
                stateInfo.getDataType(),
                deriveStateTimeToLive(
                        stateInfo.getTimeToLive().orElse(null), config.getStateRetentionTime()));
    }

    private static long deriveStateTimeToLive(
            @Nullable Duration declaration, long globalRetentionTime) {
        // User declaration take precedence. Including a potential 0.
        if (declaration != null) {
            return declaration.toMillis();
        }
        // This prepares the state layout of every PTF. It makes enabling state TTL at a later
        // point in time possible. The past has shown that users often don't consider ever-growing
        // state initially and would like to enable it later - without breaking the savepoint.
        // Setting it to Long.MAX_VALUE is a better default than 0. It comes with overhead which is
        // why a 0 declaration can override this for efficiency.
        if (globalRetentionTime == 0) {
            return Long.MAX_VALUE;
        }
        return globalRetentionTime;
    }
}
