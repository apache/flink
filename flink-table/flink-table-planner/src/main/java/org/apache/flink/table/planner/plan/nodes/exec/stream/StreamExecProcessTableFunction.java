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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
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
import org.apache.flink.table.runtime.generated.GeneratedProcessTableRunner;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.process.ProcessTableOperatorFactory;
import org.apache.flink.table.runtime.operators.process.RuntimeTableSemantics;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * {@link StreamExecNode} for {@link ProcessTableFunction}.
 *
 * <p>A process table function (PTF) maps zero, one, or multiple tables to zero, one, or multiple
 * rows. PTFs enable implementing user-defined operators that can be as feature-rich as built-in
 * operations. PTFs have access to Flink's managed state, event-time and timer services, underlying
 * table changelogs, and can take multiple ordered and/or partitioned tables to produce a new table.
 */
@ExecNodeMetadata(
        name = "stream-exec-process-table-function",
        version = 1,
        producedTransformations = StreamExecProcessTableFunction.PROCESS_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class StreamExecProcessTableFunction extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String PROCESS_TRANSFORMATION = "process";

    public static final String FIELD_NAME_UID = "uid";
    public static final String FIELD_NAME_FUNCTION_CALL = "functionCall";
    public static final String FIELD_NAME_INPUT_CHANGELOG_MODES = "inputChangelogModes";

    @JsonProperty(FIELD_NAME_UID)
    private final @Nullable String uid;

    @JsonProperty(FIELD_NAME_FUNCTION_CALL)
    private final RexCall invocation;

    @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODES)
    private final List<ChangelogMode> inputChangelogModes;

    public StreamExecProcessTableFunction(
            ReadableConfig tableConfig,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description,
            @Nullable String uid,
            RexCall invocation,
            List<ChangelogMode> inputChangelogModes) {
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
                inputChangelogModes);
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
            @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODES)
                    List<ChangelogMode> inputChangelogModes) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.uid = uid;
        this.invocation = (RexCall) invocation;
        this.inputChangelogModes = inputChangelogModes;
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
        if (inputTransforms.size() != 1) {
            throw new TableException("Process table function only supports exactly one input.");
        }
        final Transformation<RowData> inputTransform = inputTransforms.get(0);

        final List<Ord<StaticArgument>> providedInputArgs =
                StreamPhysicalProcessTableFunction.getProvidedInputArgs(invocation);
        final List<RexNode> operands = invocation.getOperands();
        final List<RuntimeTableSemantics> tableSemantics =
                providedInputArgs.stream()
                        .map(
                                providedInputArg -> {
                                    final RexTableArgCall tableArgCall =
                                            (RexTableArgCall) operands.get(providedInputArg.i);
                                    final StaticArgument tabledArg = providedInputArg.e;
                                    return createTableSemantics(tabledArg, tableArgCall);
                                })
                        .collect(Collectors.toList());

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader());

        final GeneratedProcessTableRunner generatedRunner =
                ProcessTableRunnerGenerator.generate(ctx, invocation, inputChangelogModes);

        final RuntimeTableSemantics singleTableSemantics;
        if (tableSemantics.isEmpty()) {
            // For constant function calls
            singleTableSemantics = null;
        } else {
            singleTableSemantics = tableSemantics.get(0);
        }

        final ProcessTableOperatorFactory operatorFactory =
                new ProcessTableOperatorFactory(singleTableSemantics, generatedRunner);

        final String effectiveUid =
                uid != null ? uid : createTransformationUid(PROCESS_TRANSFORMATION, config);

        final TransformationMetadata metadata =
                new TransformationMetadata(
                        effectiveUid,
                        createTransformationName(config),
                        createTransformationDescription(config));

        final OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        metadata,
                        operatorFactory,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        false);

        // For one input (but non-constant) functions with set semantics
        if (singleTableSemantics != null && singleTableSemantics.hasSetSemantics()) {
            final RowDataKeySelector selector =
                    KeySelectorUtil.getRowDataSelector(
                            planner.getFlinkContext().getClassLoader(),
                            singleTableSemantics.partitionByColumns(),
                            (InternalTypeInfo<RowData>) inputTransform.getOutputType());
            transform.setStateKeySelector(selector);
            transform.setStateKeyType(selector.getProducedType());
        }

        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }

        return transform;
    }

    private RuntimeTableSemantics createTableSemantics(
            StaticArgument tableArg, RexTableArgCall tableArgCall) {
        final RowKind[] kinds =
                inputChangelogModes
                        .get(tableArgCall.getInputIndex())
                        .getContainedKinds()
                        .toArray(RowKind[]::new);
        final byte[] expectedChanges = new byte[kinds.length];
        IntStream.range(0, kinds.length).forEach(i -> expectedChanges[i] = kinds[i].toByteValue());
        final DataType dataType;
        if (tableArg.getDataType().isPresent()) {
            dataType = tableArg.getDataType().get();
        } else {
            dataType = DataTypes.of(FlinkTypeFactory.toLogicalRowType(tableArgCall.type));
        }
        return new RuntimeTableSemantics(
                tableArg.getName(),
                tableArgCall.getInputIndex(),
                dataType,
                tableArgCall.getPartitionKeys(),
                expectedChanges,
                tableArg.is(StaticArgumentTrait.PASS_COLUMNS_THROUGH),
                tableArg.is(StaticArgumentTrait.TABLE_AS_SET));
    }
}
