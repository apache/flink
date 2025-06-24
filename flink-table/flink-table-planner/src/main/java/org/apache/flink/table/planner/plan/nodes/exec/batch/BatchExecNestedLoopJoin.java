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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.NestedLoopJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link BatchExecNode} for Nested-loop Join. */
@ExecNodeMetadata(
        name = "batch-exec-nested-loop-join",
        version = 1,
        producedTransformations = BatchExecNestedLoopJoin.JOIN_TRANSFORMATION,
        consumedOptions = {"table.exec.resource.external-buffer-memory"},
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class BatchExecNestedLoopJoin extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String JOIN_TRANSFORMATION = "nested-loop-join";
    public static final String FIELD_NAME_JOIN_TYPE = "joinType";
    public static final String FIELD_NAME_LEFT_IS_BUILD = "leftIsBuild";
    public static final String FIELD_NAME_CONDITION = "condition";
    public static final String FIELD_NAME_SINGLE_ROW_JOIN = "singleRowJoin";

    @JsonProperty(FIELD_NAME_JOIN_TYPE)
    private final FlinkJoinType joinType;

    @JsonProperty(FIELD_NAME_CONDITION)
    private final RexNode condition;

    @JsonProperty(FIELD_NAME_LEFT_IS_BUILD)
    private final boolean leftIsBuild;

    @JsonProperty(FIELD_NAME_SINGLE_ROW_JOIN)
    private final boolean singleRowJoin;

    public BatchExecNestedLoopJoin(
            ReadableConfig tableConfig,
            FlinkJoinType joinType,
            RexNode condition,
            boolean leftIsBuild,
            boolean singleRowJoin,
            InputProperty leftInputProperty,
            InputProperty rightInputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecNestedLoopJoin.class),
                ExecNodeContext.newPersistedConfig(BatchExecNestedLoopJoin.class, tableConfig),
                Arrays.asList(leftInputProperty, rightInputProperty),
                outputType,
                description);
        this.joinType = checkNotNull(joinType);
        this.condition = checkNotNull(condition);
        this.leftIsBuild = leftIsBuild;
        this.singleRowJoin = singleRowJoin;
    }

    @JsonCreator
    public BatchExecNestedLoopJoin(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_JOIN_TYPE) FlinkJoinType joinType,
            @JsonProperty(FIELD_NAME_CONDITION) RexNode condition,
            @JsonProperty(FIELD_NAME_LEFT_IS_BUILD) boolean leftIsBuild,
            @JsonProperty(FIELD_NAME_SINGLE_ROW_JOIN) boolean singleRowJoin,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 2);
        this.joinType = checkNotNull(joinType);
        this.condition = checkNotNull(condition);
        this.leftIsBuild = leftIsBuild;
        this.singleRowJoin = singleRowJoin;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        ExecEdge leftInputEdge = getInputEdges().get(0);
        ExecEdge rightInputEdge = getInputEdges().get(1);

        Transformation<RowData> leftInputTransform =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        Transformation<RowData> rightInputTransform =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);

        // get input types
        RowType leftType = (RowType) leftInputEdge.getOutputType();
        RowType rightType = (RowType) rightInputEdge.getOutputType();

        CodeGenOperatorFactory<RowData> operator =
                new NestedLoopJoinCodeGenerator(
                                new CodeGeneratorContext(
                                        config, planner.getFlinkContext().getClassLoader()),
                                singleRowJoin,
                                leftIsBuild,
                                leftType,
                                rightType,
                                (RowType) getOutputType(),
                                joinType,
                                condition)
                        .gen();

        int parallelism = leftInputTransform.getParallelism();
        if (leftIsBuild) {
            parallelism = rightInputTransform.getParallelism();
        }
        long manageMem = 0;
        if (!singleRowJoin) {
            manageMem =
                    config.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY)
                            .getBytes();
        }

        return ExecNodeUtil.createTwoInputTransformation(
                leftInputTransform,
                rightInputTransform,
                createTransformationMeta(JOIN_TRANSFORMATION, config),
                operator,
                InternalTypeInfo.of(getOutputType()),
                parallelism,
                manageMem,
                false);
    }
}
