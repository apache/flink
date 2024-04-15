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

package org.apache.flink.table.planner.plan.nodes.exec.batch.runtimefilter;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.typeutils.RowTypeUtils;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.operators.runtimefilter.LocalRuntimeFilterBuilderOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/** Batch {@link ExecNode} for local runtime filter builder. */
public class BatchExecLocalRuntimeFilterBuilder extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData> {
    private final int[] buildIndices;
    private final int estimatedRowCount;
    private final int maxRowCount;

    public BatchExecLocalRuntimeFilterBuilder(
            ReadableConfig tableConfig,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description,
            int[] buildIndices,
            int estimatedRowCount,
            int maxRowCount) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecLocalRuntimeFilterBuilder.class),
                ExecNodeContext.newPersistedConfig(
                        BatchExecLocalRuntimeFilterBuilder.class, tableConfig),
                inputProperties,
                outputType,
                description);
        this.buildIndices = buildIndices;
        this.estimatedRowCount = estimatedRowCount;
        this.maxRowCount = maxRowCount;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        RowType buildType = (RowType) inputEdge.getOutputType();
        GeneratedProjection inputProj =
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(
                                config, planner.getFlinkContext().getClassLoader()),
                        "LocalRuntimeFilterBuilderProjection",
                        buildType,
                        RowTypeUtils.projectRowType(buildType, buildIndices),
                        buildIndices);

        StreamOperatorFactory<RowData> factory =
                SimpleOperatorFactory.of(
                        new LocalRuntimeFilterBuilderOperator(
                                inputProj, estimatedRowCount, maxRowCount));

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationName(config),
                createTransformationDescription(config),
                factory,
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism(),
                0,
                false);
    }
}
