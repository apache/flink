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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.runtimefilter.RuntimeFilterCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.generator.TwoInputOpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.spec.RuntimeFilterFusionCodegenSpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/** Batch {@link ExecNode} for runtime filter. */
public class BatchExecRuntimeFilter extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData> {
    private final int[] probeIndices;

    public BatchExecRuntimeFilter(
            ReadableConfig tableConfig,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description,
            int[] probeIndices) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecRuntimeFilter.class),
                ExecNodeContext.newPersistedConfig(BatchExecRuntimeFilter.class, tableConfig),
                inputProperties,
                outputType,
                description);
        this.probeIndices = probeIndices;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        ExecEdge buildInputEdge = getInputEdges().get(0);
        ExecEdge probeInputEdge = getInputEdges().get(1);

        Transformation<RowData> buildTransform =
                (Transformation<RowData>) buildInputEdge.translateToPlan(planner);
        Transformation<RowData> probeTransform =
                (Transformation<RowData>) probeInputEdge.translateToPlan(planner);

        final CodeGenOperatorFactory<RowData> operatorFactory =
                RuntimeFilterCodeGenerator.gen(
                        new CodeGeneratorContext(
                                config, planner.getFlinkContext().getClassLoader()),
                        (RowType) buildInputEdge.getOutputType(),
                        (RowType) probeInputEdge.getOutputType(),
                        probeIndices);

        return ExecNodeUtil.createTwoInputTransformation(
                buildTransform,
                probeTransform,
                createTransformationName(config),
                createTransformationDescription(config),
                operatorFactory,
                InternalTypeInfo.of(getOutputType()),
                probeTransform.getParallelism(),
                0,
                false);
    }

    @Override
    public boolean supportFusionCodegen() {
        return true;
    }

    @Override
    protected OpFusionCodegenSpecGenerator translateToFusionCodegenSpecInternal(
            PlannerBase planner, ExecNodeConfig config) {
        OpFusionCodegenSpecGenerator leftInput =
                getInputEdges().get(0).translateToFusionCodegenSpec(planner);
        OpFusionCodegenSpecGenerator rightInput =
                getInputEdges().get(1).translateToFusionCodegenSpec(planner);
        OpFusionCodegenSpecGenerator runtimeFilterGenerator =
                new TwoInputOpFusionCodegenSpecGenerator(
                        leftInput,
                        rightInput,
                        0L,
                        (RowType) getOutputType(),
                        new RuntimeFilterFusionCodegenSpec(
                                new CodeGeneratorContext(
                                        config, planner.getFlinkContext().getClassLoader()),
                                probeIndices));
        leftInput.addOutput(1, runtimeFilterGenerator);
        rightInput.addOutput(2, runtimeFilterGenerator);
        return runtimeFilterGenerator;
    }
}
