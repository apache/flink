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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.dynamicfiltering.ExecutionOrderEnforcerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.generator.TwoInputOpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.spec.ExecutionOrderEnforcerFusionCodegenSpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.operators.dynamicfiltering.DynamicFilteringDataCollectorOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/**
 * Batch {@link ExecNode} for ExecutionOrderEnforcer.
 *
 * <p>ExecutionOrderEnforcer has two inputs, one of which is a source, and the other is the
 * dependent upstream. It enforces that the input source is executed after the dependent input is
 * finished. Everything passed from the inputs is forwarded to the output, though typically the
 * dependent input should not send anything.
 *
 * <p>The ExecutionOrderEnforcer should generally be chained with the source. If chaining is
 * explicitly disabled, the enforcer can not work as expected.
 *
 * <p>The operator is used only for dynamic filtering at present.
 */
public class BatchExecExecutionOrderEnforcer extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData> {
    public BatchExecExecutionOrderEnforcer(
            ReadableConfig tableConfig,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecExecutionOrderEnforcer.class),
                ExecNodeContext.newPersistedConfig(
                        BatchExecExecutionOrderEnforcer.class, tableConfig),
                inputProperties,
                outputType,
                description);
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
                        new ExecutionOrderEnforcerFusionCodegenSpec(
                                new CodeGeneratorContext(
                                        config, planner.getFlinkContext().getClassLoader())));
        leftInput.addOutput(1, runtimeFilterGenerator);
        rightInput.addOutput(2, runtimeFilterGenerator);
        return runtimeFilterGenerator;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        Transformation<RowData> dynamicFilteringInputTransform =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
        Transformation<RowData> sourceTransform =
                (Transformation<RowData>) getInputEdges().get(1).translateToPlan(planner);

        // set dynamic filtering data listener id
        BatchExecDynamicFilteringDataCollector dynamicFilteringDataCollector =
                (BatchExecDynamicFilteringDataCollector)
                        ignoreExchange(getInputEdges().get(0).getSource());
        BatchExecTableSourceScan tableSourceScan =
                (BatchExecTableSourceScan) getInputEdges().get(1).getSource();
        ((SourceTransformation<?, ?, ?>) sourceTransform)
                .setCoordinatorListeningID(tableSourceScan.getDynamicFilteringDataListenerID());
        ((DynamicFilteringDataCollectorOperatorFactory)
                        ((OneInputTransformation<?, ?>)
                                        dynamicFilteringDataCollector.translateToPlan(planner))
                                .getOperatorFactory())
                .registerDynamicFilteringDataListenerID(
                        tableSourceScan.getDynamicFilteringDataListenerID());

        final CodeGenOperatorFactory<RowData> operatorFactory =
                ExecutionOrderEnforcerCodeGenerator.gen(
                        new CodeGeneratorContext(
                                config, planner.getFlinkContext().getClassLoader()),
                        (RowType) getInputEdges().get(0).getOutputType(),
                        (RowType) getInputEdges().get(1).getOutputType());

        return ExecNodeUtil.createTwoInputTransformation(
                dynamicFilteringInputTransform,
                sourceTransform,
                createTransformationName(config),
                createTransformationDescription(config),
                operatorFactory,
                InternalTypeInfo.of(getOutputType()),
                sourceTransform.getParallelism(),
                0,
                false);
    }

    private static ExecNode<?> ignoreExchange(ExecNode<?> execNode) {
        if (execNode instanceof BatchExecExchange) {
            return execNode.getInputEdges().get(0).getSource();
        } else {
            return execNode;
        }
    }
}
