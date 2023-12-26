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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.generator.OneInputOpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.spec.CalcFusionCodegenSpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Batch {@link ExecNode} for Calc. */
public class BatchExecCalc extends CommonExecCalc implements BatchExecNode<RowData> {

    public BatchExecCalc(
            ReadableConfig tableConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecCalc.class),
                ExecNodeContext.newPersistedConfig(BatchExecCalc.class, tableConfig),
                projection,
                condition,
                TableStreamOperator.class,
                false, // retainHeader
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    public boolean supportFusionCodegen() {
        return true;
    }

    @Override
    protected OpFusionCodegenSpecGenerator translateToFusionCodegenSpecInternal(
            PlannerBase planner, ExecNodeConfig config) {
        OpFusionCodegenSpecGenerator input =
                getInputEdges().get(0).translateToFusionCodegenSpec(planner);
        OpFusionCodegenSpecGenerator calcGenerator =
                new OneInputOpFusionCodegenSpecGenerator(
                        input,
                        0L,
                        (RowType) getOutputType(),
                        new CalcFusionCodegenSpec(
                                new CodeGeneratorContext(
                                        config, planner.getFlinkContext().getClassLoader()),
                                JavaScalaConversionUtil.toScala(projection),
                                JavaScalaConversionUtil.toScala(Optional.ofNullable(condition))));
        input.addOutput(1, calcGenerator);
        return calcGenerator;
    }
}
