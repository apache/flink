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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.Collections;
import java.util.Optional;

/** Base class for exec Calc. */
public abstract class CommonExecCalc extends ExecNodeBase<RowData> {
    private final RexProgram calcProgram;
    private final Class<?> operatorBaseClass;
    private final boolean retainHeader;

    public CommonExecCalc(
            RexProgram calcProgram,
            Class<?> operatorBaseClass,
            boolean retainHeader,
            ExecEdge inputEdge,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputEdge), outputType, description);
        this.calcProgram = calcProgram;
        this.operatorBaseClass = operatorBaseClass;
        this.retainHeader = retainHeader;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
        final Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);
        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(planner.getTableConfig())
                        .setOperatorBaseClass(operatorBaseClass);

        final Optional<RexNode> condition;
        if (calcProgram.getCondition() != null) {
            condition = Optional.of(calcProgram.expandLocalRef(calcProgram.getCondition()));
        } else {
            condition = Optional.empty();
        }

        final CodeGenOperatorFactory<RowData> substituteStreamOperator =
                CalcCodeGenerator.generateCalcOperator(
                        ctx,
                        inputTransform,
                        (RowType) getOutputType(),
                        calcProgram,
                        JavaScalaConversionUtil.toScala(condition),
                        retainHeader,
                        getClass().getSimpleName());
        final Transformation<RowData> transformation =
                new OneInputTransformation<>(
                        inputTransform,
                        getDesc(),
                        substituteStreamOperator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism());

        if (inputsContainSingleton()) {
            transformation.setParallelism(1);
            transformation.setMaxParallelism(1);
        }
        return transformation;
    }
}
