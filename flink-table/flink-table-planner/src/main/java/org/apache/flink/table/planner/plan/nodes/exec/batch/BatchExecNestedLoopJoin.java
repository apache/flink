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
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.NestedLoopJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexNode;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link BatchExecNode} for Nested-loop Join. */
public class BatchExecNestedLoopJoin extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData> {

    private final FlinkJoinType joinType;
    private final RexNode condition;
    private final boolean leftIsBuild;
    private final boolean singleRowJoin;

    public BatchExecNestedLoopJoin(
            FlinkJoinType joinType,
            RexNode condition,
            boolean leftIsBuild,
            boolean singleRowJoin,
            InputProperty leftInputProperty,
            InputProperty rightInputProperty,
            RowType outputType,
            String description) {
        super(Arrays.asList(leftInputProperty, rightInputProperty), outputType, description);
        this.joinType = checkNotNull(joinType);
        this.condition = checkNotNull(condition);
        this.leftIsBuild = leftIsBuild;
        this.singleRowJoin = singleRowJoin;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        ExecEdge leftInputEdge = getInputEdges().get(0);
        ExecEdge rightInputEdge = getInputEdges().get(1);

        Transformation<RowData> leftInputTransform =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        Transformation<RowData> rightInputTransform =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);

        // get input types
        RowType leftType = (RowType) leftInputEdge.getOutputType();
        RowType rightType = (RowType) rightInputEdge.getOutputType();

        TableConfig config = planner.getTableConfig();
        CodeGenOperatorFactory<RowData> operator =
                new NestedLoopJoinCodeGenerator(
                                new CodeGeneratorContext(config),
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
                    config.getConfiguration()
                            .get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY)
                            .getBytes();
        }

        return ExecNodeUtil.createTwoInputTransformation(
                leftInputTransform,
                rightInputTransform,
                getDescription(),
                operator,
                InternalTypeInfo.of(getOutputType()),
                parallelism,
                manageMem);
    }
}
