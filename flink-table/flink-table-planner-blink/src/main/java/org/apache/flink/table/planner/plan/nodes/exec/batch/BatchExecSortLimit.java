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
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.SortSpec;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.operators.sort.SortLimitOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;

/**
 * {@link BatchExecNode} for Sort with limit.
 *
 * <p>This node will output data rank from `limitStart` to `limitEnd`.
 */
public class BatchExecSortLimit extends ExecNodeBase<RowData> implements BatchExecNode<RowData> {

    private final SortSpec sortSpec;
    private final long limitStart;
    private final long limitEnd;
    private final boolean isGlobal;

    public BatchExecSortLimit(
            SortSpec sortSpec,
            long limitStart,
            long limitEnd,
            boolean isGlobal,
            ExecEdge inputEdge,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputEdge), outputType, description);
        this.sortSpec = sortSpec;
        this.limitStart = limitStart;
        this.limitEnd = limitEnd;
        this.isGlobal = isGlobal;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        if (limitEnd == Long.MAX_VALUE) {
            throw new TableException("Not support limitEnd is max value now!");
        }

        ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
        Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);

        RowType inputType = (RowType) inputNode.getOutputType();
        // generate comparator
        GeneratedRecordComparator genComparator =
                ComparatorCodeGenerator.gen(
                        planner.getTableConfig(),
                        "SortLimitComparator",
                        sortSpec.getFieldIndices(),
                        sortSpec.getFieldTypes(inputType),
                        sortSpec.getAscendingOrders(),
                        sortSpec.getNullsIsLast());

        // TODO If input is ordered, there is no need to use the heap.
        SortLimitOperator operator =
                new SortLimitOperator(isGlobal, limitStart, limitEnd, genComparator);

        OneInputTransformation<RowData, RowData> transform =
                new OneInputTransformation<>(
                        inputTransform,
                        getDesc(),
                        SimpleOperatorFactory.of(operator),
                        InternalTypeInfo.of(inputType),
                        inputTransform.getParallelism());

        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }
        return transform;
    }
}
