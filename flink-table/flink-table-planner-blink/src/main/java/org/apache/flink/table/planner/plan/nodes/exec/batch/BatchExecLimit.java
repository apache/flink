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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.runtime.operators.sort.LimitOperator;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Collections;

/** Batch {@link ExecNode} for Limit. */
public class BatchExecLimit extends ExecNodeBase<RowData> implements BatchExecNode<RowData> {

    private final long limitStart;
    private final long limitEnd;
    private final boolean isGlobal;

    public BatchExecLimit(
            long limitStart,
            long limitEnd,
            boolean isGlobal,
            InputProperty inputProperty,
            LogicalType outputType,
            String description) {
        super(Collections.singletonList(inputProperty), outputType, description);
        this.isGlobal = isGlobal;
        this.limitStart = limitStart;
        this.limitEnd = limitEnd;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        Transformation<RowData> inputTransform =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
        LimitOperator operator = new LimitOperator(isGlobal, limitStart, limitEnd);
        return new OneInputTransformation<>(
                inputTransform,
                getDescription(),
                SimpleOperatorFactory.of(operator),
                inputTransform.getOutputType(),
                inputTransform.getParallelism());
    }
}
