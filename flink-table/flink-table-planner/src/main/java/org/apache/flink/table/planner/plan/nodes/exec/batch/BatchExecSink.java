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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Collections;

/**
 * Batch {@link ExecNode} to to write data into an external sink defined by a {@link
 * DynamicTableSink}.
 */
public class BatchExecSink extends CommonExecSink implements BatchExecNode<Object> {
    public BatchExecSink(
            DynamicTableSinkSpec tableSinkSpec,
            InputProperty inputProperty,
            LogicalType outputType,
            String description) {
        super(
                tableSinkSpec,
                tableSinkSpec.getTableSink().getChangelogMode(ChangelogMode.insertOnly()),
                true, // isBounded
                getNewNodeId(),
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<Object> translateToPlanInternal(PlannerBase planner) {
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
        return createSinkTransformation(
                planner.getExecEnv(), planner.getTableConfig(), inputTransform, -1, false);
    }
}
