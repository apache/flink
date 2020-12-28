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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.SortSpec;
import org.apache.flink.table.runtime.operators.sort.SortOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;

/**
 * {@link BatchExecNode} for Sort without limit.
 *
 * <p>This node will output all data rather than `limit` records.
 */
public class BatchExecSort extends ExecNodeBase<RowData> implements BatchExecNode<RowData> {

    private final SortSpec sortSpec;

    public BatchExecSort(
            SortSpec sortSpec, ExecEdge inputEdge, RowType outputType, String description) {
        super(Collections.singletonList(inputEdge), outputType, description);
        this.sortSpec = sortSpec;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
        Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);

        TableConfig config = planner.getTableConfig();
        RowType inputType = (RowType) inputNode.getOutputType();
        SortCodeGenerator codeGen =
                new SortCodeGenerator(
                        config,
                        sortSpec.getFieldIndices(),
                        sortSpec.getFieldTypes(inputType),
                        sortSpec.getAscendingOrders(),
                        sortSpec.getNullsIsLast());

        SortOperator operator =
                new SortOperator(
                        codeGen.generateNormalizedKeyComputer("BatchExecSortComputer"),
                        codeGen.generateRecordComparator("BatchExecSortComparator"));

        long sortMemory =
                MemorySize.parse(
                                config.getConfiguration()
                                        .getString(
                                                ExecutionConfigOptions
                                                        .TABLE_EXEC_RESOURCE_SORT_MEMORY))
                        .getBytes();

        OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        getDesc(),
                        SimpleOperatorFactory.of(operator),
                        InternalTypeInfo.of((RowType) getOutputType()),
                        inputTransform.getParallelism(),
                        sortMemory);

        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }
        return transform;
    }
}
