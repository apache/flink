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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.operators.sort.SortLimitOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecLimit.FIELD_NAME_IS_GLOBAL;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecLimit.FIELD_NAME_LIMIT_END;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecLimit.FIELD_NAME_LIMIT_START;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort.FIELD_NAME_SORT_SPEC;

/**
 * {@link BatchExecNode} for Sort with limit.
 *
 * <p>This node will output data rank from `limitStart` to `limitEnd`.
 */
@ExecNodeMetadata(
        name = "batch-exec-sort-limit",
        version = 1,
        producedTransformations = BatchExecSortLimit.SORT_LIMIT_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class BatchExecSortLimit extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String SORT_LIMIT_TRANSFORMATION = "sort-limit";

    @JsonProperty(FIELD_NAME_SORT_SPEC)
    private final SortSpec sortSpec;

    @JsonProperty(FIELD_NAME_LIMIT_START)
    private final long limitStart;

    @JsonProperty(FIELD_NAME_LIMIT_END)
    private final long limitEnd;

    @JsonProperty(FIELD_NAME_IS_GLOBAL)
    private final boolean isGlobal;

    public BatchExecSortLimit(
            ReadableConfig tableConfig,
            SortSpec sortSpec,
            long limitStart,
            long limitEnd,
            boolean isGlobal,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecSortLimit.class),
                ExecNodeContext.newPersistedConfig(BatchExecSortLimit.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.sortSpec = sortSpec;
        this.limitStart = limitStart;
        this.limitEnd = limitEnd;
        this.isGlobal = isGlobal;
    }

    @JsonCreator
    public BatchExecSortLimit(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_SORT_SPEC) SortSpec sortSpec,
            @JsonProperty(FIELD_NAME_LIMIT_START) long limitStart,
            @JsonProperty(FIELD_NAME_LIMIT_END) long limitEnd,
            @JsonProperty(FIELD_NAME_IS_GLOBAL) boolean isGlobal,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.sortSpec = sortSpec;
        this.limitStart = limitStart;
        this.limitEnd = limitEnd;
        this.isGlobal = isGlobal;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        if (limitEnd == Long.MAX_VALUE) {
            throw new TableException("Not support limitEnd is max value now!");
        }

        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        RowType inputType = (RowType) inputEdge.getOutputType();
        // generate comparator
        GeneratedRecordComparator genComparator =
                ComparatorCodeGenerator.gen(
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        "SortLimitComparator",
                        inputType,
                        sortSpec);

        // TODO If input is ordered, there is no need to use the heap.
        SortLimitOperator operator =
                new SortLimitOperator(isGlobal, limitStart, limitEnd, genComparator);

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(SORT_LIMIT_TRANSFORMATION, config),
                SimpleOperatorFactory.of(operator),
                InternalTypeInfo.of(inputType),
                inputTransform.getParallelism(),
                false);
    }
}
