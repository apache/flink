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
 *
 */

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
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
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.SortUtil;
import org.apache.flink.table.runtime.operators.sort.RankOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * {@link BatchExecNode} for Rank.
 *
 * <p>This node supports two-stage(local and global) rank to reduce data-shuffling.
 */
@ExecNodeMetadata(
        name = "batch-exec-rank",
        version = 1,
        producedTransformations = BatchExecRank.RANK_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class BatchExecRank extends ExecNodeBase<RowData>
        implements InputSortedExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String RANK_TRANSFORMATION = "rank";

    public static final String FIELD_NAME_PARTITION_FIELDS = "partitionFields";
    public static final String FIELD_NAME_SORT_FIELDS = "sortFields";
    public static final String FIELD_NAME_RANK_START = "rankStart";
    public static final String FIELD_NAME_RANK_END = "rankEnd";
    public static final String FIELD_NAME_OUTPUT_RANK_NUMBER = "outputRowNumber";

    @JsonProperty(FIELD_NAME_PARTITION_FIELDS)
    private final int[] partitionFields;

    @JsonProperty(FIELD_NAME_SORT_FIELDS)
    private final int[] sortFields;

    @JsonProperty(FIELD_NAME_RANK_START)
    private final long rankStart;

    @JsonProperty(FIELD_NAME_RANK_END)
    private final long rankEnd;

    @JsonProperty(FIELD_NAME_OUTPUT_RANK_NUMBER)
    private final boolean outputRankNumber;

    public BatchExecRank(
            ReadableConfig tableConfig,
            int[] partitionFields,
            int[] sortFields,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecRank.class),
                ExecNodeContext.newPersistedConfig(BatchExecRank.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.partitionFields = partitionFields;
        this.sortFields = sortFields;
        this.rankStart = rankStart;
        this.rankEnd = rankEnd;
        this.outputRankNumber = outputRankNumber;
    }

    @JsonCreator
    public BatchExecRank(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_PARTITION_FIELDS) int[] partitionFields,
            @JsonProperty(FIELD_NAME_SORT_FIELDS) int[] sortFields,
            @JsonProperty(FIELD_NAME_RANK_START) long rankStart,
            @JsonProperty(FIELD_NAME_RANK_END) long rankEnd,
            @JsonProperty(FIELD_NAME_OUTPUT_RANK_NUMBER) boolean outputRankNumber,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.partitionFields = partitionFields;
        this.sortFields = sortFields;
        this.rankStart = rankStart;
        this.rankEnd = rankEnd;
        this.outputRankNumber = outputRankNumber;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        RowType inputType = (RowType) inputEdge.getOutputType();

        // operator needn't cache data
        // The collation for the partition-by and order-by fields is inessential here,
        // we only use the comparator to distinguish fields change.
        RankOperator operator =
                new RankOperator(
                        ComparatorCodeGenerator.gen(
                                config,
                                planner.getFlinkContext().getClassLoader(),
                                "PartitionByComparator",
                                inputType,
                                SortUtil.getAscendingSortSpec(partitionFields)),
                        ComparatorCodeGenerator.gen(
                                config,
                                planner.getFlinkContext().getClassLoader(),
                                "OrderByComparator",
                                inputType,
                                SortUtil.getAscendingSortSpec(sortFields)),
                        rankStart,
                        rankEnd,
                        outputRankNumber);

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(RANK_TRANSFORMATION, config),
                SimpleOperatorFactory.of(operator),
                InternalTypeInfo.of((RowType) getOutputType()),
                inputTransform.getParallelism(),
                false);
    }
}
