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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.table.planner.plan.utils.RankUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.AbstractTopNFunction;
import org.apache.flink.table.runtime.operators.rank.AppendOnlyFirstNFunction;
import org.apache.flink.table.runtime.operators.rank.AppendOnlyTopNFunction;
import org.apache.flink.table.runtime.operators.rank.ComparableRecordComparator;
import org.apache.flink.table.runtime.operators.rank.FastTop1Function;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.RetractableTopNFunction;
import org.apache.flink.table.runtime.operators.rank.UpdatableTopNFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RANK_TOPN_CACHE_SIZE;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_STATE_STALE_ERROR_HANDLING;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for Rank. */
@ExecNodeMetadata(
        name = "stream-exec-rank",
        version = 1,
        consumedOptions = {"table.exec.rank.topn-cache-size"},
        producedTransformations = StreamExecRank.RANK_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecRank extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String RANK_TRANSFORMATION = "rank";

    public static final String FIELD_NAME_RANK_TYPE = "rankType";
    public static final String FIELD_NAME_PARTITION_SPEC = "partition";
    public static final String FIELD_NAME_SORT_SPEC = "orderBy";
    public static final String FIELD_NAME_RANK_RANG = "rankRange";
    public static final String FIELD_NAME_RANK_STRATEGY = "rankStrategy";
    public static final String FIELD_NAME_GENERATE_UPDATE_BEFORE = "generateUpdateBefore";
    public static final String FIELD_NAME_OUTPUT_RANK_NUMBER = "outputRowNumber";

    @JsonProperty(FIELD_NAME_RANK_TYPE)
    private final RankType rankType;

    @JsonProperty(FIELD_NAME_PARTITION_SPEC)
    private final PartitionSpec partitionSpec;

    @JsonProperty(FIELD_NAME_SORT_SPEC)
    private final SortSpec sortSpec;

    @JsonProperty(FIELD_NAME_RANK_RANG)
    private final RankRange rankRange;

    @JsonProperty(FIELD_NAME_RANK_STRATEGY)
    private final RankProcessStrategy rankStrategy;

    @JsonProperty(FIELD_NAME_OUTPUT_RANK_NUMBER)
    private final boolean outputRankNumber;

    @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE)
    private final boolean generateUpdateBefore;

    public StreamExecRank(
            ReadableConfig tableConfig,
            RankType rankType,
            PartitionSpec partitionSpec,
            SortSpec sortSpec,
            RankRange rankRange,
            RankProcessStrategy rankStrategy,
            boolean outputRankNumber,
            boolean generateUpdateBefore,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecRank.class),
                ExecNodeContext.newPersistedConfig(StreamExecRank.class, tableConfig),
                rankType,
                partitionSpec,
                sortSpec,
                rankRange,
                rankStrategy,
                outputRankNumber,
                generateUpdateBefore,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecRank(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_RANK_TYPE) RankType rankType,
            @JsonProperty(FIELD_NAME_PARTITION_SPEC) PartitionSpec partitionSpec,
            @JsonProperty(FIELD_NAME_SORT_SPEC) SortSpec sortSpec,
            @JsonProperty(FIELD_NAME_RANK_RANG) RankRange rankRange,
            @JsonProperty(FIELD_NAME_RANK_STRATEGY) RankProcessStrategy rankStrategy,
            @JsonProperty(FIELD_NAME_OUTPUT_RANK_NUMBER) boolean outputRankNumber,
            @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE) boolean generateUpdateBefore,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.rankType = checkNotNull(rankType);
        this.rankRange = checkNotNull(rankRange);
        this.rankStrategy = checkNotNull(rankStrategy);
        this.sortSpec = checkNotNull(sortSpec);
        this.partitionSpec = checkNotNull(partitionSpec);
        this.outputRankNumber = outputRankNumber;
        this.generateUpdateBefore = generateUpdateBefore;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        switch (rankType) {
            case ROW_NUMBER:
                break;
            case RANK:
                throw new TableException("RANK() on streaming table is not supported currently");
            case DENSE_RANK:
                throw new TableException(
                        "DENSE_RANK() on streaming table is not supported currently");
            default:
                throw new TableException(
                        String.format(
                                "Streaming tables do not support %s rank function.", rankType));
        }

        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        RowType inputType = (RowType) inputEdge.getOutputType();
        InternalTypeInfo<RowData> inputRowTypeInfo = InternalTypeInfo.of(inputType);
        int[] sortFields = sortSpec.getFieldIndices();
        RowDataKeySelector sortKeySelector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(), sortFields, inputRowTypeInfo);
        // create a sort spec on sort keys.
        int[] sortKeyPositions = IntStream.range(0, sortFields.length).toArray();
        SortSpec.SortSpecBuilder builder = SortSpec.builder();
        IntStream.range(0, sortFields.length)
                .forEach(
                        idx ->
                                builder.addField(
                                        idx,
                                        sortSpec.getFieldSpec(idx).getIsAscendingOrder(),
                                        sortSpec.getFieldSpec(idx).getNullIsLast()));
        SortSpec sortSpecInSortKey = builder.build();
        GeneratedRecordComparator sortKeyComparator =
                ComparatorCodeGenerator.gen(
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        "StreamExecSortComparator",
                        RowType.of(sortSpec.getFieldTypes(inputType)),
                        sortSpecInSortKey);
        long cacheSize = config.get(TABLE_EXEC_RANK_TOPN_CACHE_SIZE);
        StateTtlConfig ttlConfig = StateConfigUtil.createTtlConfig(config.getStateRetentionTime());

        ExecutionConfigOptions.StateStaleErrorHandling stateStaleErrorHandling =
                config.get(TABLE_EXEC_STATE_STALE_ERROR_HANDLING);
        AbstractTopNFunction processFunction;
        if (rankStrategy instanceof RankProcessStrategy.AppendFastStrategy) {
            if (sortFields.length == 1
                    && TypeCheckUtils.isProcTime(inputType.getChildren().get(sortFields[0]))
                    && sortSpec.getFieldSpec(0).getIsAscendingOrder()) {
                processFunction =
                        new AppendOnlyFirstNFunction(
                                ttlConfig,
                                inputRowTypeInfo,
                                sortKeyComparator,
                                sortKeySelector,
                                rankType,
                                rankRange,
                                generateUpdateBefore,
                                outputRankNumber);
            } else if (RankUtil.isTop1(rankRange)) {
                processFunction =
                        new FastTop1Function(
                                ttlConfig,
                                inputRowTypeInfo,
                                sortKeyComparator,
                                sortKeySelector,
                                rankType,
                                rankRange,
                                generateUpdateBefore,
                                outputRankNumber,
                                cacheSize);
            } else {
                processFunction =
                        new AppendOnlyTopNFunction(
                                ttlConfig,
                                inputRowTypeInfo,
                                sortKeyComparator,
                                sortKeySelector,
                                rankType,
                                rankRange,
                                generateUpdateBefore,
                                outputRankNumber,
                                cacheSize);
            }
        } else if (rankStrategy instanceof RankProcessStrategy.UpdateFastStrategy) {
            if (RankUtil.isTop1(rankRange)) {
                processFunction =
                        new FastTop1Function(
                                ttlConfig,
                                inputRowTypeInfo,
                                sortKeyComparator,
                                sortKeySelector,
                                rankType,
                                rankRange,
                                generateUpdateBefore,
                                outputRankNumber,
                                cacheSize);
            } else {
                RankProcessStrategy.UpdateFastStrategy updateFastStrategy =
                        (RankProcessStrategy.UpdateFastStrategy) rankStrategy;
                int[] primaryKeys = updateFastStrategy.getPrimaryKeys();
                RowDataKeySelector rowKeySelector =
                        KeySelectorUtil.getRowDataSelector(
                                planner.getFlinkContext().getClassLoader(),
                                primaryKeys,
                                inputRowTypeInfo);
                processFunction =
                        new UpdatableTopNFunction(
                                ttlConfig,
                                inputRowTypeInfo,
                                rowKeySelector,
                                sortKeyComparator,
                                sortKeySelector,
                                rankType,
                                rankRange,
                                generateUpdateBefore,
                                outputRankNumber,
                                cacheSize,
                                stateStaleErrorHandling);
            }
            // TODO Use UnaryUpdateTopNFunction after SortedMapState is merged
        } else if (rankStrategy instanceof RankProcessStrategy.RetractStrategy) {
            EqualiserCodeGenerator equaliserCodeGen =
                    new EqualiserCodeGenerator(
                            inputType.getFields().stream()
                                    .map(RowType.RowField::getType)
                                    .toArray(LogicalType[]::new),
                            planner.getFlinkContext().getClassLoader());
            GeneratedRecordEqualiser generatedEqualiser =
                    equaliserCodeGen.generateRecordEqualiser("RankValueEqualiser");
            ComparableRecordComparator comparator =
                    new ComparableRecordComparator(
                            sortKeyComparator,
                            sortKeyPositions,
                            sortSpec.getFieldTypes(inputType),
                            sortSpec.getAscendingOrders(),
                            sortSpec.getNullsIsLast());
            processFunction =
                    new RetractableTopNFunction(
                            ttlConfig,
                            inputRowTypeInfo,
                            comparator,
                            sortKeySelector,
                            rankType,
                            rankRange,
                            generatedEqualiser,
                            generateUpdateBefore,
                            outputRankNumber,
                            stateStaleErrorHandling);
        } else {
            throw new TableException(
                    String.format("rank strategy:%s is not supported.", rankStrategy));
        }

        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(processFunction);
        processFunction.setKeyContext(operator);

        OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(RANK_TRANSFORMATION, config),
                        operator,
                        InternalTypeInfo.of((RowType) getOutputType()),
                        inputTransform.getParallelism());

        // set KeyType and Selector for state
        RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        partitionSpec.getFieldIndices(),
                        inputRowTypeInfo);
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        return transform;
    }
}
