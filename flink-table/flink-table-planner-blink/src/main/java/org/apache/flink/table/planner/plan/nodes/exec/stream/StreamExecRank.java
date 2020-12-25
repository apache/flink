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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.PartitionSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.SortSpec;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.AbstractTopNFunction;
import org.apache.flink.table.runtime.operators.rank.AppendOnlyTopNFunction;
import org.apache.flink.table.runtime.operators.rank.ComparableRecordComparator;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.RetractableTopNFunction;
import org.apache.flink.table.runtime.operators.rank.UpdatableTopNFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.stream.IntStream;

/** Stream {@link ExecNode} for Rank. */
public class StreamExecRank extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {

    // It is a experimental config, will may be removed later.
    @Experimental
    public static final ConfigOption<Long> TABLE_EXEC_TOPN_CACHE_SIZE =
            ConfigOptions.key("table.exec.topn.cache-size")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            "TopN operator has a cache which caches partial state contents to reduce"
                                    + " state access. Cache size is the number of records in each TopN task.");

    private final RankType rankType;
    private final PartitionSpec partitionSpec;
    private final SortSpec sortSpec;
    private final RankRange rankRange;
    private final RankProcessStrategy rankStrategy;
    private final boolean outputRankNumber;
    private final boolean generateUpdateBefore;

    public StreamExecRank(
            RankType rankType,
            PartitionSpec partitionSpec,
            SortSpec sortSpec,
            RankRange rankRange,
            RankProcessStrategy rankStrategy,
            boolean outputRankNumber,
            boolean generateUpdateBefore,
            ExecEdge inputEdge,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputEdge), outputType, description);
        this.rankType = rankType;
        this.rankRange = rankRange;
        this.rankStrategy = rankStrategy;
        this.sortSpec = sortSpec;
        this.partitionSpec = partitionSpec;
        this.outputRankNumber = outputRankNumber;
        this.generateUpdateBefore = generateUpdateBefore;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
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

        ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
        Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);

        RowType inputType = (RowType) inputNode.getOutputType();
        InternalTypeInfo<RowData> inputRowTypeInfo = InternalTypeInfo.of(inputType);
        int[] sortFields = sortSpec.getFieldIndices();
        RowDataKeySelector sortKeySelector =
                KeySelectorUtil.getRowDataSelector(sortFields, inputRowTypeInfo);
        int[] sortKeyPositions = IntStream.range(0, sortFields.length).toArray();
        TableConfig tableConfig = planner.getTableConfig();
        GeneratedRecordComparator sortKeyComparator =
                ComparatorCodeGenerator.gen(
                        tableConfig,
                        "StreamExecSortComparator",
                        sortKeyPositions,
                        sortSpec.getFieldTypes(inputType),
                        sortSpec.getAscendingOrders(),
                        sortSpec.getNullsIsLast());
        long cacheSize = tableConfig.getConfiguration().getLong(TABLE_EXEC_TOPN_CACHE_SIZE);
        long minIdleStateRetentionTime = tableConfig.getMinIdleStateRetentionTime();
        long maxIdleStateRetentionTime = tableConfig.getMaxIdleStateRetentionTime();

        AbstractTopNFunction processFunction;
        if (rankStrategy instanceof RankProcessStrategy.AppendFastStrategy) {
            processFunction =
                    new AppendOnlyTopNFunction(
                            minIdleStateRetentionTime,
                            maxIdleStateRetentionTime,
                            inputRowTypeInfo,
                            sortKeyComparator,
                            sortKeySelector,
                            rankType,
                            rankRange,
                            generateUpdateBefore,
                            outputRankNumber,
                            cacheSize);
        } else if (rankStrategy instanceof RankProcessStrategy.UpdateFastStrategy) {
            RankProcessStrategy.UpdateFastStrategy updateFastStrategy =
                    (RankProcessStrategy.UpdateFastStrategy) rankStrategy;
            int[] primaryKeys = updateFastStrategy.getPrimaryKeys();
            RowDataKeySelector rowKeySelector =
                    KeySelectorUtil.getRowDataSelector(primaryKeys, inputRowTypeInfo);
            processFunction =
                    new UpdatableTopNFunction(
                            minIdleStateRetentionTime,
                            maxIdleStateRetentionTime,
                            inputRowTypeInfo,
                            rowKeySelector,
                            sortKeyComparator,
                            sortKeySelector,
                            rankType,
                            rankRange,
                            generateUpdateBefore,
                            outputRankNumber,
                            cacheSize);
            // TODO Use UnaryUpdateTopNFunction after SortedMapState is merged
        } else if (rankStrategy instanceof RankProcessStrategy.RetractStrategy) {
            EqualiserCodeGenerator equaliserCodeGen =
                    new EqualiserCodeGenerator(
                            inputType.getFields().stream()
                                    .map(RowType.RowField::getType)
                                    .toArray(LogicalType[]::new));
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
                            minIdleStateRetentionTime,
                            maxIdleStateRetentionTime,
                            inputRowTypeInfo,
                            comparator,
                            sortKeySelector,
                            rankType,
                            rankRange,
                            generatedEqualiser,
                            generateUpdateBefore,
                            outputRankNumber);
        } else {
            throw new TableException(
                    String.format("rank strategy:%s is not supported.", rankStrategy));
        }

        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(processFunction);
        processFunction.setKeyContext(operator);

        OneInputTransformation<RowData, RowData> transform =
                new OneInputTransformation<>(
                        inputTransform,
                        getDesc(),
                        operator,
                        InternalTypeInfo.of((RowType) getOutputType()),
                        inputTransform.getParallelism());

        // set KeyType and Selector for state
        RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        partitionSpec.getFieldIndices(), inputRowTypeInfo);
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());

        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }
        return transform;
    }
}
