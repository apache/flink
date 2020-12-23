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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchGlobalGroupAggFunction;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

/** Stream {@link ExecNode} for unbounded global group aggregate. */
public class StreamExecGlobalGroupAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(StreamExecGlobalGroupAggregate.class);

    private final int[] grouping;
    private final AggregateCall[] aggCalls;
    /** Each element indicates whether the corresponding agg call needs `retract` method. */
    private final boolean[] aggCallNeedRetractions;
    /** The input row type of this node's local agg. */
    private final RowType localAggInputRowType;
    /** Whether this node will generate UPDATE_BEFORE messages. */
    private final boolean generateUpdateBefore;
    /** Whether this node consumes retraction messages. */
    private final boolean needRetraction;

    public StreamExecGlobalGroupAggregate(
            int[] grouping,
            AggregateCall[] aggCalls,
            boolean[] aggCallNeedRetractions,
            RowType localAggInputRowType,
            boolean generateUpdateBefore,
            boolean needRetraction,
            ExecEdge inputEdge,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputEdge), outputType, description);
        this.grouping = grouping;
        this.aggCalls = aggCalls;
        this.aggCallNeedRetractions = aggCallNeedRetractions;
        this.localAggInputRowType = localAggInputRowType;
        this.generateUpdateBefore = generateUpdateBefore;
        this.needRetraction = needRetraction;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final TableConfig tableConfig = planner.getTableConfig();

        if (grouping.length > 0 && tableConfig.getMinIdleStateRetentionTime() < 0) {
            LOG.warn(
                    "No state retention interval configured for a query which accumulates state. "
                            + "Please provide a query configuration with valid retention interval to prevent excessive "
                            + "state size. You may specify a retention time of 0 to not clean up the state.");
        }

        final ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
        final Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputNode.getOutputType();

        final AggregateInfoList localAggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        localAggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        aggCallNeedRetractions,
                        needRetraction,
                        false,
                        true);
        final AggregateInfoList globalAggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        localAggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        aggCallNeedRetractions,
                        needRetraction,
                        true,
                        true);

        final GeneratedAggsHandleFunction localAggsHandler =
                generateAggsHandler(
                        "LocalGroupAggsHandler",
                        localAggInfoList,
                        grouping.length,
                        localAggInfoList.getAccTypes(),
                        tableConfig,
                        planner.getRelBuilder());

        final GeneratedAggsHandleFunction globalAggsHandler =
                generateAggsHandler(
                        "GlobalGroupAggsHandler",
                        globalAggInfoList,
                        0,
                        localAggInfoList.getAccTypes(),
                        tableConfig,
                        planner.getRelBuilder());

        final int indexOfCountStar = globalAggInfoList.getIndexOfCountStar();
        final LogicalType[] globalAccTypes =
                Arrays.stream(globalAggInfoList.getAccTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);
        final LogicalType[] globalAggValueTypes =
                Arrays.stream(globalAggInfoList.getActualValueTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);
        final GeneratedRecordEqualiser recordEqualiser =
                new EqualiserCodeGenerator(globalAggValueTypes)
                        .generateRecordEqualiser("GroupAggValueEqualiser");

        final OneInputStreamOperator<RowData, RowData> operator;
        final boolean isMiniBatchEnabled =
                tableConfig
                        .getConfiguration()
                        .getBoolean(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
        if (isMiniBatchEnabled) {
            MiniBatchGlobalGroupAggFunction aggFunction =
                    new MiniBatchGlobalGroupAggFunction(
                            localAggsHandler,
                            globalAggsHandler,
                            recordEqualiser,
                            globalAccTypes,
                            indexOfCountStar,
                            generateUpdateBefore,
                            tableConfig.getIdleStateRetention().toMillis());

            operator =
                    new KeyedMapBundleOperator<>(
                            aggFunction, AggregateUtil.createMiniBatchTrigger(tableConfig));
        } else {
            throw new TableException("Local-Global optimization is only worked in miniBatch mode");
        }

        // partitioned aggregation
        final OneInputTransformation<RowData, RowData> transform =
                new OneInputTransformation<>(
                        inputTransform,
                        getDesc(),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism());

        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }

        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(grouping, InternalTypeInfo.of(inputRowType));
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());

        return transform;
    }

    private GeneratedAggsHandleFunction generateAggsHandler(
            String name,
            AggregateInfoList aggInfoList,
            int mergedAccOffset,
            DataType[] mergedAccExternalTypes,
            TableConfig config,
            RelBuilder relBuilder) {

        // For local aggregate, the result will be buffered, so copyInputField is true.
        // For global aggregate, result will be put into state, then not need copy
        // but this global aggregate result will be put into a buffered map first,
        // then multi-put to state, so copyInputField is true.
        AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                        new CodeGeneratorContext(config),
                        relBuilder,
                        JavaScalaConversionUtil.toScala(localAggInputRowType.getChildren()),
                        true);

        return generator
                .needMerge(mergedAccOffset, true, mergedAccExternalTypes)
                .generateAggsHandler(name, aggInfoList);
    }
}
