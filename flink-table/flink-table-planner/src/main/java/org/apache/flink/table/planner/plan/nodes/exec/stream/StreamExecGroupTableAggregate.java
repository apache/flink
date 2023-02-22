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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedTableAggsHandleFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.GroupTableAggFunction;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.core.AggregateCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

/** Stream {@link ExecNode} for unbounded java/scala group table aggregate. */
public class StreamExecGroupTableAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamExecGroupTableAggregate.class);

    private static final String GROUP_TABLE_AGGREGATE_TRANSFORMATION = "group-table-aggregate";

    private final int[] grouping;
    private final AggregateCall[] aggCalls;
    /** Each element indicates whether the corresponding agg call needs `retract` method. */
    private final boolean[] aggCallNeedRetractions;
    /** Whether this node will generate UPDATE_BEFORE messages. */
    private final boolean generateUpdateBefore;
    /** Whether this node consumes retraction messages. */
    private final boolean needRetraction;

    public StreamExecGroupTableAggregate(
            ReadableConfig tableConfig,
            int[] grouping,
            AggregateCall[] aggCalls,
            boolean[] aggCallNeedRetractions,
            boolean generateUpdateBefore,
            boolean needRetraction,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecGroupTableAggregate.class),
                ExecNodeContext.newPersistedConfig(
                        StreamExecGroupTableAggregate.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        Preconditions.checkArgument(aggCalls.length == aggCallNeedRetractions.length);
        this.grouping = grouping;
        this.aggCalls = aggCalls;
        this.aggCallNeedRetractions = aggCallNeedRetractions;
        this.generateUpdateBefore = generateUpdateBefore;
        this.needRetraction = needRetraction;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {

        if (grouping.length > 0 && config.getStateRetentionTime() < 0) {
            LOG.warn(
                    "No state retention interval configured for a query which accumulates state. "
                            + "Please provide a query configuration with valid retention interval to prevent excessive "
                            + "state size. You may specify a retention time of 0 to not clean up the state.");
        }

        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();

        final AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                                new CodeGeneratorContext(
                                        config, planner.getFlinkContext().getClassLoader()),
                                planner.createRelBuilder(),
                                JavaScalaConversionUtil.toScala(inputRowType.getChildren()),
                                // TODO: heap state backend do not copy key currently,
                                //  we have to copy input field
                                // TODO: copy is not need when state backend is rocksdb,
                                //  improve this in future
                                // TODO: but other operators do not copy this input field.....
                                true)
                        .needAccumulate();

        if (needRetraction) {
            generator.needRetract();
        }

        final AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        planner.getTypeFactory(),
                        inputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        aggCallNeedRetractions,
                        needRetraction,
                        true, // isStateBackendDataViews
                        true); // needDistinctInfo
        final GeneratedTableAggsHandleFunction aggsHandler =
                generator.generateTableAggsHandler("GroupTableAggHandler", aggInfoList);

        final LogicalType[] accTypes =
                Arrays.stream(aggInfoList.getAccTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);
        final int inputCountIndex = aggInfoList.getIndexOfCountStar();
        final GroupTableAggFunction aggFunction =
                new GroupTableAggFunction(
                        aggsHandler,
                        accTypes,
                        inputCountIndex,
                        generateUpdateBefore,
                        config.getStateRetentionTime());
        final OneInputStreamOperator<RowData, RowData> operator =
                new KeyedProcessOperator<>(aggFunction);

        // partitioned aggregation
        final OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(GROUP_TABLE_AGGREGATE_TRANSFORMATION, config),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        false);

        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        grouping,
                        InternalTypeInfo.of(inputRowType));
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());

        return transform;
    }
}
