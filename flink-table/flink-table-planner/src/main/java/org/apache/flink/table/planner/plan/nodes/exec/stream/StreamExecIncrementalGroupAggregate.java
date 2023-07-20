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
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchIncrementalGroupAggFunction;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilder;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for unbounded incremental group aggregate. */
@ExecNodeMetadata(
        name = "stream-exec-incremental-group-aggregate",
        version = 1,
        consumedOptions = {"table.exec.mini-batch.enabled", "table.exec.mini-batch.size"},
        producedTransformations =
                StreamExecIncrementalGroupAggregate.INCREMENTAL_GROUP_AGGREGATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecIncrementalGroupAggregate extends StreamExecAggregateBase {

    public static final String INCREMENTAL_GROUP_AGGREGATE_TRANSFORMATION =
            "incremental-group-aggregate";

    public static final String FIELD_NAME_PARTIAL_AGG_GROUPING = "partialAggGrouping";
    public static final String FIELD_NAME_FINAL_AGG_GROUPING = "finalAggGrouping";
    public static final String FIELD_NAME_PARTIAL_ORIGINAL_AGG_CALLS = "partialOriginalAggCalls";
    public static final String FIELD_NAME_PARTIAL_AGG_CALL_NEED_RETRACTIONS =
            "partialAggCallNeedRetractions";
    public static final String FIELD_NAME_PARTIAL_LOCAL_AGG_INPUT_TYPE =
            "partialLocalAggInputRowType";
    public static final String FIELD_NAME_PARTIAL_AGG_NEED_RETRACTION = "partialAggNeedRetraction";

    public static final String STATE_NAME = "incrementalGroupAggregateState";

    /** The partial agg's grouping. */
    @JsonProperty(FIELD_NAME_PARTIAL_AGG_GROUPING)
    private final int[] partialAggGrouping;

    /** The final agg's grouping. */
    @JsonProperty(FIELD_NAME_FINAL_AGG_GROUPING)
    private final int[] finalAggGrouping;

    /** The partial agg's original agg calls. */
    @JsonProperty(FIELD_NAME_PARTIAL_ORIGINAL_AGG_CALLS)
    private final AggregateCall[] partialOriginalAggCalls;

    /** Each element indicates whether the corresponding agg call needs `retract` method. */
    @JsonProperty(FIELD_NAME_PARTIAL_AGG_CALL_NEED_RETRACTIONS)
    private final boolean[] partialAggCallNeedRetractions;

    /** The input row type of this node's partial local agg. */
    @JsonProperty(FIELD_NAME_PARTIAL_LOCAL_AGG_INPUT_TYPE)
    private final RowType partialLocalAggInputType;

    /** Whether this node consumes retraction messages. */
    @JsonProperty(FIELD_NAME_PARTIAL_AGG_NEED_RETRACTION)
    private final boolean partialAggNeedRetraction;

    @Nullable
    @JsonProperty(FIELD_NAME_STATE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<StateMetadata> stateMetadataList;

    public StreamExecIncrementalGroupAggregate(
            ReadableConfig tableConfig,
            int[] partialAggGrouping,
            int[] finalAggGrouping,
            AggregateCall[] partialOriginalAggCalls,
            boolean[] partialAggCallNeedRetractions,
            RowType partialLocalAggInputType,
            boolean partialAggNeedRetraction,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecIncrementalGroupAggregate.class),
                ExecNodeContext.newPersistedConfig(
                        StreamExecIncrementalGroupAggregate.class, tableConfig),
                partialAggGrouping,
                finalAggGrouping,
                partialOriginalAggCalls,
                partialAggCallNeedRetractions,
                partialLocalAggInputType,
                partialAggNeedRetraction,
                StateMetadata.getOneInputOperatorDefaultMeta(tableConfig, STATE_NAME),
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecIncrementalGroupAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_PARTIAL_AGG_GROUPING) int[] partialAggGrouping,
            @JsonProperty(FIELD_NAME_FINAL_AGG_GROUPING) int[] finalAggGrouping,
            @JsonProperty(FIELD_NAME_PARTIAL_ORIGINAL_AGG_CALLS)
                    AggregateCall[] partialOriginalAggCalls,
            @JsonProperty(FIELD_NAME_PARTIAL_AGG_CALL_NEED_RETRACTIONS)
                    boolean[] partialAggCallNeedRetractions,
            @JsonProperty(FIELD_NAME_PARTIAL_LOCAL_AGG_INPUT_TYPE) RowType partialLocalAggInputType,
            @JsonProperty(FIELD_NAME_PARTIAL_AGG_NEED_RETRACTION) boolean partialAggNeedRetraction,
            @Nullable @JsonProperty(FIELD_NAME_STATE) List<StateMetadata> stateMetadataList,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.partialAggGrouping = checkNotNull(partialAggGrouping);
        this.finalAggGrouping = checkNotNull(finalAggGrouping);
        this.partialOriginalAggCalls = checkNotNull(partialOriginalAggCalls);
        this.partialAggCallNeedRetractions = checkNotNull(partialAggCallNeedRetractions);
        checkArgument(partialOriginalAggCalls.length == partialAggCallNeedRetractions.length);
        this.partialLocalAggInputType = checkNotNull(partialLocalAggInputType);
        this.partialAggNeedRetraction = partialAggNeedRetraction;
        this.stateMetadataList = stateMetadataList;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final AggregateInfoList partialLocalAggInfoList =
                AggregateUtil.createPartialAggInfoList(
                        planner.getTypeFactory(),
                        partialLocalAggInputType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(partialOriginalAggCalls)),
                        partialAggCallNeedRetractions,
                        partialAggNeedRetraction,
                        false);

        final GeneratedAggsHandleFunction partialAggsHandler =
                generateAggsHandler(
                        "PartialGroupAggsHandler",
                        partialLocalAggInfoList,
                        partialAggGrouping.length,
                        partialLocalAggInfoList.getAccTypes(),
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        planner.createRelBuilder(),
                        // the partial aggregate accumulators will be buffered, so need copy
                        true);

        final AggregateInfoList incrementalAggInfo =
                AggregateUtil.createIncrementalAggInfoList(
                        planner.getTypeFactory(),
                        partialLocalAggInputType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(partialOriginalAggCalls)),
                        partialAggCallNeedRetractions,
                        partialAggNeedRetraction);

        final GeneratedAggsHandleFunction finalAggsHandler =
                generateAggsHandler(
                        "FinalGroupAggsHandler",
                        incrementalAggInfo,
                        0,
                        partialLocalAggInfoList.getAccTypes(),
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        planner.createRelBuilder(),
                        // the final aggregate accumulators is not buffered
                        false);

        final RowDataKeySelector partialKeySelector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        partialAggGrouping,
                        InternalTypeInfo.of(inputEdge.getOutputType()));
        final RowDataKeySelector finalKeySelector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        finalAggGrouping,
                        partialKeySelector.getProducedType());

        final MiniBatchIncrementalGroupAggFunction aggFunction =
                new MiniBatchIncrementalGroupAggFunction(
                        partialAggsHandler,
                        finalAggsHandler,
                        finalKeySelector,
                        StateMetadata.getStateTtlForOneInputOperator(config, stateMetadataList));

        final OneInputStreamOperator<RowData, RowData> operator =
                new KeyedMapBundleOperator<>(
                        aggFunction, AggregateUtil.createMiniBatchTrigger(config));

        // partitioned aggregation
        final OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(
                                INCREMENTAL_GROUP_AGGREGATE_TRANSFORMATION, config),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        false);

        // set KeyType and Selector for state
        transform.setStateKeySelector(partialKeySelector);
        transform.setStateKeyType(partialKeySelector.getProducedType());
        return transform;
    }

    private GeneratedAggsHandleFunction generateAggsHandler(
            String name,
            AggregateInfoList aggInfoList,
            int mergedAccOffset,
            DataType[] mergedAccExternalTypes,
            ExecNodeConfig config,
            ClassLoader classLoader,
            RelBuilder relBuilder,
            boolean inputFieldCopy) {

        AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                        new CodeGeneratorContext(config, classLoader),
                        relBuilder,
                        JavaScalaConversionUtil.toScala(partialLocalAggInputType.getChildren()),
                        inputFieldCopy);

        return generator
                .needMerge(mergedAccOffset, true, mergedAccExternalTypes)
                .generateAggsHandler(name, aggInfoList);
    }
}
