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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
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
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchGlobalGroupAggFunction;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for unbounded global group aggregate. */
@ExecNodeMetadata(
        name = "stream-exec-global-group-aggregate",
        version = 1,
        consumedOptions = {"table.exec.mini-batch.enabled", "table.exec.mini-batch.size"},
        producedTransformations =
                StreamExecGlobalGroupAggregate.GLOBAL_GROUP_AGGREGATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecGlobalGroupAggregate extends StreamExecAggregateBase {

    private static final Logger LOG = LoggerFactory.getLogger(StreamExecGlobalGroupAggregate.class);

    public static final String GLOBAL_GROUP_AGGREGATE_TRANSFORMATION = "global-group-aggregate";

    public static final String FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE = "localAggInputRowType";
    public static final String FIELD_NAME_INDEX_OF_COUNT_STAR = "indexOfCountStar";

    public static final String STATE_NAME = "globalGroupAggregateState";

    @JsonProperty(FIELD_NAME_GROUPING)
    private final int[] grouping;

    @JsonProperty(FIELD_NAME_AGG_CALLS)
    private final AggregateCall[] aggCalls;

    /** Each element indicates whether the corresponding agg call needs `retract` method. */
    @JsonProperty(FIELD_NAME_AGG_CALL_NEED_RETRACTIONS)
    private final boolean[] aggCallNeedRetractions;

    /** The input row type of this node's local agg. */
    @JsonProperty(FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE)
    private final RowType localAggInputRowType;

    /** Whether this node will generate UPDATE_BEFORE messages. */
    @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE)
    private final boolean generateUpdateBefore;

    /** Whether this node consumes retraction messages. */
    @JsonProperty(FIELD_NAME_NEED_RETRACTION)
    private final boolean needRetraction;

    /** The position for the existing count star. */
    @JsonProperty(FIELD_NAME_INDEX_OF_COUNT_STAR)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    protected final Integer indexOfCountStar;

    @Nullable
    @JsonProperty(FIELD_NAME_STATE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<StateMetadata> stateMetadataList;

    public StreamExecGlobalGroupAggregate(
            ReadableConfig tableConfig,
            int[] grouping,
            AggregateCall[] aggCalls,
            boolean[] aggCallNeedRetractions,
            RowType localAggInputRowType,
            boolean generateUpdateBefore,
            boolean needRetraction,
            @Nullable Integer indexOfCountStar,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecGlobalGroupAggregate.class),
                ExecNodeContext.newPersistedConfig(
                        StreamExecGlobalGroupAggregate.class, tableConfig),
                grouping,
                aggCalls,
                aggCallNeedRetractions,
                localAggInputRowType,
                generateUpdateBefore,
                needRetraction,
                indexOfCountStar,
                StateMetadata.getOneInputOperatorDefaultMeta(tableConfig, STATE_NAME),
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecGlobalGroupAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
            @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
            @JsonProperty(FIELD_NAME_AGG_CALL_NEED_RETRACTIONS) boolean[] aggCallNeedRetractions,
            @JsonProperty(FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE) RowType localAggInputRowType,
            @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE) boolean generateUpdateBefore,
            @JsonProperty(FIELD_NAME_NEED_RETRACTION) boolean needRetraction,
            @JsonProperty(FIELD_NAME_INDEX_OF_COUNT_STAR) @Nullable Integer indexOfCountStar,
            @Nullable @JsonProperty(FIELD_NAME_STATE) List<StateMetadata> stateMetadataList,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.grouping = checkNotNull(grouping);
        this.aggCalls = checkNotNull(aggCalls);
        this.aggCallNeedRetractions = checkNotNull(aggCallNeedRetractions);
        checkArgument(aggCalls.length == aggCallNeedRetractions.length);
        this.localAggInputRowType = checkNotNull(localAggInputRowType);
        this.generateUpdateBefore = generateUpdateBefore;
        this.needRetraction = needRetraction;
        checkArgument(indexOfCountStar == null || indexOfCountStar >= 0 && needRetraction);
        this.indexOfCountStar = indexOfCountStar;
        this.stateMetadataList = stateMetadataList;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {

        long stateRetentionTime =
                StateMetadata.getStateTtlForOneInputOperator(config, stateMetadataList);
        if (grouping.length > 0 && stateRetentionTime < 0) {
            LOG.warn(
                    "No state retention interval configured for a query which accumulates state. "
                            + "Please provide a query configuration with valid retention interval to prevent excessive "
                            + "state size. You may specify a retention time of 0 to not clean up the state.");
        }

        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();

        final AggregateInfoList localAggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        planner.getTypeFactory(),
                        localAggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        aggCallNeedRetractions,
                        needRetraction,
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(indexOfCountStar)),
                        false, // isStateBackendDataViews
                        true); // needDistinctInfo
        final AggregateInfoList globalAggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        planner.getTypeFactory(),
                        localAggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        aggCallNeedRetractions,
                        needRetraction,
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(indexOfCountStar)),
                        true, // isStateBackendDataViews
                        true); // needDistinctInfo

        final GeneratedAggsHandleFunction localAggsHandler =
                generateAggsHandler(
                        "LocalGroupAggsHandler",
                        localAggInfoList,
                        grouping.length,
                        localAggInfoList.getAccTypes(),
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        planner.createRelBuilder());

        final GeneratedAggsHandleFunction globalAggsHandler =
                generateAggsHandler(
                        "GlobalGroupAggsHandler",
                        globalAggInfoList,
                        0, // mergedAccOffset
                        localAggInfoList.getAccTypes(),
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        planner.createRelBuilder());

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
                new EqualiserCodeGenerator(
                                globalAggValueTypes, planner.getFlinkContext().getClassLoader())
                        .generateRecordEqualiser("GroupAggValueEqualiser");

        final OneInputStreamOperator<RowData, RowData> operator;
        final boolean isMiniBatchEnabled =
                config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
        if (isMiniBatchEnabled) {
            MiniBatchGlobalGroupAggFunction aggFunction =
                    new MiniBatchGlobalGroupAggFunction(
                            localAggsHandler,
                            globalAggsHandler,
                            recordEqualiser,
                            globalAccTypes,
                            indexOfCountStar,
                            generateUpdateBefore,
                            stateRetentionTime);

            operator =
                    new KeyedMapBundleOperator<>(
                            aggFunction, AggregateUtil.createMiniBatchTrigger(config));
        } else {
            throw new TableException("Local-Global optimization is only worked in miniBatch mode");
        }

        // partitioned aggregation
        final OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(GLOBAL_GROUP_AGGREGATE_TRANSFORMATION, config),
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

    private GeneratedAggsHandleFunction generateAggsHandler(
            String name,
            AggregateInfoList aggInfoList,
            int mergedAccOffset,
            DataType[] mergedAccExternalTypes,
            ExecNodeConfig config,
            ClassLoader classLoader,
            RelBuilder relBuilder) {

        // For local aggregate, the result will be buffered, so copyInputField is true.
        // For global aggregate, result will be put into state, then not need copy
        // but this global aggregate result will be put into a buffered map first,
        // then multi-put to state, so copyInputField is true.
        AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                        new CodeGeneratorContext(config, classLoader),
                        relBuilder,
                        JavaScalaConversionUtil.toScala(localAggInputRowType.getChildren()),
                        true);

        return generator
                .needMerge(mergedAccOffset, true, mergedAccExternalTypes)
                .generateAggsHandler(name, aggInfoList);
    }
}
