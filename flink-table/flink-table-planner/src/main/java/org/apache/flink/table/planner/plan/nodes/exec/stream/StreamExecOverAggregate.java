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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
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
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.over.AbstractRowTimeUnboundedPrecedingOver;
import org.apache.flink.table.runtime.operators.over.NonTimeRangeUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.NonTimeRowsUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.ProcTimeRangeBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.ProcTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.ProcTimeUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRangeUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRowsUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeUnboundedPrecedingOverFunctionV2;
import org.apache.flink.table.runtime.operators.over.TimeAttribute;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isRowtimeAttribute;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for time-based over operator. */
@ExecNodeMetadata(
        name = "stream-exec-over-aggregate",
        version = 1,
        producedTransformations = StreamExecOverAggregate.OVER_AGGREGATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecOverAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamExecOverAggregate.class);

    public static final String OVER_AGGREGATE_TRANSFORMATION = "over-aggregate";

    public static final String FIELD_NAME_OVER_SPEC = "overSpec";

    public static final String FIELD_NAME_UNBOUNDED_OVER_VERSION = "unboundedOverVersion";

    @JsonProperty(FIELD_NAME_UNBOUNDED_OVER_VERSION)
    private final int unboundedOverVersion;

    @JsonProperty(FIELD_NAME_OVER_SPEC)
    private final OverSpec overSpec;

    public StreamExecOverAggregate(
            ReadableConfig tableConfig,
            OverSpec overSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecOverAggregate.class),
                ExecNodeContext.newPersistedConfig(StreamExecOverAggregate.class, tableConfig),
                overSpec,
                Collections.singletonList(inputProperty),
                outputType,
                description,
                tableConfig.get(ExecutionConfigOptions.UNBOUNDED_OVER_VERSION));
    }

    @JsonCreator
    public StreamExecOverAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_OVER_SPEC) OverSpec overSpec,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description,
            @Nullable @JsonProperty(FIELD_NAME_UNBOUNDED_OVER_VERSION)
                    Integer unboundedOverVersion) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.overSpec = checkNotNull(overSpec);

        if (unboundedOverVersion == null) {
            unboundedOverVersion = 1;
        }
        this.unboundedOverVersion = unboundedOverVersion;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        if (overSpec.getGroups().size() > 1) {
            throw new TableException("All aggregates must be computed on the same window.");
        }

        final OverSpec.GroupSpec group = overSpec.getGroups().get(0);
        final int[] orderKeys = group.getSort().getFieldIndices();
        final boolean[] isAscendingOrders = group.getSort().getAscendingOrders();
        if (orderKeys.length != 1 || isAscendingOrders.length != 1) {
            throw new TableException("The window can only be ordered by a single time column.");
        }

        if (!isAscendingOrders[0]) {
            throw new TableException("The window can only be ordered in ASCENDING mode.");
        }

        final int[] partitionKeys = overSpec.getPartition().getFieldIndices();
        if (partitionKeys.length > 0 && config.getStateRetentionTime() < 0) {
            LOG.warn(
                    "No state retention interval configured for a query which accumulates state. "
                            + "Please provide a query configuration with valid retention interval to prevent "
                            + "excessive state size. You may specify a retention time of 0 to not clean up the state.");
        }

        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();

        final int orderKey = orderKeys[0];
        final LogicalType orderKeyType = inputRowType.getFields().get(orderKey).getType();
        // check time field && identify window time attribute
        TimeAttribute timeAttribute;
        if (isRowtimeAttribute(orderKeyType)) {
            timeAttribute = TimeAttribute.ROW_TIME;
        } else if (isProctimeAttribute(orderKeyType)) {
            timeAttribute = TimeAttribute.PROC_TIME;
        } else {
            timeAttribute = TimeAttribute.NON_TIME;
        }

        final List<RexLiteral> constants = overSpec.getConstants();
        final List<String> fieldNames = new ArrayList<>(inputRowType.getFieldNames());
        final List<LogicalType> fieldTypes = new ArrayList<>(inputRowType.getChildren());
        IntStream.range(0, constants.size()).forEach(i -> fieldNames.add("TMP" + i));
        for (int i = 0; i < constants.size(); ++i) {
            fieldNames.add("TMP" + i);
            fieldTypes.add(FlinkTypeFactory.toLogicalType(constants.get(i).getType()));
        }

        final RowType aggInputRowType =
                RowType.of(
                        fieldTypes.toArray(new LogicalType[0]), fieldNames.toArray(new String[0]));

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader());
        final KeyedProcessFunction<RowData, RowData, RowData> overProcessFunction;
        if (group.getLowerBound().isPreceding()
                && group.getLowerBound().isUnbounded()
                && group.getUpperBound().isCurrentRow()) {
            // unbounded OVER window
            overProcessFunction =
                    createUnboundedOverProcessFunction(
                            ctx,
                            group.getAggCalls(),
                            constants,
                            aggInputRowType,
                            inputRowType,
                            group.isRows(),
                            orderKeys,
                            timeAttribute,
                            config,
                            planner.createRelBuilder(),
                            planner.getTypeFactory());
        } else if (group.getLowerBound().isPreceding()
                && !group.getLowerBound().isUnbounded()
                && group.getUpperBound().isCurrentRow()) {
            final Object boundValue =
                    OverAggregateUtil.getBoundary(overSpec, group.getLowerBound());

            if (boundValue instanceof BigDecimal) {
                throw new TableException(
                        "the specific value is decimal which haven not supported yet.");
            }
            // bounded OVER window
            final long precedingOffset = -1 * (long) boundValue + (group.isRows() ? 1 : 0);
            overProcessFunction =
                    createBoundedOverProcessFunction(
                            ctx,
                            group.getAggCalls(),
                            constants,
                            aggInputRowType,
                            inputRowType,
                            group.isRows(),
                            orderKeys,
                            timeAttribute,
                            precedingOffset,
                            config,
                            planner.createRelBuilder(),
                            planner.getTypeFactory());
        } else {
            throw new TableException("OVER RANGE FOLLOWING windows are not supported yet.");
        }

        final KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(overProcessFunction);

        OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(OVER_AGGREGATE_TRANSFORMATION, config),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        false);

        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        partitionKeys,
                        InternalTypeInfo.of(inputRowType));
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());

        return transform;
    }

    /**
     * Create an ProcessFunction for unbounded OVER window to evaluate final aggregate value.
     *
     * @param ctx code generator context
     * @param aggCalls physical calls to aggregate functions and their output field names
     * @param constants the constants in aggregates parameters, such as sum(1)
     * @param aggInputRowType physical type of the input row which consists of input and constants.
     * @param inputRowType physical type of the input row which only consists of input.
     * @param isRowsClause it is a tag that indicates whether the OVER clause is ROWS clause
     * @param orderKeys the order by key to sort on
     * @param timeAttribute indicates the type of time attribute the OVER clause is based on
     */
    private KeyedProcessFunction<RowData, RowData, RowData> createUnboundedOverProcessFunction(
            CodeGeneratorContext ctx,
            List<AggregateCall> aggCalls,
            List<RexLiteral> constants,
            RowType aggInputRowType,
            RowType inputRowType,
            boolean isRowsClause,
            int[] orderKeys,
            TimeAttribute timeAttribute,
            ExecNodeConfig config,
            RelBuilder relBuilder,
            FlinkTypeFactory typeFactory) {
        AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        typeFactory,
                        // use aggInputType which considers constants as input instead of
                        // inputSchema.relDataType
                        aggInputRowType,
                        JavaScalaConversionUtil.toScala(aggCalls),
                        new boolean[aggCalls.size()],
                        false, // needInputCount
                        true, // isStateBackendDataViews
                        true); // needDistinctInfo

        LogicalType[] fieldTypes = inputRowType.getChildren().toArray(new LogicalType[0]);

        AggsHandlerCodeGenerator aggsGenerator =
                new AggsHandlerCodeGenerator(
                        ctx,
                        relBuilder,
                        JavaScalaConversionUtil.toScala(Arrays.asList(fieldTypes)),
                        false); // copyInputField

        aggsGenerator =
                aggsGenerator
                        .needAccumulate()
                        // over agg code gen must pass the constants
                        .withConstants(JavaScalaConversionUtil.toScala(constants));

        GeneratedAggsHandleFunction genAggsHandler =
                aggsGenerator.generateAggsHandler("UnboundedOverAggregateHelper", aggInfoList);

        LogicalType[] flattenAccTypes =
                Arrays.stream(aggInfoList.getAccTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);

        switch (timeAttribute) {
            case ROW_TIME:
                final int rowTimeIdx = orderKeys[0];
                switch (unboundedOverVersion) {
                    // Currently there is no migration path between first and second versions.
                    case AbstractRowTimeUnboundedPrecedingOver.FIRST_OVER_VERSION:
                        if (isRowsClause) {
                            // ROWS unbounded over process function
                            return new RowTimeRowsUnboundedPrecedingFunction<>(
                                    config.getStateRetentionTime(),
                                    TableConfigUtils.getMaxIdleStateRetentionTime(config),
                                    genAggsHandler,
                                    flattenAccTypes,
                                    fieldTypes,
                                    rowTimeIdx);
                        } else {
                            // RANGE unbounded over process function
                            return new RowTimeRangeUnboundedPrecedingFunction<>(
                                    config.getStateRetentionTime(),
                                    TableConfigUtils.getMaxIdleStateRetentionTime(config),
                                    genAggsHandler,
                                    flattenAccTypes,
                                    fieldTypes,
                                    rowTimeIdx);
                        }
                    case RowTimeUnboundedPrecedingOverFunctionV2.SECOND_OVER_VERSION:
                        return new RowTimeUnboundedPrecedingOverFunctionV2<>(
                                isRowsClause,
                                config.getStateRetentionTime(),
                                TableConfigUtils.getMaxIdleStateRetentionTime(config),
                                genAggsHandler,
                                flattenAccTypes,
                                fieldTypes,
                                rowTimeIdx);
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported unbounded over version: "
                                        + unboundedOverVersion
                                        + ". Valid versions are 1 and 2.");
                }
            case PROC_TIME:
                return new ProcTimeUnboundedPrecedingFunction<>(
                        StateConfigUtil.createTtlConfig(config.getStateRetentionTime()),
                        genAggsHandler,
                        flattenAccTypes);
            case NON_TIME:
                final GeneratedRecordEqualiser generatedRecordEqualiser =
                        new EqualiserCodeGenerator(inputRowType, ctx.classLoader())
                                .generateRecordEqualiser("FirstMatchingRowEqualiser");

                final LogicalType[] sortKeyTypes = new LogicalType[orderKeys.length];
                for (int i = 0; i < orderKeys.length; i++) {
                    sortKeyTypes[i] = inputRowType.getFields().get(orderKeys[i]).getType();
                }
                final RowType sortKeyRowType = RowType.of(sortKeyTypes);

                final GeneratedRecordEqualiser generatedSortKeyEqualiser =
                        new EqualiserCodeGenerator(sortKeyRowType, ctx.classLoader())
                                .generateRecordEqualiser("FirstMatchingSortKeyEqualiser");

                // Create SortSpec to match sortKeyRowType
                SortSpec.SortSpecBuilder builder = SortSpec.builder();
                IntStream.range(0, orderKeys.length)
                        .forEach(
                                idx ->
                                        builder.addField(
                                                idx,
                                                overSpec.getGroups()
                                                        .get(0)
                                                        .getSort()
                                                        .getFieldSpec(idx)
                                                        .getIsAscendingOrder(),
                                                overSpec.getGroups()
                                                        .get(0)
                                                        .getSort()
                                                        .getFieldSpec(idx)
                                                        .getNullIsLast()));
                SortSpec sortSpecInSortKey = builder.build();

                final GeneratedRecordComparator generatedRecordComparator =
                        ComparatorCodeGenerator.gen(
                                config,
                                ctx.classLoader(),
                                "SortComparator",
                                sortKeyRowType,
                                sortSpecInSortKey);

                InternalTypeInfo<RowData> inputRowTypeInfo = InternalTypeInfo.of(inputRowType);
                RowDataKeySelector sortKeySelector =
                        KeySelectorUtil.getRowDataSelector(
                                ctx.classLoader(), orderKeys, inputRowTypeInfo);

                if (isRowsClause) {
                    // Non-Time Rows Unbounded Preceding Function
                    return new NonTimeRowsUnboundedPrecedingFunction<>(
                            config.getStateRetentionTime(),
                            genAggsHandler,
                            generatedRecordEqualiser,
                            generatedSortKeyEqualiser,
                            generatedRecordComparator,
                            flattenAccTypes,
                            fieldTypes,
                            sortKeyTypes,
                            sortKeySelector);
                } else {
                    // Non-Time Range Unbounded Preceding Function
                    return new NonTimeRangeUnboundedPrecedingFunction<>(
                            config.getStateRetentionTime(),
                            genAggsHandler,
                            generatedRecordEqualiser,
                            generatedSortKeyEqualiser,
                            generatedRecordComparator,
                            flattenAccTypes,
                            fieldTypes,
                            sortKeyTypes,
                            sortKeySelector);
                }
            default:
                throw new TableException(
                        "Unsupported unbounded operation encountered for over aggregate");
        }
    }

    /**
     * Create an ProcessFunction for ROWS clause bounded OVER window to evaluate final aggregate
     * value.
     *
     * @param ctx code generator context
     * @param aggCalls physical calls to aggregate functions and their output field names
     * @param constants the constants in aggregates parameters, such as sum(1)
     * @param aggInputType physical type of the input row which consists of input and constants.
     * @param inputType physical type of the input row which only consists of input.
     * @param isRowsClause it is a tag that indicates whether the OVER clause is ROWS clause
     * @param orderKeys the order by key to sort on
     * @param timeAttribute indicates the type of time attribute the OVER clause is based on
     */
    private KeyedProcessFunction<RowData, RowData, RowData> createBoundedOverProcessFunction(
            CodeGeneratorContext ctx,
            List<AggregateCall> aggCalls,
            List<RexLiteral> constants,
            RowType aggInputType,
            RowType inputType,
            boolean isRowsClause,
            int[] orderKeys,
            TimeAttribute timeAttribute,
            long precedingOffset,
            ExecNodeConfig config,
            RelBuilder relBuilder,
            FlinkTypeFactory typeFactory) {

        boolean[] aggCallNeedRetractions = new boolean[aggCalls.size()];
        Arrays.fill(aggCallNeedRetractions, true);
        AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        typeFactory,
                        // use aggInputType which considers constants as input instead of
                        // inputSchema.relDataType
                        aggInputType,
                        JavaScalaConversionUtil.toScala(aggCalls),
                        aggCallNeedRetractions,
                        true, // needInputCount,
                        true, // isStateBackendDataViews
                        true); // needDistinctInfo

        LogicalType[] fieldTypes = inputType.getChildren().toArray(new LogicalType[0]);
        AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                        ctx,
                        relBuilder,
                        JavaScalaConversionUtil.toScala(Arrays.asList(fieldTypes)),
                        false); // copyInputField

        GeneratedAggsHandleFunction genAggsHandler =
                generator
                        .needRetract()
                        .needAccumulate()
                        // over agg code gen must pass the constants
                        .withConstants(JavaScalaConversionUtil.toScala(constants))
                        .generateAggsHandler("BoundedOverAggregateHelper", aggInfoList);

        LogicalType[] flattenAccTypes =
                Arrays.stream(aggInfoList.getAccTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);

        switch (timeAttribute) {
            case ROW_TIME:
                final int rowTimeIdx = orderKeys[0];
                if (isRowsClause) {
                    return new RowTimeRowsBoundedPrecedingFunction<>(
                            config.getStateRetentionTime(),
                            TableConfigUtils.getMaxIdleStateRetentionTime(config),
                            genAggsHandler,
                            flattenAccTypes,
                            fieldTypes,
                            precedingOffset,
                            rowTimeIdx);
                } else {
                    return new RowTimeRangeBoundedPrecedingFunction<>(
                            genAggsHandler,
                            flattenAccTypes,
                            fieldTypes,
                            precedingOffset,
                            rowTimeIdx);
                }
            case PROC_TIME:
                if (isRowsClause) {
                    return new ProcTimeRowsBoundedPrecedingFunction<>(
                            config.getStateRetentionTime(),
                            TableConfigUtils.getMaxIdleStateRetentionTime(config),
                            genAggsHandler,
                            flattenAccTypes,
                            fieldTypes,
                            precedingOffset);
                } else {
                    return new ProcTimeRangeBoundedPrecedingFunction<>(
                            genAggsHandler, flattenAccTypes, fieldTypes, precedingOffset);
                }
            case NON_TIME:
                throw new TableException(
                        "Non-time attribute sort is not supported for bounded OVER window.");
            default:
                throw new TableException("Unsupported bounded operation for OVER window.");
        }
    }
}
