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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.over.ProcTimeRangeBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.ProcTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.ProcTimeUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRangeUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRowsUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for time-based over operator. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamExecOverAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(StreamExecOverAggregate.class);

    public static final String FIELD_NAME_OVER_SPEC = "overSpec";

    @JsonProperty(FIELD_NAME_OVER_SPEC)
    private final OverSpec overSpec;

    public StreamExecOverAggregate(
            OverSpec overSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                overSpec,
                getNewNodeId(),
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecOverAggregate(
            @JsonProperty(FIELD_NAME_OVER_SPEC) OverSpec overSpec,
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.overSpec = checkNotNull(overSpec);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
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
        final TableConfig tableConfig = planner.getTableConfig();
        if (partitionKeys.length > 0 && tableConfig.getMinIdleStateRetentionTime() < 0) {
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
        // check time field && identify window rowtime attribute
        final int rowTimeIdx;
        if (orderKeyType instanceof TimestampType
                && ((TimestampType) orderKeyType).getKind() == TimestampKind.ROWTIME) {
            rowTimeIdx = orderKey;
        } else if (orderKeyType instanceof TimestampType
                && ((TimestampType) orderKeyType).getKind() == TimestampKind.PROCTIME) {
            rowTimeIdx = -1;
        } else {
            throw new TableException(
                    "OVER windows' ordering in stream mode must be defined on a time attribute.");
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

        final CodeGeneratorContext ctx = new CodeGeneratorContext(tableConfig);
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
                            rowTimeIdx,
                            group.isRows(),
                            tableConfig,
                            planner.getRelBuilder());
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
                            rowTimeIdx,
                            group.isRows(),
                            precedingOffset,
                            tableConfig,
                            planner.getRelBuilder());
        } else {
            throw new TableException("OVER RANGE FOLLOWING windows are not supported yet.");
        }

        final KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(overProcessFunction);

        OneInputTransformation<RowData, RowData> transform =
                new OneInputTransformation<>(
                        inputTransform,
                        getDescription(),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism());

        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        partitionKeys, InternalTypeInfo.of(inputRowType));
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
     * @param rowTimeIdx the index of the rowtime field or None in case of processing time.
     * @param isRowsClause it is a tag that indicates whether the OVER clause is ROWS clause
     */
    private KeyedProcessFunction<RowData, RowData, RowData> createUnboundedOverProcessFunction(
            CodeGeneratorContext ctx,
            List<AggregateCall> aggCalls,
            List<RexLiteral> constants,
            RowType aggInputRowType,
            RowType inputRowType,
            int rowTimeIdx,
            boolean isRowsClause,
            TableConfig tableConfig,
            RelBuilder relBuilder) {
        AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        // use aggInputType which considers constants as input instead of
                        // inputSchema.relDataType
                        aggInputRowType,
                        JavaScalaConversionUtil.toScala(aggCalls),
                        new boolean[aggCalls.size()],
                        false, // needRetraction
                        true, // isStateBackendDataViews
                        true); // needDistinctInfo

        LogicalType[] fieldTypes = inputRowType.getChildren().toArray(new LogicalType[0]);
        AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                        ctx,
                        relBuilder,
                        JavaScalaConversionUtil.toScala(Arrays.asList(fieldTypes)),
                        false); // copyInputField

        GeneratedAggsHandleFunction genAggsHandler =
                generator
                        .needAccumulate()
                        // over agg code gen must pass the constants
                        .withConstants(JavaScalaConversionUtil.toScala(constants))
                        .generateAggsHandler("UnboundedOverAggregateHelper", aggInfoList);

        LogicalType[] flattenAccTypes =
                Arrays.stream(aggInfoList.getAccTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);

        if (rowTimeIdx >= 0) {
            if (isRowsClause) {
                // ROWS unbounded over process function
                return new RowTimeRowsUnboundedPrecedingFunction<>(
                        tableConfig.getMinIdleStateRetentionTime(),
                        tableConfig.getMaxIdleStateRetentionTime(),
                        genAggsHandler,
                        flattenAccTypes,
                        fieldTypes,
                        rowTimeIdx);
            } else {
                // RANGE unbounded over process function
                return new RowTimeRangeUnboundedPrecedingFunction<>(
                        tableConfig.getMinIdleStateRetentionTime(),
                        tableConfig.getMaxIdleStateRetentionTime(),
                        genAggsHandler,
                        flattenAccTypes,
                        fieldTypes,
                        rowTimeIdx);
            }
        } else {
            return new ProcTimeUnboundedPrecedingFunction<>(
                    tableConfig.getMinIdleStateRetentionTime(),
                    tableConfig.getMaxIdleStateRetentionTime(),
                    genAggsHandler,
                    flattenAccTypes);
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
     * @param rowTimeIdx the index of the rowtime field or None in case of processing time.
     * @param isRowsClause it is a tag that indicates whether the OVER clause is ROWS clause
     */
    private KeyedProcessFunction<RowData, RowData, RowData> createBoundedOverProcessFunction(
            CodeGeneratorContext ctx,
            List<AggregateCall> aggCalls,
            List<RexLiteral> constants,
            RowType aggInputType,
            RowType inputType,
            int rowTimeIdx,
            boolean isRowsClause,
            long precedingOffset,
            TableConfig tableConfig,
            RelBuilder relBuilder) {

        boolean[] aggCallNeedRetractions = new boolean[aggCalls.size()];
        Arrays.fill(aggCallNeedRetractions, true);
        AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
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

        if (rowTimeIdx >= 0) {
            if (isRowsClause) {
                return new RowTimeRowsBoundedPrecedingFunction<>(
                        tableConfig.getMinIdleStateRetentionTime(),
                        tableConfig.getMaxIdleStateRetentionTime(),
                        genAggsHandler,
                        flattenAccTypes,
                        fieldTypes,
                        precedingOffset,
                        rowTimeIdx);
            } else {
                return new RowTimeRangeBoundedPrecedingFunction<>(
                        genAggsHandler, flattenAccTypes, fieldTypes, precedingOffset, rowTimeIdx);
            }
        } else {
            if (isRowsClause) {
                return new ProcTimeRowsBoundedPrecedingFunction<>(
                        tableConfig.getMinIdleStateRetentionTime(),
                        tableConfig.getMaxIdleStateRetentionTime(),
                        genAggsHandler,
                        flattenAccTypes,
                        fieldTypes,
                        precedingOffset);
            } else {
                return new ProcTimeRangeBoundedPrecedingFunction<>(
                        genAggsHandler, flattenAccTypes, fieldTypes, precedingOffset);
            }
        }
    }
}
