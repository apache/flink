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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.codegen.over.MultiFieldRangeBoundComparatorCodeGenerator;
import org.apache.flink.table.planner.codegen.over.RangeBoundComparatorCodeGenerator;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec.GroupSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil;
import org.apache.flink.table.planner.plan.utils.SortUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.over.BufferDataOverWindowOperator;
import org.apache.flink.table.runtime.operators.over.NonBufferOverWindowOperator;
import org.apache.flink.table.runtime.operators.over.frame.InsensitiveOverFrame;
import org.apache.flink.table.runtime.operators.over.frame.OffsetOverFrame;
import org.apache.flink.table.runtime.operators.over.frame.OverWindowFrame;
import org.apache.flink.table.runtime.operators.over.frame.RangeSlidingOverFrame;
import org.apache.flink.table.runtime.operators.over.frame.RangeUnboundedFollowingOverFrame;
import org.apache.flink.table.runtime.operators.over.frame.RangeUnboundedPrecedingOverFrame;
import org.apache.flink.table.runtime.operators.over.frame.RowSlidingOverFrame;
import org.apache.flink.table.runtime.operators.over.frame.RowUnboundedFollowingOverFrame;
import org.apache.flink.table.runtime.operators.over.frame.RowUnboundedPrecedingOverFrame;
import org.apache.flink.table.runtime.operators.over.frame.UnboundedOverWindowFrame;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Batch {@link ExecNode} for sort-based over window aggregate. */
public class BatchExecOverAggregate extends BatchExecOverAggregateBase {

    public BatchExecOverAggregate(
            OverSpec overSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(overSpec, inputProperty, outputType, description);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputType = (RowType) inputEdge.getOutputType();

        final TableConfig tableConfig = planner.getTableConfig();
        // The generated sort is used for generating the comparator among partitions.
        // So here not care the ASC or DESC for the grouping fields.
        // TODO just replace comparator to equaliser
        final int[] partitionFields = overSpec.getPartition().getFieldIndices();
        final GeneratedRecordComparator genComparator =
                ComparatorCodeGenerator.gen(
                        tableConfig,
                        "SortComparator",
                        inputType,
                        SortUtil.getAscendingSortSpec(partitionFields));
        // use aggInputType which considers constants as input instead of inputType
        final RowType inputTypeWithConstants = getInputTypeWithConstants();
        // Over operator could support different order-by keys with collation satisfied.
        // Currently, this operator requires all order keys (combined with partition keys) are
        // the same, but order-by keys may be different. Consider the following sql:
        // select *, sum(b) over partition by a order by a, count(c) over partition by a from T
        // So we can use any one from the groups. To keep the behavior with the rule, we use the
        // last one.
        final SortSpec sortSpec =
                overSpec.getGroups().get(overSpec.getGroups().size() - 1).getSort();

        final TableStreamOperator<RowData> operator;
        final long managedMemory;
        if (!needBufferData()) {
            // operator needn't cache data
            final int numOfGroup = overSpec.getGroups().size();
            final GeneratedAggsHandleFunction[] aggsHandlers =
                    new GeneratedAggsHandleFunction[numOfGroup];
            final boolean[] resetAccumulators = new boolean[numOfGroup];
            for (int i = 0; i < numOfGroup; ++i) {
                GroupSpec group = overSpec.getGroups().get(i);
                AggregateInfoList aggInfoList =
                        AggregateUtil.transformToBatchAggregateInfoList(
                                inputTypeWithConstants,
                                JavaScalaConversionUtil.toScala(group.getAggCalls()),
                                null, // aggCallNeedRetractions
                                sortSpec.getFieldIndices());
                AggsHandlerCodeGenerator generator =
                        new AggsHandlerCodeGenerator(
                                new CodeGeneratorContext(tableConfig),
                                planner.getRelBuilder(),
                                JavaScalaConversionUtil.toScala(inputType.getChildren()),
                                false); // copyInputField
                // over agg code gen must pass the constants
                aggsHandlers[i] =
                        generator
                                .needAccumulate()
                                .withConstants(JavaScalaConversionUtil.toScala(getConstants()))
                                .generateAggsHandler("BoundedOverAggregateHelper", aggInfoList);
                OverWindowMode mode = inferGroupMode(group);
                resetAccumulators[i] =
                        mode == OverWindowMode.ROW
                                && group.getLowerBound().isCurrentRow()
                                && group.getUpperBound().isCurrentRow();
            }

            operator =
                    new NonBufferOverWindowOperator(aggsHandlers, genComparator, resetAccumulators);
            managedMemory = 0L;
        } else {
            List<OverWindowFrame> windowFrames =
                    createOverWindowFrames(planner, inputType, sortSpec, inputTypeWithConstants);
            operator =
                    new BufferDataOverWindowOperator(
                            windowFrames.toArray(new OverWindowFrame[0]),
                            genComparator,
                            inputType.getChildren().stream()
                                    .allMatch(BinaryRowData::isInFixedLengthPart));
            managedMemory =
                    tableConfig
                            .getConfiguration()
                            .get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY)
                            .getBytes();
        }
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                getDescription(),
                SimpleOperatorFactory.of(operator),
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism(),
                managedMemory);
    }

    private List<OverWindowFrame> createOverWindowFrames(
            PlannerBase planner,
            RowType inputType,
            SortSpec sortSpec,
            RowType inputTypeWithConstants) {
        final List<OverWindowFrame> windowFrames = new ArrayList<>();
        for (GroupSpec group : overSpec.getGroups()) {
            OverWindowMode mode = inferGroupMode(group);
            if (mode == OverWindowMode.OFFSET) {
                for (AggregateCall aggCall : group.getAggCalls()) {
                    AggregateInfoList aggInfoList =
                            AggregateUtil.transformToBatchAggregateInfoList(
                                    inputTypeWithConstants,
                                    JavaScalaConversionUtil.toScala(
                                            Collections.singletonList(aggCall)),
                                    new boolean[] {
                                        true
                                    }, /* needRetraction = true, See LeadLagAggFunction */
                                    sortSpec.getFieldIndices());

                    AggsHandlerCodeGenerator generator =
                            new AggsHandlerCodeGenerator(
                                    new CodeGeneratorContext(planner.getTableConfig()),
                                    planner.getRelBuilder(),
                                    JavaScalaConversionUtil.toScala(inputType.getChildren()),
                                    false); // copyInputField

                    // over agg code gen must pass the constants
                    GeneratedAggsHandleFunction genAggsHandler =
                            generator
                                    .needAccumulate()
                                    .needRetract()
                                    .withConstants(JavaScalaConversionUtil.toScala(getConstants()))
                                    .generateAggsHandler("BoundedOverAggregateHelper", aggInfoList);

                    // LEAD is behind the currentRow, so we need plus offset.
                    // LAG is in front of the currentRow, so we need minus offset.
                    long flag = aggCall.getAggregation().kind == SqlKind.LEAD ? 1L : -1L;

                    final Long offset;
                    final OffsetOverFrame.CalcOffsetFunc calcOffsetFunc;

                    // LEAD ( expression [, offset [, default] ] )
                    // LAG ( expression [, offset [, default] ] )
                    // The second arg mean the offset arg index for leag/lag function, default is 1.
                    if (aggCall.getArgList().size() >= 2) {
                        int constantIndex =
                                aggCall.getArgList().get(1) - overSpec.getOriginalInputFields();
                        if (constantIndex < 0) {
                            offset = null;
                            int rowIndex = aggCall.getArgList().get(1);
                            switch (inputType.getTypeAt(rowIndex).getTypeRoot()) {
                                case BIGINT:
                                    calcOffsetFunc =
                                            (OffsetOverFrame.CalcOffsetFunc)
                                                    row -> row.getLong(rowIndex) * flag;
                                    break;
                                case INTEGER:
                                    calcOffsetFunc =
                                            (OffsetOverFrame.CalcOffsetFunc)
                                                    row -> (long) row.getInt(rowIndex) * flag;
                                    break;
                                case SMALLINT:
                                    calcOffsetFunc =
                                            (OffsetOverFrame.CalcOffsetFunc)
                                                    row -> (long) row.getShort(rowIndex) * flag;
                                    break;
                                default:
                                    throw new RuntimeException(
                                            "The column type must be in long/int/short.");
                            }
                        } else {
                            long constantOffset =
                                    getConstants().get(constantIndex).getValueAs(Long.class);
                            offset = constantOffset * flag;
                            calcOffsetFunc = null;
                        }
                    } else {
                        offset = flag;
                        calcOffsetFunc = null;
                    }
                    windowFrames.add(new OffsetOverFrame(genAggsHandler, offset, calcOffsetFunc));
                }
            } else {
                AggregateInfoList aggInfoList =
                        AggregateUtil.transformToBatchAggregateInfoList(
                                // use aggInputType which considers constants as input instead of
                                // inputSchema.relDataType
                                inputTypeWithConstants,
                                JavaScalaConversionUtil.toScala(group.getAggCalls()),
                                null, // aggCallNeedRetractions
                                sortSpec.getFieldIndices());
                AggsHandlerCodeGenerator generator =
                        new AggsHandlerCodeGenerator(
                                new CodeGeneratorContext(planner.getTableConfig()),
                                planner.getRelBuilder(),
                                JavaScalaConversionUtil.toScala(inputType.getChildren()),
                                false); // copyInputField
                // over agg code gen must pass the constants
                GeneratedAggsHandleFunction genAggsHandler =
                        generator
                                .needAccumulate()
                                .withConstants(JavaScalaConversionUtil.toScala(getConstants()))
                                .generateAggsHandler("BoundedOverAggregateHelper", aggInfoList);

                RowType valueType = generator.valueType();
                final OverWindowFrame frame;
                switch (mode) {
                    case RANGE:
                        if (isUnboundedWindow(group)) {
                            frame = new UnboundedOverWindowFrame(genAggsHandler, valueType);
                        } else if (isUnboundedPrecedingWindow(group)) {
                            GeneratedRecordComparator genBoundComparator =
                                    createBoundComparator(
                                            planner,
                                            sortSpec,
                                            group.getUpperBound(),
                                            false,
                                            inputType);
                            frame =
                                    new RangeUnboundedPrecedingOverFrame(
                                            genAggsHandler, genBoundComparator);
                        } else if (isUnboundedFollowingWindow(group)) {
                            GeneratedRecordComparator genBoundComparator =
                                    createBoundComparator(
                                            planner,
                                            sortSpec,
                                            group.getLowerBound(),
                                            true,
                                            inputType);
                            frame =
                                    new RangeUnboundedFollowingOverFrame(
                                            valueType, genAggsHandler, genBoundComparator);
                        } else if (isSlidingWindow(group)) {
                            GeneratedRecordComparator genLeftBoundComparator =
                                    createBoundComparator(
                                            planner,
                                            sortSpec,
                                            group.getLowerBound(),
                                            true,
                                            inputType);
                            GeneratedRecordComparator genRightBoundComparator =
                                    createBoundComparator(
                                            planner,
                                            sortSpec,
                                            group.getUpperBound(),
                                            false,
                                            inputType);
                            frame =
                                    new RangeSlidingOverFrame(
                                            inputType,
                                            valueType,
                                            genAggsHandler,
                                            genLeftBoundComparator,
                                            genRightBoundComparator);
                        } else {
                            throw new TableException("This should not happen.");
                        }
                        break;
                    case ROW:
                        if (isUnboundedWindow(group)) {
                            frame = new UnboundedOverWindowFrame(genAggsHandler, valueType);
                        } else if (isUnboundedPrecedingWindow(group)) {
                            frame =
                                    new RowUnboundedPrecedingOverFrame(
                                            genAggsHandler,
                                            OverAggregateUtil.getLongBoundary(
                                                    overSpec, group.getUpperBound()));
                        } else if (isUnboundedFollowingWindow(group)) {
                            frame =
                                    new RowUnboundedFollowingOverFrame(
                                            valueType,
                                            genAggsHandler,
                                            OverAggregateUtil.getLongBoundary(
                                                    overSpec, group.getLowerBound()));
                        } else if (isSlidingWindow(group)) {
                            frame =
                                    new RowSlidingOverFrame(
                                            inputType,
                                            valueType,
                                            genAggsHandler,
                                            OverAggregateUtil.getLongBoundary(
                                                    overSpec, group.getLowerBound()),
                                            OverAggregateUtil.getLongBoundary(
                                                    overSpec, group.getUpperBound()));
                        } else {
                            throw new TableException("This should not happen.");
                        }
                        break;
                    case INSENSITIVE:
                        frame = new InsensitiveOverFrame(genAggsHandler);
                        break;
                    default:
                        throw new TableException("This should not happen.");
                }
                windowFrames.add(frame);
            }
        }
        return windowFrames;
    }

    private GeneratedRecordComparator createBoundComparator(
            PlannerBase planner,
            SortSpec sortSpec,
            RexWindowBound windowBound,
            boolean isLowerBound,
            RowType inputType) {
        Object bound = OverAggregateUtil.getBoundary(overSpec, windowBound);
        if (!windowBound.isCurrentRow()) {
            // Range Window only support comparing based on a field.
            int sortKey = sortSpec.getFieldIndices()[0];
            return new RangeBoundComparatorCodeGenerator(
                            planner.getRelBuilder(),
                            planner.getTableConfig(),
                            inputType,
                            bound,
                            sortKey,
                            inputType.getTypeAt(sortKey),
                            sortSpec.getAscendingOrders()[0],
                            isLowerBound)
                    .generateBoundComparator("RangeBoundComparator");
        } else {
            // if the bound is current row, then window support comparing based on multi fields.
            return new MultiFieldRangeBoundComparatorCodeGenerator(
                            planner.getTableConfig(), inputType, sortSpec, isLowerBound)
                    .generateBoundComparator("MultiFieldRangeBoundComparator");
        }
    }

    private boolean needBufferData() {
        return overSpec.getGroups().stream()
                .anyMatch(
                        group -> {
                            OverWindowMode mode = inferGroupMode(group);
                            switch (mode) {
                                case INSENSITIVE:
                                    return false;
                                case ROW:
                                    return (!group.getLowerBound().isCurrentRow()
                                                    || !group.getUpperBound().isCurrentRow())
                                            && (!group.getLowerBound().isUnbounded()
                                                    || !group.getUpperBound().isCurrentRow());
                                default:
                                    return true;
                            }
                        });
    }
}
