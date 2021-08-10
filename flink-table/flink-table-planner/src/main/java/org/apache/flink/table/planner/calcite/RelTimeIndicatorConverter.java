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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalLegacySink;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalRank;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalSink;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalTableAggregate;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowTableAggregate;
import org.apache.flink.table.planner.plan.nodes.hive.LogicalDistribution;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;
import org.apache.flink.table.planner.plan.trait.RelWindowProperties;
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSnapshot;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.isProctimeIndicatorType;
import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.isRowtimeIndicatorType;
import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.isTimeIndicatorType;
import static org.apache.flink.table.planner.plan.utils.MatchUtil.isFinalOnRowTimeIndicator;
import static org.apache.flink.table.planner.plan.utils.MatchUtil.isMatchRowTimeIndicator;
import static org.apache.flink.table.planner.plan.utils.WindowUtil.groupingContainsWindowStartEnd;

/**
 * Traverses a {@link RelNode} tree and converts fields with {@link TimeIndicatorRelDataType} type.
 * If a time attribute is accessed for a calculation, it will be materialized. Forwarding is allowed
 * in some cases, but not all.
 */
public final class RelTimeIndicatorConverter implements RelShuttle {

    private final RexBuilder rexBuilder;

    private RelTimeIndicatorConverter(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
    }

    public static RelNode convert(
            RelNode rootRel, RexBuilder rexBuilder, boolean needFinalTimeIndicatorConversion) {
        RelTimeIndicatorConverter converter = new RelTimeIndicatorConverter(rexBuilder);
        RelNode convertedRoot = rootRel.accept(converter);

        // LogicalLegacySink and LogicalSink are already converted
        if (rootRel instanceof LogicalLegacySink
                || rootRel instanceof LogicalSink
                || !needFinalTimeIndicatorConversion) {
            return convertedRoot;
        }
        // materialize remaining procTime indicators
        return converter.materializeProcTime(convertedRoot);
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        return visitSetOp(intersect);
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        return visitSetOp(union);
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        return visitSetOp(minus);
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        return visitAggregate(aggregate);
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        return visitSimpleRel(sort);
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        RelNode newInput = match.getInput().accept(this);
        RexTimeIndicatorMaterializer materializer = new RexTimeIndicatorMaterializer(newInput);

        Function<Map<String, RexNode>, Map<String, RexNode>> materializeExprs =
                rexNodesMap ->
                        rexNodesMap.entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                                Map.Entry::getKey,
                                                e -> e.getValue().accept(materializer),
                                                (e1, e2) -> e1,
                                                LinkedHashMap::new));
        // update input expressions
        Map<String, RexNode> newPatternDefs = materializeExprs.apply(match.getPatternDefinitions());
        Map<String, RexNode> newMeasures = materializeExprs.apply(match.getMeasures());
        RexNode newInterval = null;
        if (match.getInterval() != null) {
            newInterval = match.getInterval().accept(materializer);
        }

        Predicate<String> isNoLongerTimeIndicator =
                fieldName -> {
                    RexNode newMeasure = newMeasures.get(fieldName);
                    if (newMeasure == null) {
                        return false;
                    } else {
                        return !isTimeIndicatorType(newMeasure.getType());
                    }
                };

        // decide the MATCH_ROWTIME() return type is TIMESTAMP or TIMESTAMP_LTZ, if it is
        // TIMESTAMP_LTZ, we need to materialize the output type of LogicalMatch node to
        // TIMESTAMP_LTZ too.
        boolean isTimestampLtz =
                newMeasures.values().stream().anyMatch(node -> isTimestampLtzType(node.getType()));
        // materialize all output types
        RelDataType newOutputType =
                getRowTypeWithoutTimeIndicator(
                        match.getRowType(), isTimestampLtz, isNoLongerTimeIndicator);
        return new LogicalMatch(
                match.getCluster(),
                match.getTraitSet(),
                newInput,
                newOutputType,
                match.getPattern(),
                match.isStrictStart(),
                match.isStrictEnd(),
                newPatternDefs,
                newMeasures,
                match.getAfter(),
                match.getSubsets(),
                match.isAllRows(),
                match.getPartitionKeys(),
                match.getOrderKeys(),
                newInterval);
    }

    @Override
    public RelNode visit(RelNode node) {
        if (node instanceof Collect) {
            return node;
        } else if (node instanceof Uncollect
                || node instanceof LogicalTableFunctionScan
                || node instanceof LogicalSnapshot
                || node instanceof LogicalRank
                || node instanceof LogicalDistribution
                || node instanceof LogicalWatermarkAssigner) {
            return visitSimpleRel(node);
        } else if (node instanceof LogicalWindowAggregate) {
            return visitAggregate((LogicalWindowAggregate) node);
        } else if (node instanceof LogicalWindowTableAggregate) {
            LogicalWindowTableAggregate tableAgg = (LogicalWindowTableAggregate) node;
            LogicalWindowAggregate correspondingAgg =
                    new LogicalWindowAggregate(
                            tableAgg.getCluster(),
                            tableAgg.getTraitSet(),
                            tableAgg.getInput(),
                            tableAgg.getGroupSet(),
                            tableAgg.getAggCallList(),
                            tableAgg.getWindow(),
                            tableAgg.getNamedProperties());
            LogicalWindowAggregate convertedAgg =
                    (LogicalWindowAggregate) visitAggregate(correspondingAgg);
            return new LogicalWindowTableAggregate(
                    tableAgg.getCluster(),
                    tableAgg.getTraitSet(),
                    convertedAgg.getInput(),
                    tableAgg.getGroupSet(),
                    tableAgg.getGroupSets(),
                    convertedAgg.getAggCallList(),
                    tableAgg.getWindow(),
                    tableAgg.getNamedProperties());
        } else if (node instanceof LogicalTableAggregate) {
            LogicalTableAggregate tableAgg = (LogicalTableAggregate) node;
            LogicalAggregate correspondingAgg =
                    LogicalAggregate.create(
                            tableAgg.getInput(),
                            tableAgg.getGroupSet(),
                            tableAgg.getGroupSets(),
                            tableAgg.getAggCallList());
            LogicalAggregate convertedAgg = (LogicalAggregate) visitAggregate(correspondingAgg);
            return new LogicalTableAggregate(
                    tableAgg.getCluster(),
                    tableAgg.getTraitSet(),
                    convertedAgg.getInput(),
                    convertedAgg.getGroupSet(),
                    convertedAgg.getGroupSets(),
                    convertedAgg.getAggCallList());
        } else if (node instanceof LogicalSink) {
            return visitSink((LogicalSink) node);
        } else if (node instanceof LogicalLegacySink) {
            return visitSink((LogicalLegacySink) node);
        } else {
            throw new TableException(
                    "Unsupported logical operator: " + node.getClass().getSimpleName());
        }
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        throw new TableException("Logical exchange in a stream environment is not supported yet.");
    }

    @Override
    public RelNode visit(TableScan scan) {
        return scan;
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        throw new TableException(
                "Table function scan in a stream environment is not supported yet.");
    }

    @Override
    public RelNode visit(LogicalValues values) {
        return values;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        // visit children and update inputs
        RelNode newInput = filter.getInput().accept(this);

        // check if input field contains time indicator type
        // materialize field if no time indicator is present anymore
        // if input field is already materialized, change to timestamp type
        RexTimeIndicatorMaterializer materializer = new RexTimeIndicatorMaterializer(newInput);
        // materialize condition due to filter will validate condition type
        RexNode newCondition = null;
        if (filter.getCondition() != null) {
            newCondition = filter.getCondition().accept(materializer);
        }
        return LogicalFilter.create(newInput, newCondition);
    }

    @Override
    public RelNode visit(LogicalProject project) {
        // visit children and update inputs
        RelNode newInput = project.getInput().accept(this);

        // check if input field contains time indicator type
        // materialize field if no time indicator is present anymore
        // if input field is already materialized, change to timestamp type
        RexTimeIndicatorMaterializer materializer = new RexTimeIndicatorMaterializer(newInput);
        List<RexNode> newProjects =
                project.getProjects().stream()
                        .map(p -> p.accept(materializer))
                        .collect(Collectors.toList());
        return LogicalProject.create(
                newInput,
                Collections.emptyList(),
                newProjects,
                project.getRowType().getFieldNames());
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        // Do nothing for Calc now.
        return calc;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        RelNode newLeft = join.getLeft().accept(this);
        RelNode newRight = join.getRight().accept(this);
        int leftFieldCount = newLeft.getRowType().getFieldCount();

        // temporal table join
        if (TemporalJoinUtil.containsTemporalJoinCondition(join.getCondition())) {
            RelNode rewrittenTemporalJoin =
                    join.copy(
                            join.getTraitSet(),
                            join.getCondition(),
                            newLeft,
                            newRight,
                            join.getJoinType(),
                            join.isSemiJoinDone());

            // Materialize all of the time attributes from the right side of temporal join
            Set<Integer> rightIndices =
                    IntStream.range(0, newRight.getRowType().getFieldCount())
                            .mapToObj(startIdx -> leftFieldCount + startIdx)
                            .collect(Collectors.toSet());
            return createProjectToMaterializeTimeIndicators(rewrittenTemporalJoin, rightIndices);
        } else {
            List<RelDataTypeField> leftRightFields = new ArrayList<>();
            leftRightFields.addAll(newLeft.getRowType().getFieldList());
            leftRightFields.addAll(newRight.getRowType().getFieldList());

            RexNode newCondition =
                    join.getCondition()
                            .accept(
                                    new RexShuttle() {
                                        @Override
                                        public RexNode visitInputRef(RexInputRef inputRef) {
                                            if (isTimeIndicatorType(inputRef.getType())) {
                                                return RexInputRef.of(
                                                        inputRef.getIndex(), leftRightFields);
                                            } else {
                                                return super.visitInputRef(inputRef);
                                            }
                                        }
                                    });
            return LogicalJoin.create(
                    newLeft,
                    newRight,
                    Collections.emptyList(),
                    newCondition,
                    join.getVariablesSet(),
                    join.getJoinType());
        }
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        // visit children and update inputs
        RelNode newLeft = correlate.getLeft().accept(this);
        RelNode newRight = correlate.getRight().accept(this);
        if (newRight instanceof LogicalTableFunctionScan) {
            LogicalTableFunctionScan newScan = (LogicalTableFunctionScan) newRight;
            List<RelNode> newScanInputs =
                    newScan.getInputs().stream()
                            .map(input -> input.accept(this))
                            .collect(Collectors.toList());
            // check if input field contains time indicator type
            // materialize field if no time indicator is present anymore
            // if input field is already materialized, change to timestamp type
            RexTimeIndicatorMaterializer materializer = new RexTimeIndicatorMaterializer(newLeft);

            RexNode newScanCall = newScan.getCall().accept(materializer);
            newRight =
                    newScan.copy(
                            newScan.getTraitSet(),
                            newScanInputs,
                            newScanCall,
                            newScan.getElementType(),
                            newScan.getRowType(),
                            newScan.getColumnMappings());
        }
        return LogicalCorrelate.create(
                newLeft,
                newRight,
                correlate.getCorrelationId(),
                correlate.getRequiredColumns(),
                correlate.getJoinType());
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        return visitSimpleRel(modify);
    }

    private RelNode visitSimpleRel(RelNode node) {
        List<RelNode> newInputs =
                node.getInputs().stream()
                        .map(input -> input.accept(this))
                        .collect(Collectors.toList());
        return node.copy(node.getTraitSet(), newInputs);
    }

    private RelNode visitSetOp(SetOp setOp) {
        RelNode convertedSetOp = visitSimpleRel(setOp);

        // make sure that time indicator types match
        List<RelDataTypeField> headInputFields =
                convertedSetOp.getInputs().get(0).getRowType().getFieldList();
        int fieldCnt = headInputFields.size();
        for (int inputIdx = 1; inputIdx < convertedSetOp.getInputs().size(); inputIdx++) {
            List<RelDataTypeField> currentInputFields =
                    convertedSetOp.getInputs().get(inputIdx).getRowType().getFieldList();
            for (int fieldIdx = 0; fieldIdx < fieldCnt; fieldIdx++) {
                RelDataType headFieldType = headInputFields.get(fieldIdx).getType();
                RelDataType currentInputFieldType = currentInputFields.get(fieldIdx).getType();
                validateType(currentInputFieldType, headFieldType);
            }
        }
        return convertedSetOp;
    }

    private RelNode visitSink(SingleRel sink) {
        Preconditions.checkArgument(
                sink instanceof LogicalLegacySink || sink instanceof LogicalSink);
        RelNode newInput = sink.getInput().accept(this);
        newInput = materializeProcTime(newInput);
        return sink.copy(sink.getTraitSet(), Collections.singletonList(newInput));
    }

    private RelNode visitAggregate(Aggregate agg) {
        RelNode newInput = convertAggInput(agg);
        List<AggregateCall> updatedAggCalls = convertAggregateCalls(agg);
        return agg.copy(
                agg.getTraitSet(),
                newInput,
                agg.getGroupSet(),
                agg.getGroupSets(),
                updatedAggCalls);
    }

    private RelNode convertAggInput(Aggregate agg) {
        RelNode newInput = agg.getInput().accept(this);
        // materialize aggregation arguments/grouping keys
        Set<Integer> timeIndicatorIndices = gatherIndicesToMaterialize(agg, newInput);
        return materializeTimeIndicators(newInput, timeIndicatorIndices);
    }

    private Set<Integer> gatherIndicesToMaterialize(Aggregate agg, RelNode newInput) {
        List<RelDataType> inputFieldTypes = RelOptUtil.getFieldTypeList(newInput.getRowType());
        Predicate<Integer> isTimeIndicator = idx -> isTimeIndicatorType(inputFieldTypes.get(idx));
        // add arguments of agg calls
        Set<Integer> aggCallArgs =
                agg.getAggCallList().stream()
                        .map(AggregateCall::getArgList)
                        .flatMap(List::stream)
                        .filter(isTimeIndicator)
                        .collect(Collectors.toSet());

        FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(agg.getCluster().getMetadataQuery());
        RelWindowProperties windowProps = fmq.getRelWindowProperties(newInput);
        // add grouping sets
        Set<Integer> groupSets =
                agg.getGroupSets().stream()
                        .map(
                                grouping -> {
                                    if (windowProps != null
                                            && groupingContainsWindowStartEnd(
                                                    grouping, windowProps)) {
                                        // for window aggregate we should reserve the time attribute
                                        // of window_time column
                                        return grouping.except(windowProps.getWindowTimeColumns());
                                    } else {
                                        return grouping;
                                    }
                                })
                        .flatMap(set -> set.asList().stream())
                        .filter(isTimeIndicator)
                        .collect(Collectors.toSet());
        Set<Integer> timeIndicatorIndices = new HashSet<>(aggCallArgs);
        timeIndicatorIndices.addAll(groupSets);
        return timeIndicatorIndices;
    }

    private List<AggregateCall> convertAggregateCalls(Aggregate agg) {
        // remove time indicator type as agg call return type
        return agg.getAggCallList().stream()
                .map(
                        call -> {
                            if (isTimeIndicatorType(call.getType())) {
                                RelDataType callType =
                                        timestamp(
                                                call.getType().isNullable(),
                                                isTimestampLtzType(call.getType()));
                                return AggregateCall.create(
                                        call.getAggregation(),
                                        call.isDistinct(),
                                        false,
                                        false,
                                        call.getArgList(),
                                        call.filterArg,
                                        RelCollations.EMPTY,
                                        callType,
                                        call.name);
                            } else {
                                return call;
                            }
                        })
                .collect(Collectors.toList());
    }

    // ----------------------------------------------------------------------------------------
    //                                       Utility
    // ----------------------------------------------------------------------------------------

    private RelNode materializeProcTime(RelNode node) {
        Set<Integer> procTimeFieldIndices = gatherProcTimeIndices(node);
        return materializeTimeIndicators(node, procTimeFieldIndices);
    }

    private RelNode materializeTimeIndicators(RelNode node, Set<Integer> timeIndicatorIndices) {
        if (timeIndicatorIndices.isEmpty()) {
            return node;
        }
        // insert or merge with input project if
        // a time attribute is accessed and needs to be materialized
        if (node instanceof LogicalProject) {
            // merge original calc
            return mergeProjectToMaterializeTimeIndicators(
                    (LogicalProject) node, timeIndicatorIndices);
        } else {
            return createProjectToMaterializeTimeIndicators(node, timeIndicatorIndices);
        }
    }

    private RelNode mergeProjectToMaterializeTimeIndicators(
            LogicalProject project, Set<Integer> refIndices) {
        List<RexNode> oldProjects = project.getProjects();
        List<RexNode> newProjects =
                IntStream.range(0, oldProjects.size())
                        .mapToObj(
                                idx -> {
                                    RexNode oldProject = oldProjects.get(idx);
                                    if (refIndices.contains(idx)) {
                                        return materializeTimeIndicators(oldProject);
                                    } else {
                                        return oldProject;
                                    }
                                })
                        .collect(Collectors.toList());
        return LogicalProject.create(
                project.getInput(),
                Collections.emptyList(),
                newProjects,
                project.getRowType().getFieldNames());
    }

    private RelNode createProjectToMaterializeTimeIndicators(
            RelNode input, Set<Integer> refIndices) {
        // create new project
        List<RexNode> projects =
                input.getRowType().getFieldList().stream()
                        .map(
                                field -> {
                                    RexNode project =
                                            new RexInputRef(field.getIndex(), field.getType());
                                    if (refIndices.contains(field.getIndex())) {
                                        project = materializeTimeIndicators(project);
                                    }
                                    return project;
                                })
                        .collect(Collectors.toList());
        return LogicalProject.create(
                input, Collections.emptyList(), projects, input.getRowType().getFieldNames());
    }

    private RexNode materializeTimeIndicators(RexNode expr) {
        if (isRowtimeIndicatorType(expr.getType())) {
            // cast rowTime indicator to regular timestamp
            return rexBuilder.makeAbstractCast(
                    timestamp(expr.getType().isNullable(), isTimestampLtzType(expr.getType())),
                    expr);
        } else if (isProctimeIndicatorType(expr.getType())) {
            // generate procTime access
            return rexBuilder.makeCall(FlinkSqlOperatorTable.PROCTIME_MATERIALIZE, expr);
        } else {
            return expr;
        }
    }

    private void validateType(RelDataType l, RelDataType r) {
        boolean isValid;
        // check if time indicators match
        if (isTimeIndicatorType(l) && isTimeIndicatorType(r)) {
            boolean leftIsEventTime = ((TimeIndicatorRelDataType) l).isEventTime();
            boolean rightIsEventTime = ((TimeIndicatorRelDataType) r).isEventTime();
            if (leftIsEventTime && rightIsEventTime) {
                isValid = isTimestampLtzType(l) == isTimestampLtzType(r);
            } else {
                isValid = leftIsEventTime == rightIsEventTime;
            }
        } else {
            isValid = !isTimeIndicatorType(l) && !isTimeIndicatorType(r);
        }
        if (!isValid) {
            throw new ValidationException(
                    String.format(
                            "Union fields with time attributes requires same types, but the types are %s and %s.",
                            l, r));
        }
    }

    private RelDataType getRowTypeWithoutTimeIndicator(
            RelDataType relType, boolean isTimestampLtzType, Predicate<String> shouldMaterialize) {
        Map<String, RelDataType> convertedFields =
                relType.getFieldList().stream()
                        .map(
                                field -> {
                                    RelDataType fieldType = field.getType();
                                    if (isTimeIndicatorType(fieldType)) {
                                        if (isTimestampLtzType) {
                                            fieldType =
                                                    ((FlinkTypeFactory) rexBuilder.getTypeFactory())
                                                            .createFieldTypeFromLogicalType(
                                                                    new LocalZonedTimestampType(
                                                                            fieldType.isNullable(),
                                                                            TimestampKind.ROWTIME,
                                                                            3));
                                        }
                                        if (shouldMaterialize.test(field.getName())) {
                                            fieldType =
                                                    timestamp(
                                                            fieldType.isNullable(),
                                                            isTimestampLtzType(fieldType));
                                        }
                                    }
                                    return Tuple2.of(field.getName(), fieldType);
                                })
                        .collect(
                                Collectors.toMap(
                                        t -> t.f0, t -> t.f1, (e1, e2) -> e1, LinkedHashMap::new));
        return rexBuilder.getTypeFactory().builder().addAll(convertedFields.entrySet()).build();
    }

    private Set<Integer> gatherProcTimeIndices(RelNode node) {
        return node.getRowType().getFieldList().stream()
                .filter(f -> isProctimeIndicatorType(f.getType()))
                .map(RelDataTypeField::getIndex)
                .collect(Collectors.toSet());
    }

    private RelDataType timestamp(boolean isNullable, boolean isTimestampLtzIndicator) {
        LogicalType logicalType;
        if (isTimestampLtzIndicator) {
            logicalType = new LocalZonedTimestampType(isNullable, 3);
        } else {
            logicalType = new TimestampType(isNullable, 3);
        }
        return ((FlinkTypeFactory) rexBuilder.getTypeFactory())
                .createFieldTypeFromLogicalType(logicalType);
    }

    private boolean isTimestampLtzType(RelDataType type) {
        return type.getSqlTypeName().equals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }

    // ----------------------------------------------------------------------------------------
    //          Materializer for RexNode including time indicator
    // ----------------------------------------------------------------------------------------

    private class RexTimeIndicatorMaterializer extends RexShuttle {

        private final List<RelDataType> inputFieldTypes;

        private RexTimeIndicatorMaterializer(RelNode node) {
            this(RelOptUtil.getFieldTypeList(node.getRowType()));
        }

        private RexTimeIndicatorMaterializer(List<RelDataType> inputFieldTypes) {
            this.inputFieldTypes = inputFieldTypes;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            RexCall updatedCall = (RexCall) super.visitCall(call);

            // materialize operands with time indicators
            List<RexNode> materializedOperands;
            SqlOperator updatedCallOp = updatedCall.getOperator();
            if (updatedCallOp == FlinkSqlOperatorTable.SESSION_OLD
                    || updatedCallOp == FlinkSqlOperatorTable.HOP_OLD
                    || updatedCallOp == FlinkSqlOperatorTable.TUMBLE_OLD) {
                // skip materialization for special operators
                materializedOperands = updatedCall.getOperands();
            } else {
                materializedOperands =
                        updatedCall.getOperands().stream()
                                .map(RelTimeIndicatorConverter.this::materializeTimeIndicators)
                                .collect(Collectors.toList());
            }

            if (isFinalOnRowTimeIndicator(call)) {
                // All calls in MEASURES and DEFINE are wrapped with FINAL/RUNNING, therefore
                // we should treat FINAL(MATCH_ROWTIME) and FINAL(MATCH_PROCTIME) as a time
                // attribute extraction.
                // The type of FINAL(MATCH_ROWTIME) is inferred by first operand's type,
                // the initial type of MATCH_ROWTIME is TIMESTAMP(3) *ROWTIME*, it may be rewrote.
                RelDataType rowTimeType = updatedCall.getOperands().get(0).getType();
                return rexBuilder.makeCall(
                        rowTimeType, updatedCall.getOperator(), updatedCall.getOperands());
            } else if (isMatchRowTimeIndicator(updatedCall)) {
                // MATCH_ROWTIME() is a no-args function, it can own two kind of return types based
                // on the rowTime attribute type of its input, we rewrite the return type here
                RelDataType firstRowTypeType =
                        inputFieldTypes.stream()
                                .filter(FlinkTypeFactory::isTimeIndicatorType)
                                .findFirst()
                                .get();
                return rexBuilder.makeCall(
                        ((FlinkTypeFactory) rexBuilder.getTypeFactory())
                                .createRowtimeIndicatorType(
                                        updatedCall.getType().isNullable(),
                                        isTimestampLtzType(firstRowTypeType)),
                        updatedCall.getOperator(),
                        materializedOperands);
            } else if (isTimeIndicatorType(updatedCall.getType())) {
                // do not modify window time attributes and some special operators
                if (updatedCallOp == FlinkSqlOperatorTable.TUMBLE_ROWTIME
                        || updatedCallOp == FlinkSqlOperatorTable.TUMBLE_PROCTIME
                        || updatedCallOp == FlinkSqlOperatorTable.HOP_ROWTIME
                        || updatedCallOp == FlinkSqlOperatorTable.HOP_PROCTIME
                        || updatedCallOp == FlinkSqlOperatorTable.SESSION_ROWTIME
                        || updatedCallOp == FlinkSqlOperatorTable.SESSION_PROCTIME
                        || updatedCallOp == FlinkSqlOperatorTable.MATCH_PROCTIME
                        || updatedCallOp == FlinkSqlOperatorTable.PROCTIME
                        || updatedCallOp == SqlStdOperatorTable.AS) {
                    return updatedCall;
                } else {
                    // materialize function's result and operands
                    return updatedCall.clone(
                            timestamp(
                                    updatedCall.getType().isNullable(),
                                    isTimestampLtzType(updatedCall.getType())),
                            materializedOperands);
                }
            } else {
                // materialize function's operands only
                return updatedCall.clone(updatedCall.getType(), materializedOperands);
            }
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            RelDataType oldType = inputRef.getType();
            if (isTimeIndicatorType(oldType)) {
                RelDataType resolvedRefType = inputFieldTypes.get(inputRef.getIndex());
                if (isTimestampLtzType(resolvedRefType) && !isTimestampLtzType(oldType)) {
                    // input has been converted from TIMESTAMP to TIMESTAMP_LTZ type
                    return rexBuilder.makeInputRef(resolvedRefType, inputRef.getIndex());
                } else if (!isTimeIndicatorType(resolvedRefType)) {
                    // input has been materialized
                    return new RexInputRef(inputRef.getIndex(), resolvedRefType);
                }
            }
            return super.visitInputRef(inputRef);
        }

        @Override
        public RexNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
            RelDataType oldType = fieldRef.getType();
            if (isTimeIndicatorType(oldType)) {
                RelDataType resolvedRefType = inputFieldTypes.get(fieldRef.getIndex());
                if (isTimestampLtzType(resolvedRefType) && !isTimestampLtzType(oldType)) {
                    // input has been converted from TIMESTAMP to TIMESTAMP_LTZ type
                    return rexBuilder.makePatternFieldRef(
                            fieldRef.getAlpha(), resolvedRefType, fieldRef.getIndex());
                } else if (!isTimeIndicatorType(resolvedRefType)) {
                    // input has been materialized
                    return new RexPatternFieldRef(
                            fieldRef.getAlpha(), fieldRef.getIndex(), resolvedRefType);
                }
            }
            return super.visitPatternFieldRef(fieldRef);
        }
    }
}
