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
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalAggregate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalDistribution;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalExpand;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalIntersect;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalLegacySink;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalMatch;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalMinus;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalOverAggregate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRank;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalScriptTransform;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSink;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSnapshot;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSort;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableAggregate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalUnion;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalValues;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWindowTableAggregate;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;
import org.apache.flink.table.planner.plan.trait.RelWindowProperties;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

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
import static org.apache.flink.table.planner.plan.utils.MatchUtil.isFinalOnMatchTimeIndicator;
import static org.apache.flink.table.planner.plan.utils.WindowUtil.groupingContainsWindowStartEnd;

/**
 * Traverses a {@link RelNode} tree and converts fields with {@link TimeIndicatorRelDataType} type.
 * If a time attribute is accessed for a calculation, it will be materialized. Forwarding is allowed
 * in some cases, but not all.
 */
public final class RelTimeIndicatorConverter extends RelHomogeneousShuttle {

    private final RexBuilder rexBuilder;

    private RelTimeIndicatorConverter(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
    }

    public static RelNode convert(
            RelNode rootRel, RexBuilder rexBuilder, boolean needFinalTimeIndicatorConversion) {
        RelTimeIndicatorConverter converter = new RelTimeIndicatorConverter(rexBuilder);
        RelNode convertedRoot = rootRel.accept(converter);

        // FlinkLogicalLegacySink and FlinkLogicalSink are already converted
        if (rootRel instanceof FlinkLogicalLegacySink
                || rootRel instanceof FlinkLogicalSink
                || !needFinalTimeIndicatorConversion) {
            return convertedRoot;
        }
        // materialize remaining procTime indicators
        return converter.materializeProcTime(convertedRoot);
    }

    @Override
    public RelNode visit(RelNode node) {
        if (node instanceof FlinkLogicalValues || node instanceof TableScan) {
            return node;
        } else if (node instanceof FlinkLogicalIntersect
                || node instanceof FlinkLogicalUnion
                || node instanceof FlinkLogicalMinus) {
            return visitSetOp((SetOp) node);
        } else if (node instanceof FlinkLogicalTableFunctionScan
                || node instanceof FlinkLogicalSnapshot
                || node instanceof FlinkLogicalRank
                || node instanceof FlinkLogicalDistribution
                || node instanceof FlinkLogicalWatermarkAssigner
                || node instanceof FlinkLogicalSort
                || node instanceof FlinkLogicalOverAggregate
                || node instanceof FlinkLogicalExpand
                || node instanceof FlinkLogicalScriptTransform) {
            return visitSimpleRel(node);
        } else if (node instanceof FlinkLogicalWindowAggregate) {
            return visitWindowAggregate((FlinkLogicalWindowAggregate) node);
        } else if (node instanceof FlinkLogicalWindowTableAggregate) {
            return visitWindowTableAggregate((FlinkLogicalWindowTableAggregate) node);
        } else if (node instanceof FlinkLogicalAggregate) {
            return visitAggregate((FlinkLogicalAggregate) node);
        } else if (node instanceof FlinkLogicalTableAggregate) {
            return visitTableAggregate((FlinkLogicalTableAggregate) node);
        } else if (node instanceof FlinkLogicalMatch) {
            return visitMatch((FlinkLogicalMatch) node);
        } else if (node instanceof FlinkLogicalCalc) {
            return visitCalc((FlinkLogicalCalc) node);
        } else if (node instanceof FlinkLogicalCorrelate) {
            return visitCorrelate((FlinkLogicalCorrelate) node);
        } else if (node instanceof FlinkLogicalJoin) {
            return visitJoin((FlinkLogicalJoin) node);
        } else if (node instanceof FlinkLogicalSink) {
            return visitSink((FlinkLogicalSink) node);
        } else if (node instanceof FlinkLogicalLegacySink) {
            return visitSink((FlinkLogicalLegacySink) node);
        } else {
            return visitInvalidRel(node);
        }
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        return visitInvalidRel(calc);
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        return visitInvalidRel(modify);
    }

    private RelNode visitMatch(FlinkLogicalMatch match) {
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

        // materialize all output types
        RelDataType newOutputType =
                getRowTypeWithoutTimeIndicator(match.getRowType(), isNoLongerTimeIndicator);
        return new FlinkLogicalMatch(
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

    private RelNode visitCalc(FlinkLogicalCalc calc) {
        // visit children and update inputs
        RelNode newInput = calc.getInput().accept(this);
        RexProgram program = calc.getProgram();

        // check if input field contains time indicator type
        // materialize field if no time indicator is present anymore
        // if input field is already materialized, change to timestamp type
        RexTimeIndicatorMaterializer materializer = new RexTimeIndicatorMaterializer(newInput);
        List<RexNode> newProjects =
                program.getProjectList().stream()
                        .map(project -> program.expandLocalRef(project).accept(materializer))
                        .collect(Collectors.toList());
        // materialize condition due to filter will validate condition type
        RexNode newCondition = null;
        if (program.getCondition() != null) {
            newCondition = program.expandLocalRef(program.getCondition()).accept(materializer);
        }
        RexProgram newProgram =
                RexProgram.create(
                        newInput.getRowType(),
                        newProjects,
                        newCondition,
                        program.getOutputRowType().getFieldNames(),
                        rexBuilder);
        return calc.copy(calc.getTraitSet(), newInput, newProgram);
    }

    private RelNode visitJoin(FlinkLogicalJoin join) {
        RelNode newLeft = join.getLeft().accept(this);
        RelNode newRight = join.getRight().accept(this);
        int leftFieldCount = newLeft.getRowType().getFieldCount();

        // temporal table join
        if (TemporalJoinUtil.satisfyTemporalJoin(join, newLeft, newRight)) {
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
            return createCalcToMaterializeTimeIndicators(rewrittenTemporalJoin, rightIndices);
        } else {
            if (JoinUtil.satisfyRegularJoin(join, newLeft, newRight)) {
                // materialize time attribute fields of regular join's inputs
                newLeft = materializeTimeIndicators(newLeft);
                newRight = materializeTimeIndicators(newRight);
            }
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
            return FlinkLogicalJoin.create(
                    newLeft, newRight, newCondition, join.getHints(), join.getJoinType());
        }
    }

    private RelNode visitCorrelate(FlinkLogicalCorrelate correlate) {
        // visit children and update inputs
        RelNode newLeft = correlate.getLeft().accept(this);
        RelNode newRight = correlate.getRight().accept(this);
        if (newRight instanceof FlinkLogicalTableFunctionScan) {
            FlinkLogicalTableFunctionScan newScan = (FlinkLogicalTableFunctionScan) newRight;
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
        return FlinkLogicalCorrelate.create(
                newLeft,
                newRight,
                correlate.getCorrelationId(),
                correlate.getRequiredColumns(),
                correlate.getJoinType());
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
                sink instanceof FlinkLogicalLegacySink || sink instanceof FlinkLogicalSink);
        RelNode newInput = sink.getInput().accept(this);
        newInput = materializeProcTime(newInput);
        return sink.copy(sink.getTraitSet(), Collections.singletonList(newInput));
    }

    private FlinkLogicalAggregate visitAggregate(FlinkLogicalAggregate agg) {
        RelNode newInput = convertAggInput(agg);
        List<AggregateCall> updatedAggCalls = convertAggregateCalls(agg);
        return (FlinkLogicalAggregate)
                agg.copy(
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
                                        null,
                                        RelCollations.EMPTY,
                                        callType,
                                        call.name);
                            } else {
                                return call;
                            }
                        })
                .collect(Collectors.toList());
    }

    private RelNode visitTableAggregate(FlinkLogicalTableAggregate tableAgg) {
        FlinkLogicalAggregate correspondingAgg =
                FlinkLogicalAggregate.create(
                        tableAgg.getInput(),
                        tableAgg.getGroupSet(),
                        tableAgg.getGroupSets(),
                        tableAgg.getAggCallList());
        FlinkLogicalAggregate convertedAgg = visitAggregate(correspondingAgg);
        return new FlinkLogicalTableAggregate(
                tableAgg.getCluster(),
                tableAgg.getTraitSet(),
                convertedAgg.getInput(),
                convertedAgg.getGroupSet(),
                convertedAgg.getGroupSets(),
                convertedAgg.getAggCallList());
    }

    private FlinkLogicalWindowAggregate visitWindowAggregate(FlinkLogicalWindowAggregate agg) {
        RelNode newInput = convertAggInput(agg);
        List<AggregateCall> updatedAggCalls = convertAggregateCalls(agg);
        return new FlinkLogicalWindowAggregate(
                agg.getCluster(),
                agg.getTraitSet(),
                newInput,
                agg.getGroupSet(),
                updatedAggCalls,
                agg.getWindow(),
                agg.getNamedProperties());
    }

    private RelNode visitWindowTableAggregate(FlinkLogicalWindowTableAggregate tableAgg) {
        FlinkLogicalWindowAggregate correspondingAgg =
                new FlinkLogicalWindowAggregate(
                        tableAgg.getCluster(),
                        tableAgg.getTraitSet(),
                        tableAgg.getInput(),
                        tableAgg.getGroupSet(),
                        tableAgg.getAggCallList(),
                        tableAgg.getWindow(),
                        tableAgg.getNamedProperties());
        FlinkLogicalWindowAggregate convertedWindowAgg = visitWindowAggregate(correspondingAgg);
        return new FlinkLogicalWindowTableAggregate(
                tableAgg.getCluster(),
                tableAgg.getTraitSet(),
                convertedWindowAgg.getInput(),
                tableAgg.getGroupSet(),
                tableAgg.getGroupSets(),
                convertedWindowAgg.getAggCallList(),
                tableAgg.getWindow(),
                tableAgg.getNamedProperties());
    }

    private RelNode visitInvalidRel(RelNode node) {
        throw new TableException(
                String.format(
                        "This is a bug and should not happen. Please file an issue. Unknown node %s.",
                        node.getRelTypeName()));
    }

    // ----------------------------------------------------------------------------------------
    //                                       Utility
    // ----------------------------------------------------------------------------------------

    private RelNode materializeProcTime(RelNode node) {
        // there is no need to add a redundant calc to materialize proc-time if input is empty
        // values. Otherwise we need add a PruneEmptyRules after the RelTimeIndicatorConverter to
        // remove the redundant calc.
        if (node instanceof FlinkLogicalValues
                && FlinkLogicalValues.isEmpty((FlinkLogicalValues) node)) {
            return node;
        }
        Set<Integer> procTimeFieldIndices = gatherProcTimeIndices(node);
        return materializeTimeIndicators(node, procTimeFieldIndices);
    }

    private RelNode materializeTimeIndicators(RelNode node) {
        Set<Integer> timeFieldIndices = gatherTimeAttributeIndices(node);
        return materializeTimeIndicators(node, timeFieldIndices);
    }

    private RelNode materializeTimeIndicators(RelNode node, Set<Integer> timeIndicatorIndices) {
        if (timeIndicatorIndices.isEmpty()) {
            return node;
        }
        // insert or merge with input calc if
        // a time attribute is accessed and needs to be materialized
        if (node instanceof FlinkLogicalCalc) {
            // merge original calc
            return mergeCalcToMaterializeTimeIndicators(
                    (FlinkLogicalCalc) node, timeIndicatorIndices);
        } else {
            return createCalcToMaterializeTimeIndicators(node, timeIndicatorIndices);
        }
    }

    private RelNode mergeCalcToMaterializeTimeIndicators(
            FlinkLogicalCalc calc, Set<Integer> refIndices) {
        RexProgram program = calc.getProgram();
        RexProgramBuilder newProgramBuilder =
                new RexProgramBuilder(program.getInputRowType(), rexBuilder);
        for (int idx = 0; idx < program.getNamedProjects().size(); idx++) {
            Pair<RexLocalRef, String> pair = program.getNamedProjects().get(idx);
            RexNode project = program.expandLocalRef(pair.left);
            if (refIndices.contains(idx)) {
                project = materializeTimeIndicators(project);
            }
            newProgramBuilder.addProject(project, pair.right);
        }
        if (program.getCondition() != null) {
            newProgramBuilder.addCondition(program.expandLocalRef(program.getCondition()));
        }
        RexProgram newProgram = newProgramBuilder.getProgram();
        return FlinkLogicalCalc.create(calc.getInput(), newProgram);
    }

    private RelNode createCalcToMaterializeTimeIndicators(RelNode input, Set<Integer> refIndices) {
        // create new calc
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
        RexProgram newProgram =
                RexProgram.create(
                        input.getRowType(),
                        projects,
                        null,
                        input.getRowType().getFieldNames(),
                        rexBuilder);
        return FlinkLogicalCalc.create(input, newProgram);
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
            isValid = leftIsEventTime == rightIsEventTime;
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
            RelDataType relType, Predicate<String> shouldMaterialize) {
        Map<String, RelDataType> convertedFields =
                relType.getFieldList().stream()
                        .map(
                                field -> {
                                    RelDataType fieldType = field.getType();
                                    if (isTimeIndicatorType(fieldType)
                                            && shouldMaterialize.test(field.getName())) {
                                        fieldType =
                                                timestamp(
                                                        fieldType.isNullable(),
                                                        isTimestampLtzType(fieldType));
                                    }
                                    return Tuple2.of(field.getName(), fieldType);
                                })
                        .collect(
                                Collectors.toMap(
                                        t -> t.f0, t -> t.f1, (e1, e2) -> e1, LinkedHashMap::new));
        return rexBuilder.getTypeFactory().builder().addAll(convertedFields.entrySet()).build();
    }

    private Set<Integer> gatherProcTimeIndices(RelNode node) {
        return gatherTimeAttributeIndices(node, f -> isProctimeIndicatorType(f.getType()));
    }

    private Set<Integer> gatherTimeAttributeIndices(RelNode node) {
        return gatherTimeAttributeIndices(node, f -> isTimeIndicatorType(f.getType()));
    }

    private Set<Integer> gatherTimeAttributeIndices(
            RelNode node, Predicate<RelDataTypeField> predicate) {
        return node.getRowType().getFieldList().stream()
                .filter(predicate)
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

            // All calls in MEASURES and DEFINE are wrapped with FINAL/RUNNING, therefore
            // we should treat FINAL(MATCH_ROWTIME) and FINAL(MATCH_PROCTIME) as a time attribute
            // extraction
            if (isFinalOnMatchTimeIndicator(call)) {
                return updatedCall;
            } else if (isTimeIndicatorType(updatedCall.getType())) {
                // do not modify window time attributes and some special operators
                if (updatedCallOp == FlinkSqlOperatorTable.TUMBLE_ROWTIME
                        || updatedCallOp == FlinkSqlOperatorTable.TUMBLE_PROCTIME
                        || updatedCallOp == FlinkSqlOperatorTable.HOP_ROWTIME
                        || updatedCallOp == FlinkSqlOperatorTable.HOP_PROCTIME
                        || updatedCallOp == FlinkSqlOperatorTable.SESSION_ROWTIME
                        || updatedCallOp == FlinkSqlOperatorTable.SESSION_PROCTIME
                        || updatedCallOp == FlinkSqlOperatorTable.MATCH_ROWTIME
                        || updatedCallOp == FlinkSqlOperatorTable.MATCH_PROCTIME
                        || updatedCallOp == FlinkSqlOperatorTable.PROCTIME
                        || updatedCallOp == SqlStdOperatorTable.AS
                        || updatedCallOp == SqlStdOperatorTable.CAST
                        || updatedCallOp == FlinkSqlOperatorTable.REINTERPRET) {
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
                if (!isTimeIndicatorType(resolvedRefType)) {
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
                if (!isTimeIndicatorType(resolvedRefType)) {
                    // input has been materialized
                    return new RexPatternFieldRef(
                            fieldRef.getAlpha(), fieldRef.getIndex(), resolvedRefType);
                }
            }
            return super.visitPatternFieldRef(fieldRef);
        }
    }
}
