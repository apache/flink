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
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.isRowtimeIndicatorType;
import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.isTimestampLtzIndicatorType;
import static org.apache.flink.table.planner.plan.utils.MatchUtil.isFinalOnMatchTimeIndicator;
import static org.apache.flink.table.planner.plan.utils.MatchUtil.isMatchRowTimeIndicator;

/**
 * Traverses a {@link RelNode} tree and converts return type of {@link
 * FlinkSqlOperatorTable#MATCH_ROWTIME} from TIMESTAMP to TIMESTAMP_LTZ type if needed.
 */
public final class MatchRowTimeConverter extends RelShuttleImpl {
    private final RexBuilder rexBuilder;

    private MatchRowTimeConverter(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
    }

    public static RelNode convert(RelNode rootRel, RexBuilder rexBuilder) {
        MatchRowTimeConverter converter = new MatchRowTimeConverter(rexBuilder);
        return rootRel.accept(converter);
    }

    @Override
    public RelNode visit(LogicalProject project) {
        Function<List<RelNode>, RelNode> convertFunc =
                inputs -> {
                    MatchRowTimeExprConverter exprRewritter =
                            new MatchRowTimeExprConverter(inputs.get(0));
                    List<RexNode> convertedProjects =
                            project.getProjects().stream()
                                    .map(e -> e.accept(exprRewritter))
                                    .collect(Collectors.toList());
                    List<String> fieldNames = project.getRowType().getFieldNames();
                    return LogicalProject.create(
                            inputs.get(0), Collections.emptyList(), convertedProjects, fieldNames);
                };
        return visitRel(project, convertFunc);
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        Function<List<RelNode>, RelNode> convertFunc =
                inputs -> {
                    MatchRowTimeExprConverter exprRewritter =
                            new MatchRowTimeExprConverter(inputs.get(0));
                    RexNode condition = filter.getCondition().accept(exprRewritter);
                    return LogicalFilter.create(inputs.get(0), condition);
                };
        return visitRel(filter, convertFunc);
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        Function<List<RelNode>, RelNode> convertFunc =
                inputs -> {
                    List<RelDataTypeField> leftRightFieldTypes =
                            inputs.stream()
                                    .map(input -> input.getRowType().getFieldList())
                                    .flatMap(List::stream)
                                    .collect(Collectors.toList());

                    MatchRowTimeExprConverter exprRewritter =
                            new MatchRowTimeExprConverter(leftRightFieldTypes);
                    RexNode newCondition = join.getCondition().accept(exprRewritter);
                    return LogicalJoin.create(
                            inputs.get(0),
                            inputs.get(1),
                            Collections.emptyList(),
                            newCondition,
                            join.getVariablesSet(),
                            join.getJoinType());
                };
        return visitRel(join, convertFunc);
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        Function<List<RelNode>, RelNode> convertFunc =
                inputs -> {
                    MatchRowTimeExprConverter exprRewritter =
                            new MatchRowTimeExprConverter(inputs.get(0));
                    // convert return type MATCH_ROWTIME() from TIMESTAMP to TIMESTAMP_LTZ
                    Function<Map<String, RexNode>, Map<String, RexNode>> convertExprs =
                            rexNodesMap ->
                                    rexNodesMap.entrySet().stream()
                                            .collect(
                                                    Collectors.toMap(
                                                            Map.Entry::getKey,
                                                            e -> e.getValue().accept(exprRewritter),
                                                            (e1, e2) -> e1,
                                                            LinkedHashMap::new));
                    Map<String, RexNode> newPatternDefs =
                            convertExprs.apply(match.getPatternDefinitions());
                    Map<String, RexNode> newMeasures = convertExprs.apply(match.getMeasures());
                    RexNode newInterval = null;
                    if (match.getInterval() != null) {
                        newInterval = match.getInterval().accept(exprRewritter);
                    }

                    // if rewritten MATCH_ROWTIME() returns TIMESTAMP_LTZ, we need to convert the
                    // output
                    // type of LogicalMatch node to TIMESTAMP_LTZ too.
                    Map<String, RelDataType> convertedOutputFields =
                            match.getRowType().getFieldList().stream()
                                    .map(
                                            field -> {
                                                RelDataType fieldType = field.getType();
                                                if (isRowtimeIndicatorType(fieldType)) {
                                                    fieldType =
                                                            ((FlinkTypeFactory)
                                                                            rexBuilder
                                                                                    .getTypeFactory())
                                                                    .createRowtimeIndicatorType(
                                                                            field.getType()
                                                                                    .isNullable(),
                                                                            true);
                                                }
                                                return Tuple2.of(field.getName(), fieldType);
                                            })
                                    .collect(
                                            Collectors.toMap(
                                                    t -> t.f0,
                                                    t -> t.f1,
                                                    (e1, e2) -> e1,
                                                    LinkedHashMap::new));
                    RelDataType outputType =
                            rexBuilder
                                    .getTypeFactory()
                                    .builder()
                                    .addAll(convertedOutputFields.entrySet())
                                    .build();
                    return LogicalMatch.create(
                            inputs.get(0),
                            outputType,
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
                };
        return visitRel(match, convertFunc);
    }

    private <T extends RelNode> RelNode visitRel(
            T r, Function<List<RelNode>, RelNode> convertFunc) {
        List<RelNode> newInputs =
                r.getInputs().stream()
                        .map(input -> input.accept(this))
                        .collect(Collectors.toList());
        boolean hasLtzRowTime =
                newInputs.stream()
                        .anyMatch(
                                input ->
                                        input.getRowType().getFieldList().stream()
                                                .anyMatch(
                                                        f ->
                                                                isTimestampLtzIndicatorType(
                                                                                f.getType())
                                                                        && isRowtimeIndicatorType(
                                                                                f.getType())));
        if (!hasLtzRowTime) {
            return r.copy(r.getTraitSet(), newInputs);
        }
        return convertFunc.apply(newInputs);
    }

    private class MatchRowTimeExprConverter extends RexShuttle {

        private final List<RelDataTypeField> newInputFields;

        private MatchRowTimeExprConverter(List<RelDataTypeField> newInputFields) {
            this.newInputFields = newInputFields;
        }

        private MatchRowTimeExprConverter(RelNode node) {
            this(node.getRowType().getFieldList());
        }

        @Override
        public RexNode visitCall(RexCall call) {
            if (isFinalOnMatchTimeIndicator(call)) {
                // All calls in MEASURES and DEFINE are wrapped with FINAL/RUNNING, therefore
                // we should treat FINAL(MATCH_ROWTIME) and FINAL(MATCH_PROCTIME) as a time
                // attribute extraction.
                RexNode convertedMatchTimeIndicator = call.getOperands().get(0).accept(this);
                RelDataType rowTimeType = convertedMatchTimeIndicator.getType();
                return call.clone(
                        rowTimeType, Collections.singletonList(convertedMatchTimeIndicator));
            } else if (isMatchRowTimeIndicator(call)) {
                // MATCH_ROWTIME() is a no-args function, it can own two kind of return types based
                // on the rowTime attribute type of its input, we rewrite the return type here
                if (!isTimestampLtzIndicatorType(call.getType())) {
                    return call.clone(
                            ((FlinkTypeFactory) rexBuilder.getTypeFactory())
                                    .createRowtimeIndicatorType(call.getType().isNullable(), true),
                            call.getOperands());
                } else {
                    return call;
                }
            } else {
                return super.visitCall(call);
            }
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            RelDataType oldType = inputRef.getType();
            RelDataType newType = newInputFields.get(inputRef.getIndex()).getType();
            if (isTimestampLtzIndicatorType(newType) && !isTimestampLtzIndicatorType(oldType)) {
                return RexInputRef.of(inputRef.getIndex(), newInputFields);
            }
            return super.visitInputRef(inputRef);
        }
    }
}
