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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.SqlWindowTableFunction;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExpand;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;
import org.apache.flink.table.planner.plan.trait.RelWindowProperties;
import org.apache.flink.table.planner.plan.utils.WindowUtil;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Tuple4;

/**
 * This rule transposes {@link StreamPhysicalExpand} past {@link StreamPhysicalWindowTableFunction}
 * to make {@link PullUpWindowTableFunctionIntoWindowAggregateRule} can match the rel tree pattern
 * and optimize them into {@link StreamPhysicalWindowAggregate}.
 *
 * <p>Example:
 *
 * <p>MyTable: a INT, c STRING, rowtime TIMESTAMP(3)
 *
 * <p>SQL:
 *
 * <pre>{@code
 * SELECT
 *    window_start,
 *    window_end,
 *    count(distinct a),
 *    count(distinct c)
 * FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
 * GROUP BY window_start, window_end
 * }</pre>
 *
 * <p>We will get part of the initial physical plan like following:
 *
 * <pre>{@code
 * WindowAggregate(groupBy=[$f4, $f5], window=[TUMBLE(win_start=[window_start],
 * win_end=[window_end], size=[15 min])], select=[$f4, $f5, COUNT(DISTINCT a) FILTER $g_1 AS $f2,
 * COUNT(DISTINCT c) FILTER $g_2 AS $f3, start('w$) AS window_start, end('w$) AS window_end])
 * +- Exchange(distribution=[hash[$f4, $f5]])
 *    +- Calc(select=[window_start, window_end, a, c, $f4, $f5, =($e, 1) AS $g_1, =($e, 2) AS $g_2])
 *       +- Expand(projects=[{window_start, window_end, a, c, $f4, null AS $f5, 1 AS $e},
 *       {window_start, window_end, a, c, null AS $f4, $f5, 2 AS $e}])
 *          +- Calc(select=[window_start, window_end, a, c,
 *          MOD(HASH_CODE(a), 1024) AS $f4, MOD(HASH_CODE(c), 1024) AS $f5])
 *             +- WindowTableFunction(window=[TUMBLE(time_col=[rowtime], size=[15 min])])
 * }</pre>
 *
 * <p>However, it can't match {@link PullUpWindowTableFunctionIntoWindowAggregateRule}, because
 * {@link StreamPhysicalWindowTableFunction} is not near {@link StreamPhysicalWindowAggregate}. So
 * we need to transpose {@link StreamPhysicalExpand} past {@link StreamPhysicalWindowTableFunction}
 * to make the part of rel tree like this which can be matched by {@link
 * PullUpWindowTableFunctionIntoWindowAggregateRule}.
 *
 * <pre>{@code
 * WindowAggregate(groupBy=[$f4, $f5], window=[TUMBLE(win_start=[window_start],
 * win_end=[window_end], size=[15 min])], select=[$f4, $f5, COUNT(DISTINCT a) FILTER $g_1 AS $f2,
 * COUNT(DISTINCT c) FILTER $g_2 AS $f3, start('w$) AS window_start, end('w$) AS window_end])
 * +- Exchange(distribution=[hash[$f4, $f5]])
 *   +- Calc(select=[window_start, window_end, a, c, $f4, $f5, ($e = 1) AS $g_1, ($e = 2) AS $g_2])
 *     +- WindowTableFunction(window=[TUMBLE(time_col=[rowtime], size=[15 min])])
 *       +- Expand(...)
 * }</pre>
 */
@Value.Enclosing
public class ExpandWindowTableFunctionTransposeRule
        extends RelRule<
                ExpandWindowTableFunctionTransposeRule
                        .ExpandWindowTableFunctionTransposeRuleConfig> {

    public static final ExpandWindowTableFunctionTransposeRule INSTANCE =
            ExpandWindowTableFunctionTransposeRule.ExpandWindowTableFunctionTransposeRuleConfig
                    .DEFAULT
                    .toRule();

    protected ExpandWindowTableFunctionTransposeRule(
            ExpandWindowTableFunctionTransposeRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        StreamPhysicalExpand expand = call.rel(0);
        StreamPhysicalCalc calc = call.rel(1);
        FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(calc.getCluster().getMetadataQuery());

        // condition and projection of Calc shouldn't contain calls on window columns,
        // otherwise, we can't transpose WindowTVF and Calc
        if (WindowUtil.calcContainsCallsOnWindowColumns(calc, fmq)) {
            return false;
        }

        // we only transpose WindowTVF when expand propagate window_start and window_end,
        // otherwise, it's meaningless to transpose
        RelWindowProperties expandWindowProps = fmq.getRelWindowProperties(expand);
        return expandWindowProps != null
                && !expandWindowProps.getWindowStartColumns().isEmpty()
                && !expandWindowProps.getWindowEndColumns().isEmpty();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamPhysicalExpand expand = call.rel(0);
        StreamPhysicalCalc calc = call.rel(1);
        StreamPhysicalWindowTableFunction windowTVF = call.rel(2);
        RelOptCluster cluster = expand.getCluster();
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(cluster.getMetadataQuery());
        FlinkTypeFactory typeFactory = (FlinkTypeFactory) cluster.getTypeFactory();
        RelNode input = windowTVF.getInput();
        RelDataType inputRowType = input.getRowType();

        RelTraitSet requiredInputTraitSet =
                input.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
        RelNode newInput = RelOptRule.convert(input, requiredInputTraitSet);

        // -------------------------------------------------------------------------
        //  1. transpose Calc and WindowTVF, build the new Calc node (the top node)
        // -------------------------------------------------------------------------
        ImmutableBitSet windowColumns = fmq.getRelWindowProperties(windowTVF).getWindowColumns();
        Tuple4<RexProgram, int[], Integer, Boolean> programInfo =
                WindowUtil.buildNewProgramWithoutWindowColumns(
                        cluster.getRexBuilder(),
                        calc.getProgram(),
                        inputRowType,
                        windowTVF.windowing().getTimeAttributeIndex(),
                        windowColumns.toArray());
        RexProgram newProgram = programInfo._1();
        int[] fieldShifting = programInfo._2();
        int newTimeField = programInfo._3();
        boolean timeFieldAdded = programInfo._4();

        StreamPhysicalCalc newCalc =
                new StreamPhysicalCalc(
                        cluster,
                        calc.getTraitSet(),
                        newInput,
                        newProgram,
                        newProgram.getOutputRowType());

        // -------------------------------------------------------------------------
        //  2. Adjust input ref index in Expand, append time attribute ref if needed
        // -------------------------------------------------------------------------
        StreamPhysicalExpand newExpand =
                buildNewExpand(expand, newCalc, fieldShifting, newTimeField, timeFieldAdded);

        // -------------------------------------------------------------------------
        //  3. Apply WindowTVF on the new Expand node
        // -------------------------------------------------------------------------
        RelDataType newOutputType =
                SqlWindowTableFunction.inferRowType(
                        typeFactory,
                        newExpand.getRowType(),
                        typeFactory.createFieldTypeFromLogicalType(
                                windowTVF.windowing().getTimeAttributeType()));
        // the time attribute ref is appended
        int timeAttributeOnExpand =
                timeFieldAdded ? newExpand.getRowType().getFieldCount() - 1 : newTimeField;
        TimeAttributeWindowingStrategy newWindowing =
                new TimeAttributeWindowingStrategy(
                        windowTVF.windowing().getWindow(),
                        windowTVF.windowing().getTimeAttributeType(),
                        timeAttributeOnExpand);
        StreamPhysicalWindowTableFunction newWindowTVF =
                new StreamPhysicalWindowTableFunction(
                        cluster, windowTVF.getTraitSet(), newExpand, newOutputType, newWindowing);

        // -------------------------------------------------------------------------
        //  4. Apply Calc on the new WindowTVF to adjust the fields mapping
        // -------------------------------------------------------------------------
        int[] projectionMapping = getProjectionMapping(fmq, expand, newWindowTVF);
        List<RexNode> projectExprs =
                Arrays.stream(projectionMapping)
                        .mapToObj(index -> RexInputRef.of(index, newWindowTVF.getRowType()))
                        .collect(Collectors.toList());
        RexProgram topRexProgram =
                RexProgram.create(
                        newWindowTVF.getRowType(),
                        projectExprs,
                        null,
                        expand.getRowType(),
                        cluster.getRexBuilder());
        StreamPhysicalCalc topCalc =
                new StreamPhysicalCalc(
                        cluster,
                        expand.getTraitSet(),
                        newWindowTVF,
                        topRexProgram,
                        topRexProgram.getOutputRowType());

        // -------------------------------------------------------------------------
        //  5. Finish
        // -------------------------------------------------------------------------
        call.transformTo(topCalc);
    }

    private StreamPhysicalExpand buildNewExpand(
            StreamPhysicalExpand expand,
            StreamPhysicalCalc newCalc,
            int[] inputFieldShifting,
            int newTimeField,
            boolean timeFieldAdded) {
        RelDataType newInputRowType = newCalc.getRowType();
        int expandIdIndex = expand.expandIdIndex();
        int newExpandIdIndex = -1;
        List<List<RexNode>> newProjects = new ArrayList<>();

        for (List<RexNode> exprs : expand.projects()) {
            List<RexNode> newExprs = new ArrayList<>();
            int baseOffset = 0;
            for (int exprIndex = 0; exprIndex < exprs.size(); exprIndex++) {
                RexNode expr = exprs.get(exprIndex);
                if (expr instanceof RexInputRef) {
                    int shiftedIndex = inputFieldShifting[((RexInputRef) expr).getIndex()];
                    if (shiftedIndex < 0) {
                        // skip the window columns
                        continue;
                    }
                    newExprs.add(RexInputRef.of(shiftedIndex, newInputRowType));
                    // we only use the type from input ref instead of literal
                    baseOffset++;
                } else if (expr instanceof RexLiteral) {
                    newExprs.add(expr);
                    if (exprIndex == expandIdIndex) {
                        // this is the expand id, we should remember the new index of expand id
                        // and update type for this expr
                        newExpandIdIndex = baseOffset;
                    }
                    baseOffset++;
                } else {
                    throw new IllegalArgumentException(
                            "Expand node should only contain RexInputRef and RexLiteral, but got "
                                    + expr);
                }
            }
            if (timeFieldAdded) {
                // append time attribute reference if needed
                newExprs.add(RexInputRef.of(newTimeField, newInputRowType));
            }
            newProjects.add(newExprs);
        }

        return new StreamPhysicalExpand(
                expand.getCluster(), expand.getTraitSet(), newCalc, newProjects, newExpandIdIndex);
    }

    private int[] getProjectionMapping(
            FlinkRelMetadataQuery fmq,
            StreamPhysicalExpand oldExpand,
            StreamPhysicalWindowTableFunction newWindowTVF) {
        RelWindowProperties windowProps = fmq.getRelWindowProperties(oldExpand);
        Set<Integer> startColumns =
                Arrays.stream(windowProps.getWindowStartColumns().toArray())
                        .boxed()
                        .collect(Collectors.toSet());
        Set<Integer> endColumns =
                Arrays.stream(windowProps.getWindowEndColumns().toArray())
                        .boxed()
                        .collect(Collectors.toSet());
        Set<Integer> timeColumns =
                Arrays.stream(windowProps.getWindowTimeColumns().toArray())
                        .boxed()
                        .collect(Collectors.toSet());
        int newWindowTimePos = newWindowTVF.getRowType().getFieldCount() - 1;
        int newWindowEndPos = newWindowTVF.getRowType().getFieldCount() - 2;
        int newWindowStartPos = newWindowTVF.getRowType().getFieldCount() - 3;
        int numWindowColumns = 0;
        List<Integer> projectMapping = new ArrayList<>();

        for (int index = 0; index < oldExpand.getRowType().getFieldCount(); index++) {
            if (startColumns.contains(index)) {
                projectMapping.add(newWindowStartPos);
                numWindowColumns++;
            } else if (endColumns.contains(index)) {
                projectMapping.add(newWindowEndPos);
                numWindowColumns++;
            } else if (timeColumns.contains(index)) {
                projectMapping.add(newWindowTimePos);
                numWindowColumns++;
            } else {
                projectMapping.add(index - numWindowColumns);
            }
        }

        return projectMapping.stream().mapToInt(Integer::intValue).toArray();
    }

    /** Configuration for {@link ExpandWindowTableFunctionTransposeRule}. */
    @Value.Immutable(singleton = false)
    public interface ExpandWindowTableFunctionTransposeRuleConfig extends RelRule.Config {
        ExpandWindowTableFunctionTransposeRule.ExpandWindowTableFunctionTransposeRuleConfig
                DEFAULT =
                        ImmutableExpandWindowTableFunctionTransposeRule
                                .ExpandWindowTableFunctionTransposeRuleConfig.builder()
                                .build()
                                .withOperandSupplier(
                                        b0 ->
                                                b0.operand(StreamPhysicalExpand.class)
                                                        .oneInput(
                                                                b1 ->
                                                                        b1.operand(
                                                                                        StreamPhysicalCalc
                                                                                                .class)
                                                                                .oneInput(
                                                                                        b2 ->
                                                                                                b2.operand(
                                                                                                                StreamPhysicalWindowTableFunction
                                                                                                                        .class)
                                                                                                        .anyInputs())))
                                .withDescription("ExpandWindowTableFunctionTransposeRule")
                                .as(
                                        ExpandWindowTableFunctionTransposeRule
                                                .ExpandWindowTableFunctionTransposeRuleConfig
                                                .class);

        @Override
        default ExpandWindowTableFunctionTransposeRule toRule() {
            return new ExpandWindowTableFunctionTransposeRule(this);
        }
    }
}
