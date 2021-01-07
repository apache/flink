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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.planner.calcite.FlinkRelFactories;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SessionGroupWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalPythonGroupWindowAggregate;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.plan.utils.PythonUtil;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedList;
import java.util.List;

import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Seq;

/**
 * The physical rule is responsible for convert {@link FlinkLogicalWindowAggregate} to {@link
 * BatchPhysicalPythonGroupWindowAggregate}.
 */
public class BatchPhysicalPythonWindowAggregateRule extends RelOptRule {

    public static final RelOptRule INSTANCE = new BatchPhysicalPythonWindowAggregateRule();

    private BatchPhysicalPythonWindowAggregateRule() {
        super(
                operand(FlinkLogicalWindowAggregate.class, operand(RelNode.class, any())),
                FlinkRelFactories.LOGICAL_BUILDER_WITHOUT_AGG_INPUT_PRUNE(),
                "BatchPhysicalPythonWindowAggregateRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalWindowAggregate agg = call.rel(0);
        List<AggregateCall> aggCalls = agg.getAggCallList();

        boolean existGeneralPythonFunction =
                aggCalls.stream()
                        .anyMatch(x -> PythonUtil.isPythonAggregate(x, PythonFunctionKind.GENERAL));
        boolean existPandasFunction =
                aggCalls.stream()
                        .anyMatch(x -> PythonUtil.isPythonAggregate(x, PythonFunctionKind.PANDAS));
        boolean existJavaFunction =
                aggCalls.stream().anyMatch(x -> !PythonUtil.isPythonAggregate(x, null));
        if (existPandasFunction || existGeneralPythonFunction) {
            if (existJavaFunction) {
                throw new TableException(
                        "Python UDAF and Java/Scala UDAF cannot be used together.");
            }
            if (existPandasFunction && existGeneralPythonFunction) {
                throw new TableException(
                        "Pandas UDAF and non-Pandas UDAF cannot be used together.");
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalWindowAggregate agg = call.rel(0);
        RelNode input = agg.getInput();
        LogicalWindow window = agg.getWindow();

        if (!(window instanceof TumblingGroupWindow
                        && AggregateUtil.hasTimeIntervalType(((TumblingGroupWindow) window).size())
                || window instanceof SlidingGroupWindow
                        && AggregateUtil.hasTimeIntervalType(((SlidingGroupWindow) window).size())
                || window instanceof SessionGroupWindow)) {
            // sliding & tumbling count window and session window not supported
            throw new TableException("Window " + window + " is not supported right now.");
        }

        int[] groupSet = agg.getGroupSet().toArray();
        RelTraitSet traitSet = agg.getTraitSet().replace(FlinkConventions.BATCH_PHYSICAL());

        Tuple2<int[], Seq<AggregateCall>> auxGroupSetAndCallsTuple =
                AggregateUtil.checkAndSplitAggCalls(agg);
        int[] auxGroupSet = auxGroupSetAndCallsTuple._1;
        Seq<AggregateCall> aggCallsWithoutAuxGroupCalls = auxGroupSetAndCallsTuple._2;

        Tuple3<int[][], DataType[][], UserDefinedFunction[]> aggBufferTypesAndFunctions =
                AggregateUtil.transformToBatchAggregateFunctions(
                        FlinkTypeFactory.toLogicalRowType(input.getRowType()),
                        aggCallsWithoutAuxGroupCalls,
                        null);
        UserDefinedFunction[] aggFunctions = aggBufferTypesAndFunctions._3();

        int inputTimeFieldIndex =
                AggregateUtil.timeFieldIndex(
                        input.getRowType(), call.builder(), window.timeAttribute());
        RelDataType inputTimeFieldType =
                input.getRowType().getFieldList().get(inputTimeFieldIndex).getType();
        boolean inputTimeIsDate = inputTimeFieldType.getSqlTypeName() == SqlTypeName.DATE;

        RelTraitSet requiredTraitSet = agg.getTraitSet().replace(FlinkConventions.BATCH_PHYSICAL());
        if (groupSet.length != 0) {
            FlinkRelDistribution requiredDistribution = FlinkRelDistribution.hash(groupSet, false);
            requiredTraitSet = requiredTraitSet.replace(requiredDistribution);
        } else {
            requiredTraitSet = requiredTraitSet.replace(FlinkRelDistribution.SINGLETON());
        }

        RelCollation sortCollation = createRelCollation(groupSet, inputTimeFieldIndex);
        requiredTraitSet = requiredTraitSet.replace(sortCollation);

        RelNode newInput = RelOptRule.convert(input, requiredTraitSet);
        BatchPhysicalPythonGroupWindowAggregate windowAgg =
                new BatchPhysicalPythonGroupWindowAggregate(
                        agg.getCluster(),
                        traitSet,
                        newInput,
                        agg.getRowType(),
                        newInput.getRowType(),
                        groupSet,
                        auxGroupSet,
                        aggCallsWithoutAuxGroupCalls,
                        aggFunctions,
                        window,
                        inputTimeFieldIndex,
                        inputTimeIsDate,
                        agg.getNamedProperties());
        call.transformTo(windowAgg);
    }

    private RelCollation createRelCollation(int[] groupSet, int timeIndex) {
        List<RelFieldCollation> fields = new LinkedList<>();
        for (int value : groupSet) {
            fields.add(FlinkRelOptUtil.ofRelFieldCollation(value));
        }
        fields.add(FlinkRelOptUtil.ofRelFieldCollation(timeIndex));
        return RelCollations.of(fields);
    }
}
