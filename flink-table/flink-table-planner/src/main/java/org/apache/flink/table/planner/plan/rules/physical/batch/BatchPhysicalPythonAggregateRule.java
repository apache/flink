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
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalPythonGroupAggregate;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.plan.utils.PythonUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.AggregateCall;

import java.util.LinkedList;
import java.util.List;

import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Seq;

/**
 * The physical rule which is responsible for converting {@link FlinkLogicalAggregate} to {@link
 * BatchPhysicalPythonGroupAggregate}.
 */
public class BatchPhysicalPythonAggregateRule extends ConverterRule {

    public static final RelOptRule INSTANCE =
            new BatchPhysicalPythonAggregateRule(
                    Config.INSTANCE
                            .withConversion(
                                    FlinkLogicalAggregate.class,
                                    FlinkConventions.LOGICAL(),
                                    FlinkConventions.BATCH_PHYSICAL(),
                                    "BatchPhysicalPythonAggregateRule")
                            .withRuleFactory(BatchPhysicalPythonAggregateRule::new));

    protected BatchPhysicalPythonAggregateRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalAggregate agg = call.rel(0);
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
            if (existGeneralPythonFunction) {
                throw new TableException(
                        "non-Pandas UDAFs are not supported in batch mode currently.");
            }
            if (existJavaFunction) {
                throw new TableException(
                        "Python UDAF and Java/Scala UDAF cannot be used together.");
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public RelNode convert(RelNode relNode) {
        FlinkLogicalAggregate agg = (FlinkLogicalAggregate) relNode;
        RelNode input = agg.getInput();

        int[] groupSet = agg.getGroupSet().toArray();
        RelTraitSet traitSet = relNode.getTraitSet().replace(FlinkConventions.BATCH_PHYSICAL());

        Tuple2<int[], Seq<AggregateCall>> auxGroupSetAndCallsTuple =
                AggregateUtil.checkAndSplitAggCalls(agg);
        int[] auxGroupSet = auxGroupSetAndCallsTuple._1;
        Seq<AggregateCall> aggCallsWithoutAuxGroupCalls = auxGroupSetAndCallsTuple._2;

        Tuple3<int[][], DataType[][], UserDefinedFunction[]> aggBufferTypesAndFunctions =
                AggregateUtil.transformToBatchAggregateFunctions(
                        ShortcutUtils.unwrapTypeFactory(input),
                        FlinkTypeFactory.toLogicalRowType(input.getRowType()),
                        aggCallsWithoutAuxGroupCalls,
                        null);
        UserDefinedFunction[] aggFunctions = aggBufferTypesAndFunctions._3();

        RelTraitSet requiredTraitSet =
                input.getTraitSet().replace(FlinkConventions.BATCH_PHYSICAL());
        if (groupSet.length != 0) {
            FlinkRelDistribution requiredDistribution = FlinkRelDistribution.hash(groupSet, false);
            requiredTraitSet = requiredTraitSet.replace(requiredDistribution);
            RelCollation sortCollation = createRelCollation(groupSet);
            requiredTraitSet = requiredTraitSet.replace(sortCollation);
        } else {
            requiredTraitSet = requiredTraitSet.replace(FlinkRelDistribution.SINGLETON());
        }
        RelNode convInput = RelOptRule.convert(input, requiredTraitSet);

        return new BatchPhysicalPythonGroupAggregate(
                relNode.getCluster(),
                traitSet,
                convInput,
                agg.getRowType(),
                convInput.getRowType(),
                convInput.getRowType(),
                groupSet,
                auxGroupSet,
                aggCallsWithoutAuxGroupCalls,
                aggFunctions);
    }

    private RelCollation createRelCollation(int[] groupSet) {
        List<RelFieldCollation> fields = new LinkedList<>();
        for (int value : groupSet) {
            fields.add(FlinkRelOptUtil.ofRelFieldCollation(value));
        }
        return RelCollations.of(fields);
    }
}
