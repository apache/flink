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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalOverAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalPythonOverAggregate;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.utils.PythonUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * The physical rule is responsible for converting {@link FlinkLogicalOverAggregate} to {@link
 * StreamPhysicalPythonOverAggregate}.
 */
public class StreamPhysicalPythonOverAggregateRule extends ConverterRule {

    public static final StreamPhysicalPythonOverAggregateRule INSTANCE =
            new StreamPhysicalPythonOverAggregateRule(
                    Config.INSTANCE
                            .withConversion(
                                    FlinkLogicalOverAggregate.class,
                                    FlinkConventions.LOGICAL(),
                                    FlinkConventions.STREAM_PHYSICAL(),
                                    "StreamPhysicalPythonOverAggregateRule")
                            .withRuleFactory(StreamPhysicalPythonOverAggregateRule::new));

    private StreamPhysicalPythonOverAggregateRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalOverAggregate logicWindow = call.rel(0);
        List<AggregateCall> aggCalls = logicWindow.groups.get(0).getAggregateCalls(logicWindow);

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
                        "Non-Pandas Python UDAFs are not supported in stream mode currently.");
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
    public RelNode convert(RelNode rel) {
        FlinkLogicalOverAggregate logicWindow = (FlinkLogicalOverAggregate) rel;

        if (logicWindow.groups.size() > 1) {
            throw new TableException(
                    "Over Agg: Unsupported use of OVER windows. "
                            + "All aggregates must be computed on the same window. "
                            + "please re-check the over window statement.");
        }

        ImmutableBitSet keys = logicWindow.groups.get(0).keys;

        FlinkRelDistribution requiredDistribution;
        if (!keys.isEmpty()) {
            requiredDistribution = FlinkRelDistribution.hash(keys.asList(), true);
        } else {
            requiredDistribution = FlinkRelDistribution.SINGLETON();
        }
        RelNode input = logicWindow.getInput();
        RelTraitSet requiredTraitSet =
                input.getTraitSet()
                        .replace(FlinkConventions.STREAM_PHYSICAL())
                        .replace(requiredDistribution);

        RelTraitSet providedTraitSet =
                rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
        RelNode newInput = RelOptRule.convert(input, requiredTraitSet);

        return new StreamPhysicalPythonOverAggregate(
                rel.getCluster(), providedTraitSet, newInput, rel.getRowType(), logicWindow);
    }
}
