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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule to convert complex aggregation functions into simpler ones. Have a look at {@link
 * AggregateReduceFunctionsRule} for details.
 */
public class WindowAggregateReduceFunctionsRule extends AggregateReduceFunctionsRule {
    private static final RelBuilderFactory LOGICAL_BUILDER_WITHOUT_AGG_INPUT_PRUNE =
            RelBuilder.proto(
                    Contexts.of(
                            RelFactories.DEFAULT_STRUCT,
                            RelBuilder.Config.DEFAULT.withPruneInputOfAggregate(false)));

    public static final WindowAggregateReduceFunctionsRule INSTANCE =
            new WindowAggregateReduceFunctionsRule(
                    Config.DEFAULT
                            .withRelBuilderFactory(LOGICAL_BUILDER_WITHOUT_AGG_INPUT_PRUNE)
                            .withOperandSupplier(
                                    b -> b.operand(LogicalWindowAggregate.class).anyInputs())
                            .withDescription("WindowAggregateReduceFunctionsRule")
                            .as(Config.class));

    protected WindowAggregateReduceFunctionsRule(Config config) {
        super(config);
    }

    @Override
    protected void newAggregateRel(
            RelBuilder relBuilder, Aggregate oldAgg, List<AggregateCall> newCalls) {

        // create a LogicalAggregate with simpler aggregation functions
        super.newAggregateRel(relBuilder, oldAgg, newCalls);
        // pop LogicalAggregate from RelBuilder
        LogicalAggregate newAgg = (LogicalAggregate) relBuilder.build();

        // create a new LogicalWindowAggregate (based on the new LogicalAggregate) and push it on
        // the
        // RelBuilder
        LogicalWindowAggregate oldWindowAgg = (LogicalWindowAggregate) oldAgg;
        LogicalWindowAggregate newWindowAgg =
                LogicalWindowAggregate.create(
                        oldWindowAgg.getWindow(), oldWindowAgg.getNamedProperties(), newAgg);
        relBuilder.push(newWindowAgg);
    }

    @Override
    protected void newCalcRel(RelBuilder relBuilder, RelDataType rowType, List<RexNode> exprs) {
        int numExprs = exprs.size();
        // add all named properties of the window to the selection
        new ArrayList<>(rowType.getFieldList().subList(numExprs, rowType.getFieldCount()))
                .forEach(f -> exprs.add(relBuilder.field(f.getName())));
        // create a LogicalCalc that computes the complex aggregates and forwards the window
        // properties
        relBuilder.project(exprs, rowType.getFieldNames());
    }
}
