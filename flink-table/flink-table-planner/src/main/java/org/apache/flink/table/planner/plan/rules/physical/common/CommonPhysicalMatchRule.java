/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.physical.common;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.plan.logical.MatchRecognize;
import org.apache.flink.table.planner.plan.nodes.FlinkConvention;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalMatch;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.utils.MatchUtil.AggregationPatternVariableFinder;
import org.apache.flink.table.planner.plan.utils.RexDefaultVisitor;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The physical rule is responsible for converting {@link FlinkLogicalMatch} to physical Match rel.
 */
public abstract class CommonPhysicalMatchRule extends ConverterRule {

    public CommonPhysicalMatchRule(
            Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String descriptionPrefix) {
        super(Config.INSTANCE.as(Config.class).withConversion(clazz, in, out, descriptionPrefix));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalMatch logicalMatch = call.rel(0);

        validateAggregations(logicalMatch.getMeasures().values());
        validateAggregations(logicalMatch.getPatternDefinitions().values());
        // This check might be obsolete once CALCITE-2747 is resolved
        validateAmbiguousColumns(logicalMatch);
        return true;
    }

    public RelNode convert(RelNode rel, FlinkConvention convention) {
        FlinkLogicalMatch logicalMatch = (FlinkLogicalMatch) rel;
        RelTraitSet traitSet = rel.getTraitSet().replace(convention);
        ImmutableBitSet partitionKeys = logicalMatch.getPartitionKeys();

        FlinkRelDistribution requiredDistribution =
                partitionKeys.isEmpty()
                        ? FlinkRelDistribution.SINGLETON()
                        : FlinkRelDistribution.hash(logicalMatch.getPartitionKeys().asList(), true);
        RelTraitSet requiredTraitSet =
                rel.getCluster()
                        .getPlanner()
                        .emptyTraitSet()
                        .replace(requiredDistribution)
                        .replace(convention);

        RelNode convertInput = RelOptRule.convert(logicalMatch.getInput(), requiredTraitSet);

        try {
            Class.forName(
                    "org.apache.flink.cep.pattern.Pattern",
                    false,
                    ShortcutUtils.unwrapContext(rel).getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new TableException(
                    "MATCH RECOGNIZE clause requires flink-cep dependency to be present on the classpath.",
                    e);
        }
        return convertToPhysicalMatch(
                rel.getCluster(),
                traitSet,
                convertInput,
                new MatchRecognize(
                        logicalMatch.getPattern(),
                        logicalMatch.getPatternDefinitions(),
                        logicalMatch.getMeasures(),
                        logicalMatch.getAfter(),
                        logicalMatch.getSubsets(),
                        logicalMatch.isAllRows(),
                        logicalMatch.getPartitionKeys(),
                        logicalMatch.getOrderKeys(),
                        logicalMatch.getInterval()),
                logicalMatch.getRowType());
    }

    protected abstract RelNode convertToPhysicalMatch(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode convertInput,
            MatchRecognize matchRecognize,
            RelDataType rowType);

    private void validateAggregations(Iterable<RexNode> expr) {
        AggregationsValidator validator = new AggregationsValidator();
        expr.forEach(e -> e.accept(validator));
    }

    private void validateAmbiguousColumns(FlinkLogicalMatch logicalMatch) {
        if (logicalMatch.isAllRows()) {
            throw new TableException("All rows per match mode is not supported yet.");
        } else {
            validateAmbiguousColumnsOnRowPerMatch(
                    logicalMatch.getPartitionKeys(),
                    logicalMatch.getMeasures().keySet(),
                    logicalMatch.getInput().getRowType(),
                    logicalMatch.getRowType());
        }
    }

    private void validateAmbiguousColumnsOnRowPerMatch(
            ImmutableBitSet partitionKeys,
            Set<String> measuresNames,
            RelDataType inputSchema,
            RelDataType expectedSchema) {
        int actualSize = partitionKeys.toArray().length + measuresNames.size();
        int expectedSize = expectedSchema.getFieldCount();
        if (actualSize != expectedSize) {
            // try to find ambiguous column

            String ambiguousColumns =
                    Arrays.stream(partitionKeys.toArray())
                            .mapToObj(k -> inputSchema.getFieldList().get(k).getName())
                            .filter(measuresNames::contains)
                            .collect(Collectors.joining(", ", "{", "}"));

            throw new ValidationException(
                    String.format("Columns ambiguously defined: %s", ambiguousColumns));
        }
    }

    private static class AggregationsValidator extends RexDefaultVisitor<Object> {

        @Override
        public Object visitCall(RexCall call) {
            SqlOperator operator = call.getOperator();
            if (operator instanceof SqlAggFunction) {
                call.accept(new AggregationPatternVariableFinder());
            } else {
                call.getOperands().forEach(o -> o.accept(this));
            }
            return null;
        }

        @Override
        public Object visitNode(RexNode rexNode) {
            return null;
        }
    }
}
