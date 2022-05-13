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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.hint.FlinkHints;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlJsonArrayAggAggFunction;
import org.apache.calcite.sql.fun.SqlJsonObjectAggAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.JSON_STRING;

/**
 * Transforms JSON aggregation functions by wrapping operands into {@link
 * BuiltInFunctionDefinitions#JSON_STRING}.
 *
 * <p>Essentially, a call like {@code JSON_OBJECTAGG(f0 VALUE f1)} will be transformed into {@code
 * JSON_OBJECTAGG(f0 VALUE JSON_STRING(f1))}. By placing a marker {@link RelHint} on the aggregation
 * afterwards we ensure that this transformation occurs just once.
 *
 * <p>{@link BuiltInFunctionDefinitions#JSON_STRING} will take care of serializing the values into
 * their correct representation, and the actual aggregation function's implementation can simply
 * insert the values as raw nodes instead. This avoids having to re-implement the logic for all
 * supported types in the aggregation function again.
 */
@Internal
public class WrapJsonAggFunctionArgumentsRule
        extends RelRule<WrapJsonAggFunctionArgumentsRule.Config> {

    public static final RelOptRule INSTANCE =
            Config.EMPTY.as(Config.class).onJsonAggregateFunctions().toRule();

    /** Marker hint that a call has already been transformed. */
    private static final RelHint MARKER_HINT =
            RelHint.builder(FlinkHints.HINT_NAME_JSON_AGGREGATE_WRAPPED).build();

    public WrapJsonAggFunctionArgumentsRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalAggregate aggregate = call.rel(0);
        final AggregateCall aggCall = aggregate.getAggCallList().get(0);

        final RelNode aggInput = aggregate.getInput();
        final RelBuilder relBuilder = call.builder().push(aggInput);

        final List<Integer> affectedArgs = getAffectedArgs(aggCall);
        addProjections(aggregate.getCluster(), relBuilder, affectedArgs);

        final TargetMapping argsMapping =
                getAggArgsMapping(aggInput.getRowType().getFieldCount(), affectedArgs);

        final AggregateCall newAggregateCall = aggCall.transform(argsMapping);
        final LogicalAggregate newAggregate =
                aggregate.copy(
                        aggregate.getTraitSet(),
                        relBuilder.build(),
                        aggregate.getGroupSet(),
                        aggregate.getGroupSets(),
                        Collections.singletonList(newAggregateCall));
        call.transformTo(newAggregate.withHints(Collections.singletonList(MARKER_HINT)));
    }

    /**
     * Returns the aggregation's arguments which need to be wrapped.
     *
     * <p>This list is a subset of {@link AggregateCall#getArgList()} as not every argument may need
     * to be wrapped into a {@link BuiltInFunctionDefinitions#JSON_STRING} call.
     *
     * <p>Duplicates (e.g. for {@code JSON_OBJECTAGG(f0 VALUE f0)}) are removed as we only need to
     * wrap them once.
     */
    private List<Integer> getAffectedArgs(AggregateCall aggCall) {
        if (aggCall.getAggregation() instanceof SqlJsonObjectAggAggFunction) {
            // For JSON_OBJECTAGG we only need to wrap its second (= value) argument
            final int valueIndex = aggCall.getArgList().get(1);
            return Collections.singletonList(valueIndex);
        }

        return aggCall.getArgList().stream().distinct().collect(Collectors.toList());
    }

    /**
     * Adds (wrapped) projections for affected arguments of the aggregation.
     *
     * <p>Note that we cannot override any of the projections as a field may be used multiple times,
     * and in particular outside of the aggregation call. Therefore, we explicitly add the wrapped
     * projection as an additional one.
     */
    private void addProjections(
            RelOptCluster cluster, RelBuilder relBuilder, List<Integer> affectedArgs) {
        final BridgingSqlFunction operandToStringOperator =
                BridgingSqlFunction.of(cluster, JSON_STRING);

        final List<RexNode> projects = new ArrayList<>();
        affectedArgs.stream()
                .map(argIdx -> relBuilder.call(operandToStringOperator, relBuilder.field(argIdx)))
                .forEach(projects::add);

        relBuilder.projectPlus(projects);
    }

    /**
     * Returns a {@link TargetMapping} that defines how the arguments of the aggregation must be
     * mapped such that the wrapped arguments are used instead.
     */
    private TargetMapping getAggArgsMapping(int inputCount, List<Integer> affectedArgs) {
        final int newCount = inputCount + affectedArgs.size();

        final TargetMapping argsMapping =
                Mappings.create(MappingType.BIJECTION, newCount, newCount);
        for (int i = 0; i < affectedArgs.size(); i++) {
            argsMapping.set(affectedArgs.get(i), inputCount + i);
        }

        return argsMapping;
    }

    private static boolean isJsonAggregation(AggregateCall aggCall) {
        final SqlAggFunction aggregation = aggCall.getAggregation();
        return aggregation instanceof SqlJsonObjectAggAggFunction
                || aggregation instanceof SqlJsonArrayAggAggFunction;
    }

    // ---------------------------------------------------------------------------------------------

    /** Configuration for {@link WrapJsonAggFunctionArgumentsRule}. */
    public interface Config extends RelRule.Config {

        @Override
        default RelOptRule toRule() {
            return new WrapJsonAggFunctionArgumentsRule(this);
        }

        default Config onJsonAggregateFunctions() {
            final Predicate<LogicalAggregate> jsonAggPredicate =
                    aggregate ->
                            aggregate.getAggCallList().stream()
                                    .anyMatch(WrapJsonAggFunctionArgumentsRule::isJsonAggregation);

            final RelRule.OperandTransform aggTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(LogicalAggregate.class)
                                    .predicate(jsonAggPredicate)
                                    .anyInputs();

            return withOperandSupplier(aggTransform).as(Config.class);
        }
    }
}
