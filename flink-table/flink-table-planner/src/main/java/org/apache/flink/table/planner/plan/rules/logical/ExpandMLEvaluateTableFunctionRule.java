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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.PredictRuntimeProvider;
import org.apache.flink.table.ml.TaskType;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexModelCall;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.sql.ml.MLEvaluationAggregationFunction;
import org.apache.flink.table.planner.functions.sql.ml.SqlMLEvaluateTableFunction;
import org.apache.flink.table.planner.functions.sql.ml.SqlMLPredictTableFunction;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.NlsString;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Rule that expands ML evaluation table function calls.
 *
 * <p>This rule matches {@link FlinkLogicalTableFunctionScan} with a {@link
 * SqlMLEvaluateTableFunction} call and expands it into ml predict table function and an aggregation
 * function following it.
 */
@Internal
@Value.Enclosing
public class ExpandMLEvaluateTableFunctionRule
        extends RelRule<ExpandMLEvaluateTableFunctionRule.Config> {

    public static final RelOptRule INSTANCE = new ExpandMLEvaluateTableFunctionRule(Config.DEFAULT);

    public ExpandMLEvaluateTableFunctionRule(Config config) {
        super(config);
    }

    private static RexCall getConfigMap(RexCall rexCall) {
        if (rexCall.getOperands().size() > 5) {
            return (RexCall) rexCall.getOperands().get(5);
        }
        if (rexCall.getOperands().size() > 4) {
            RexNode node = rexCall.getOperands().get(4);
            if (node instanceof RexCall
                    && ((RexCall) node).getOperator().getKind() == SqlKind.MAP_VALUE_CONSTRUCTOR) {
                return (RexCall) node;
            }
        }
        return null;
    }

    private static String getTask(RexCall rexCall) {
        final RexModelCall modelCall = (RexModelCall) rexCall.getOperands().get(1);
        final RexNode taskNode = rexCall.getOperands().get(4);
        String task = null;
        if (taskNode instanceof RexLiteral) {
            task = ((RexLiteral) taskNode).getValueAs(NlsString.class).getValue();
            if (task == null || task.isEmpty()) {
                task = null;
            }
        }
        if (task == null) {
            throw new ValidationException(
                    "Task type must be specified in the model options or as a parameter to the ML_EVALUATE function.");
        }
        TaskType.throwOrReturnInvalidTaskType(task, true);
        return task;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalTableFunctionScan scan = call.rel(0);
        final RelDataType resultType = scan.getRowType();
        final RelBuilder relBuilder = call.builder().push(scan.getInput(0));

        final RexCall rexCall = (RexCall) scan.getCall();

        RelDataType predictOutputType = addPredictTableFunction(relBuilder, rexCall);
        addProjection(relBuilder, rexCall, predictOutputType);
        addAggregate(relBuilder, rexCall, resultType);

        call.transformTo(relBuilder.build());
    }

    private void addAggregate(RelBuilder relBuilder, RexCall rexCall, RelDataType resultType) {
        final String task = getTask(rexCall);
        final MLEvaluationAggregationFunction aggregationFunction =
                new MLEvaluationAggregationFunction(task);
        final FlinkContext context = ShortcutUtils.unwrapContext(relBuilder.getCluster());
        final FlinkTypeFactory typeFactory =
                ShortcutUtils.unwrapTypeFactory(relBuilder.getCluster());
        relBuilder.aggregate(
                relBuilder.groupKey(),
                List.of(
                        AggregateCall.create(
                                BridgingSqlAggFunction.of(
                                        context,
                                        typeFactory,
                                        ContextResolvedFunction.temporary(
                                                FunctionIdentifier.of("ml_evaluate"),
                                                aggregationFunction)),
                                false,
                                false,
                                false,
                                List.of(0, 1),
                                -1,
                                null,
                                RelCollations.EMPTY,
                                resultType.getFieldList().get(0).getType(),
                                "result")));
    }

    private void addProjection(
            RelBuilder relBuilder, RexCall rexCall, RelDataType predictOutputType) {
        final RexCall labelDescriptor = (RexCall) rexCall.getOperands().get(2);
        final String labelCol =
                ((RexLiteral) labelDescriptor.getOperands().get(0))
                        .getValueAs(NlsString.class)
                        .getValue();

        // Project the label column and the last column (prediction). Only one label and prediction
        // column is expected. Validation is done in SqlMLEvaluateTableFunction.
        final List<RexNode> projectFields =
                predictOutputType.getFieldList().stream()
                        .filter(
                                field ->
                                        field.getName().equals(labelCol)
                                                || field.getIndex()
                                                        == predictOutputType.getFieldCount() - 1)
                        .map(field -> relBuilder.field(field.getIndex()))
                        .collect(Collectors.toList());
        relBuilder.project(projectFields);
    }

    private RelDataType addPredictTableFunction(RelBuilder relBuilder, RexCall rexCall) {
        final RexCall tableArg = (RexCall) rexCall.getOperands().get(0);
        final RexCall modelCall = (RexCall) rexCall.getOperands().get(1);
        final RexCall featuresDescriptor = (RexCall) rexCall.getOperands().get(3);

        // Get optional config map if present
        final RexCall configMap = getConfigMap(rexCall);

        final List<RexNode> predictOperands = new ArrayList<>();
        predictOperands.add(tableArg);
        predictOperands.add(modelCall);
        predictOperands.add(featuresDescriptor);
        if (configMap != null) {
            predictOperands.add(configMap);
        }

        final RexCall predictCall =
                (RexCall)
                        relBuilder
                                .getRexBuilder()
                                .makeCall(new SqlMLPredictTableFunction(), predictOperands);

        RexCallBinding callBinding =
                new RexCallBinding(
                        relBuilder.getTypeFactory(),
                        predictCall.getOperator(),
                        predictOperands,
                        List.of());

        RelDataType predictReturnType =
                ((SqlMLPredictTableFunction) predictCall.getOperator())
                        .getRowTypeInference()
                        .inferReturnType(callBinding);

        relBuilder.push(
                LogicalTableFunctionScan.create(
                        relBuilder.getCluster(),
                        List.of(relBuilder.build()),
                        predictCall,
                        null,
                        predictReturnType,
                        Collections.emptySet()));

        return predictReturnType;
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                ImmutableExpandMLEvaluateTableFunctionRule.Config.builder()
                        .build()
                        .withDescription("ExpandMLEvaluateTableFunctionRule")
                        .as(Config.class)
                        .onMLEvaluateFunction();

        @Override
        default RelOptRule toRule() {
            return new ExpandMLEvaluateTableFunctionRule(this);
        }

        default Config onMLEvaluateFunction() {
            final RelRule.OperandTransform scanTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(LogicalTableFunctionScan.class)
                                    .predicate(
                                            scan -> {
                                                if (!(scan.getCall() instanceof RexCall)) {
                                                    return false;
                                                }
                                                RexCall call = (RexCall) scan.getCall();
                                                if (!(call.getOperator()
                                                        instanceof SqlMLEvaluateTableFunction)) {
                                                    return false;
                                                }
                                                final RexModelCall modelCall =
                                                        (RexModelCall) call.getOperands().get(1);
                                                return modelCall.getModelProvider()
                                                                instanceof PredictRuntimeProvider
                                                        || modelCall.getModelProvider()
                                                                instanceof
                                                                AsyncPredictRuntimeProvider;
                                            })
                                    .anyInputs();

            return withOperandSupplier(scanTransform).as(Config.class);
        }
    }
}
