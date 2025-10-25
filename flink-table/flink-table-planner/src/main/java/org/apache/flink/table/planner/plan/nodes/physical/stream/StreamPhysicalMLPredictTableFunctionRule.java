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

package org.apache.flink.table.planner.plan.nodes.physical.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.MLPredictRuntimeConfigOptions;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.PredictRuntimeProvider;
import org.apache.flink.table.planner.calcite.RexModelCall;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.MapSqlType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.sql.SqlKind.MAP_VALUE_CONSTRUCTOR;
import static org.apache.flink.table.api.config.MLPredictRuntimeConfigOptions.ASYNC;
import static org.apache.flink.table.api.config.MLPredictRuntimeConfigOptions.ASYNC_MAX_CONCURRENT_OPERATIONS;
import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.CHARACTER_STRING;

/**
 * Rule to convert a {@link FlinkLogicalTableFunctionScan} with ml_predict call into a {@link
 * StreamPhysicalMLPredictTableFunction}.
 */
public class StreamPhysicalMLPredictTableFunctionRule extends ConverterRule {

    private static final String CONFIG_ERROR_MESSAGE =
            "Config parameter of ML_PREDICT function should be a MAP data type consisting String literals.";

    public static final StreamPhysicalMLPredictTableFunctionRule INSTANCE =
            new StreamPhysicalMLPredictTableFunctionRule(
                    Config.INSTANCE.withConversion(
                            FlinkLogicalTableFunctionScan.class,
                            FlinkConventions.LOGICAL(),
                            FlinkConventions.STREAM_PHYSICAL(),
                            "StreamPhysicalModelTableFunctionRule"));

    private StreamPhysicalMLPredictTableFunctionRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalTableFunctionScan scan = call.rel(0);
        final RexCall rexCall = (RexCall) scan.getCall();
        final FunctionDefinition definition = ShortcutUtils.unwrapFunctionDefinition(rexCall);
        if (!isMLPredictFunction(definition)) {
            return false;
        }

        final RexModelCall modelCall = (RexModelCall) rexCall.getOperands().get(1);
        return modelCall.getModelProvider() instanceof PredictRuntimeProvider
                || modelCall.getModelProvider() instanceof AsyncPredictRuntimeProvider;
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        final FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) rel;
        final RelNode newInput =
                RelOptRule.convert(scan.getInput(0), FlinkConventions.STREAM_PHYSICAL());

        final RelTraitSet providedTraitSet =
                rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());

        // Extract and validate configuration from the 4th operand if present
        final RexCall rexCall = (RexCall) scan.getCall();
        final Map<String, String> runtimeConfig = buildRuntimeConfig(rexCall);

        return new StreamPhysicalMLPredictTableFunction(
                scan.getCluster(),
                providedTraitSet,
                newInput,
                scan,
                scan.getRowType(),
                runtimeConfig);
    }

    public static boolean isMLPredictFunction(FunctionDefinition definition) {
        return definition instanceof BuiltInFunctionDefinition
                && BuiltInFunctionDefinitions.ML_PREDICT
                        .getName()
                        .equals(((BuiltInFunctionDefinition) definition).getName());
    }

    /**
     * Builds runtime configuration map from the 4th operand of rexCall. The operand should be a map
     * of string literals. Similar to checkConfig in SqlMLTableFunction.
     */
    private static Map<String, String> buildRuntimeConfig(RexCall rexCall) {
        final List<RexNode> operands = rexCall.getOperands();

        // Check if we have the 4th operand (config parameter)
        if (operands.size() < 4) {
            return Collections.emptyMap();
        }

        final RexNode configOperand = operands.get(3);
        if (configOperand.getKind() == SqlKind.DEFAULT) {
            // If the config operand is DEFAULT, return an empty map
            return Collections.emptyMap();
        }

        // Check if the operand is a MAP_VALUE_CONSTRUCTOR
        if (configOperand.getKind() != MAP_VALUE_CONSTRUCTOR) {
            throw new ValidationException(CONFIG_ERROR_MESSAGE);
        }

        if (!(configOperand instanceof RexCall)) {
            throw new ValidationException(CONFIG_ERROR_MESSAGE);
        }

        final RexCall mapConstructorCall = (RexCall) configOperand;
        final RelDataType mapType = mapConstructorCall.getType();
        if (!(mapType instanceof MapSqlType)) {
            throw new ValidationException(CONFIG_ERROR_MESSAGE);
        }

        LogicalType keyType = toLogicalType(mapType.getKeyType());
        LogicalType valueType = toLogicalType(mapType.getValueType());
        if (!keyType.is(CHARACTER_STRING) || !valueType.is(CHARACTER_STRING)) {
            throw new ValidationException(CONFIG_ERROR_MESSAGE);
        }

        final Map<String, String> runtimeConfig = FunctionCallUtil.convert(mapConstructorCall);

        // Validate the configuration values
        validateRuntimeConfig(runtimeConfig);

        return runtimeConfig;
    }

    /**
     * Validates the runtime configuration values. Similar to checkConfigValue in
     * SqlMLTableFunction.
     */
    private static void validateRuntimeConfig(Map<String, String> runtimeConfig) {
        final Configuration config = Configuration.fromMap(runtimeConfig);

        try {
            MLPredictRuntimeConfigOptions.getSupportedOptions().forEach(config::get);
        } catch (Throwable t) {
            throw new ValidationException("Failed to parse the config.", t);
        }

        // Option value check
        // async options are all optional
        Boolean async = config.get(ASYNC);
        if (Boolean.TRUE.equals(async)) {
            Integer maxConcurrentOperations = config.get(ASYNC_MAX_CONCURRENT_OPERATIONS);
            if (maxConcurrentOperations != null && maxConcurrentOperations <= 0) {
                throw new ValidationException(
                        String.format(
                                "Invalid runtime config option '%s'. Its value should be positive integer but was %s.",
                                ASYNC_MAX_CONCURRENT_OPERATIONS.key(), maxConcurrentOperations));
            }
        }
    }
}
