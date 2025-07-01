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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.MLPredictRuntimeConfigOptions;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.ModelProvider;
import org.apache.flink.table.ml.PredictRuntimeProvider;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexModelCall;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.MLPredictSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.ModelSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMLPredictTableFunction;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil.FunctionParam;
import org.apache.flink.table.planner.plan.utils.MLPredictUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDescriptorOperator;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.SqlKind.MAP_VALUE_CONSTRUCTOR;

/** Stream physical RelNode for ml predict table function. */
public class StreamPhysicalMLPredictTableFunction extends SingleRel implements StreamPhysicalRel {

    private final RelDataType outputRowType;
    private final FlinkLogicalTableFunctionScan scan;

    public StreamPhysicalMLPredictTableFunction(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode inputRel,
            FlinkLogicalTableFunctionScan scan,
            RelDataType outputRowType) {
        super(cluster, traits, inputRel);
        this.scan = scan;
        this.outputRowType = outputRowType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new StreamPhysicalMLPredictTableFunction(
                getCluster(), traitSet, inputs.get(0), scan, getRowType());
    }

    @Override
    public boolean requireWatermark() {
        return false;
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        RexModelCall modelCall = extractOperand(operand -> operand instanceof RexModelCall);
        Map<String, String> runtimeConfig = buildRuntimeConfig();
        return new StreamExecMLPredictTableFunction(
                ShortcutUtils.unwrapTableConfig(this),
                buildMLPredictSpec(runtimeConfig),
                buildModelSpec(modelCall),
                buildAsyncOptions(modelCall, runtimeConfig),
                InputProperty.DEFAULT,
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription());
    }

    @Override
    protected RelDataType deriveRowType() {
        return outputRowType;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("invocation", scan.getCall())
                .item("rowType", getRowType());
    }

    private MLPredictSpec buildMLPredictSpec(Map<String, String> runtimeConfig) {
        RexTableArgCall tableCall = extractOperand(operand -> operand instanceof RexTableArgCall);
        RexCall descriptorCall =
                extractOperand(
                        operand ->
                                operand instanceof RexCall
                                        && ((RexCall) operand).getOperator()
                                                instanceof SqlDescriptorOperator);
        Map<String, Integer> column2Index = new HashMap<>();
        List<String> fieldNames = tableCall.getType().getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            column2Index.put(fieldNames.get(i), i);
        }
        List<FunctionParam> features =
                descriptorCall.getOperands().stream()
                        .map(
                                operand -> {
                                    if (operand instanceof RexLiteral) {
                                        RexLiteral literal = (RexLiteral) operand;
                                        String fieldName = RexLiteral.stringValue(literal);
                                        Integer index = column2Index.get(fieldName);
                                        if (index == null) {
                                            throw new TableException(
                                                    String.format(
                                                            "Field %s is not found in input schema: %s.",
                                                            fieldName, tableCall.getType()));
                                        }
                                        return new FunctionCallUtil.FieldRef(index);
                                    } else {
                                        throw new TableException(
                                                String.format(
                                                        "Unknown operand for descriptor operator: %s.",
                                                        operand));
                                    }
                                })
                        .collect(Collectors.toList());
        return new MLPredictSpec(features, runtimeConfig);
    }

    private ModelSpec buildModelSpec(RexModelCall modelCall) {
        ModelSpec modelSpec = new ModelSpec(modelCall.getContextResolvedModel());
        modelSpec.setModelProvider(modelCall.getModelProvider());
        return modelSpec;
    }

    private @Nullable FunctionCallUtil.AsyncOptions buildAsyncOptions(
            RexModelCall modelCall, Map<String, String> runtimeConfig) {
        boolean isAsyncEnabled = isAsyncMLPredict(modelCall.getModelProvider(), runtimeConfig);
        if (isAsyncEnabled) {
            return MLPredictUtil.getMergedMLPredictAsyncOptions(
                    runtimeConfig, ShortcutUtils.unwrapTableConfig(getCluster()));
        } else {
            return null;
        }
    }

    private Map<String, String> buildRuntimeConfig() {
        Optional<RexCall> optionalMapConstructor =
                extractOptionalOperand(operand -> operand.getKind() == MAP_VALUE_CONSTRUCTOR);
        if (optionalMapConstructor.isEmpty()) {
            return Collections.emptyMap();
        }
        RexCall mapConstructor = optionalMapConstructor.get();
        Map<String, String> reducedConfig = new HashMap<>();
        assert mapConstructor.getOperands().size() % 2 == 0;
        for (int i = 0; i < mapConstructor.getOperands().size(); i += 2) {
            String key = RexLiteral.stringValue(mapConstructor.getOperands().get(i));
            String value = RexLiteral.stringValue(mapConstructor.getOperands().get(i + 1));
            reducedConfig.put(key, value);
        }
        return reducedConfig;
    }

    @SuppressWarnings("unchecked")
    private <T> Optional<T> extractOptionalOperand(Predicate<RexNode> predicate) {
        return (Optional<T>)
                ((RexCall) scan.getCall()).getOperands().stream().filter(predicate).findFirst();
    }

    @SuppressWarnings("unchecked")
    private <T> T extractOperand(Predicate<RexNode> predicate) {
        return (T)
                extractOptionalOperand(predicate)
                        .orElseThrow(
                                () ->
                                        new TableException(
                                                String.format(
                                                        "MLPredict doesn't contain specified operand: %s",
                                                        scan.getCall().toString())));
    }

    private boolean isAsyncMLPredict(ModelProvider provider, Map<String, String> runtimeConfig) {
        boolean syncFound = false;
        boolean asyncFound = false;
        Optional<Boolean> requiredMode =
                Configuration.fromMap(runtimeConfig)
                        .getOptional(MLPredictRuntimeConfigOptions.ASYNC);

        if (provider instanceof PredictRuntimeProvider) {
            syncFound = true;
        }
        if (provider instanceof AsyncPredictRuntimeProvider) {
            asyncFound = true;
        }

        if (!syncFound && !asyncFound) {
            throw new TableException(
                    String.format(
                            "Unknown model provider found: %s.", provider.getClass().getName()));
        }

        if (requiredMode.isEmpty()) {
            return asyncFound;
        } else if (requiredMode.get()) {
            if (!asyncFound) {
                throw new TableException(
                        String.format(
                                "Require async mode, but model provider %s doesn't support async mode.",
                                provider.getClass().getName()));
            }
            return true;
        } else {
            if (!syncFound) {
                throw new TableException(
                        String.format(
                                "Require sync mode, but model provider %s doesn't support sync mode.",
                                provider.getClass().getName()));
            }
            return false;
        }
    }
}
