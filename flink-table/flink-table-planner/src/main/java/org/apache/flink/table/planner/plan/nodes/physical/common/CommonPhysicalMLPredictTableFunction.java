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

package org.apache.flink.table.planner.plan.nodes.physical.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.MLPredictRuntimeConfigOptions;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.ModelProvider;
import org.apache.flink.table.ml.PredictRuntimeProvider;
import org.apache.flink.table.planner.calcite.RexModelCall;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.plan.nodes.exec.spec.MLPredictSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.ModelSpec;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Common physical node for {@code ML_PREDICT}. */
public abstract class CommonPhysicalMLPredictTableFunction extends SingleRel
        implements FlinkPhysicalRel {

    protected final RelDataType outputRowType;
    protected final FlinkLogicalTableFunctionScan scan;
    protected final Map<String, String> runtimeConfig;

    protected CommonPhysicalMLPredictTableFunction(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode inputRel,
            FlinkLogicalTableFunctionScan scan,
            RelDataType outputRowType,
            Map<String, String> runtimeConfig) {
        super(cluster, traits, inputRel);
        this.scan = scan;
        this.outputRowType = outputRowType;
        this.runtimeConfig = runtimeConfig;
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

    public RexNode getMLPredictCall() {
        return scan.getCall();
    }

    protected MLPredictSpec buildMLPredictSpec(Map<String, String> runtimeConfig) {
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

    protected ModelSpec buildModelSpec(RexModelCall modelCall) {
        ModelSpec modelSpec = new ModelSpec(modelCall.getContextResolvedModel());
        modelSpec.setModelProvider(modelCall.getModelProvider());
        return modelSpec;
    }

    protected @Nullable FunctionCallUtil.AsyncOptions buildAsyncOptions(
            RexModelCall modelCall, Map<String, String> runtimeConfig) {
        boolean isAsyncEnabled = isAsyncMLPredict(modelCall.getModelProvider(), runtimeConfig);
        if (isAsyncEnabled) {
            return MLPredictUtil.getMergedMLPredictAsyncOptions(
                    runtimeConfig, ShortcutUtils.unwrapTableConfig(getCluster()));
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> Optional<T> extractOptionalOperand(Predicate<RexNode> predicate) {
        return (Optional<T>)
                ((RexCall) scan.getCall()).getOperands().stream().filter(predicate).findFirst();
    }

    @SuppressWarnings("unchecked")
    protected <T> T extractOperand(Predicate<RexNode> predicate) {
        return (T)
                extractOptionalOperand(predicate)
                        .orElseThrow(
                                () ->
                                        new TableException(
                                                String.format(
                                                        "MLPredict doesn't contain specified operand: %s",
                                                        scan.getCall().toString())));
    }

    protected boolean isAsyncMLPredict(ModelProvider provider, Map<String, String> runtimeConfig) {
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
