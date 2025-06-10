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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
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
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtils;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtils.FunctionParam;
import org.apache.flink.table.planner.plan.utils.MLPredictUtils;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDescriptorOperator;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
        return new StreamExecMLPredictTableFunction(
                ShortcutUtils.unwrapTableConfig(this),
                buildMLPredictSpec(),
                buildModelSpec(modelCall),
                buildAsyncOptions(modelCall),
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

    private MLPredictSpec buildMLPredictSpec() {
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
                                        return new FunctionCallUtils.FieldRef(index);
                                    } else {
                                        throw new TableException(
                                                String.format(
                                                        "Unknown operand for descriptor operator: %s.",
                                                        operand));
                                    }
                                })
                        .collect(Collectors.toList());
        return new MLPredictSpec(features, Collections.emptyMap());
    }

    private ModelSpec buildModelSpec(RexModelCall modelCall) {
        ModelSpec modelSpec = new ModelSpec(modelCall.getContextResolvedModel());
        modelSpec.setModelProvider(modelCall.getModelProvider());
        return modelSpec;
    }

    private FunctionCallUtils.AsyncOptions buildAsyncOptions(RexModelCall modelCall) {
        boolean isAsyncEnabled = isAsyncMLPredict(modelCall.getModelProvider());
        if (isAsyncEnabled) {
            return MLPredictUtils.getMergedMLPredictAsyncOptions(
                    // TODO: extract runtime config
                    Collections.emptyMap(),
                    ShortcutUtils.unwrapTableConfig(getCluster()),
                    getInputChangelogMode(getInput()));
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T extractOperand(Predicate<RexNode> predicate) {
        return (T)
                ((RexCall) scan.getCall())
                        .getOperands().stream()
                                .filter(predicate)
                                .findFirst()
                                .orElseThrow(
                                        () ->
                                                new TableException(
                                                        String.format(
                                                                "MLPredict doesn't contain specified operand: %s",
                                                                scan.getCall().toString())));
    }

    private boolean isAsyncMLPredict(ModelProvider provider) {
        boolean syncFound = false;
        boolean asyncFound = false;
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
        return asyncFound;
    }

    private ChangelogMode getInputChangelogMode(RelNode rel) {
        if (rel instanceof StreamPhysicalRel) {
            return JavaScalaConversionUtil.toJava(
                            ChangelogPlanUtils.getChangelogMode((StreamPhysicalRel) rel))
                    .orElse(ChangelogMode.insertOnly());
        } else if (rel instanceof HepRelVertex) {
            return getInputChangelogMode(((HepRelVertex) rel).getCurrentRel());
        } else {
            return ChangelogMode.insertOnly();
        }
    }
}
