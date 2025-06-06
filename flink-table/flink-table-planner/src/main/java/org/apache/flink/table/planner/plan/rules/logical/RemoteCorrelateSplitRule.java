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

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamPhysicalCorrelateRule;
import org.apache.flink.table.planner.plan.utils.RexDefaultVisitor;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.immutables.value.Value;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;

/**
 * Rule will split the Remote {@link FlinkLogicalTableFunctionScan} with Java calls or the Java
 * {@link FlinkLogicalTableFunctionScan} with Remote calls into a {@link FlinkLogicalCalc} which
 * will be the left input of the new {@link FlinkLogicalCorrelate} and a new {@link
 * FlinkLogicalTableFunctionScan}.
 */
@Value.Enclosing
public class RemoteCorrelateSplitRule extends RelRule<RemoteCorrelateSplitRule.Config> {
    private final RemoteCalcCallFinder callFinder;

    RemoteCorrelateSplitRule(Config config, RemoteCalcCallFinder callFinder) {
        super(config);
        this.callFinder = callFinder;
    }

    private FlinkLogicalTableFunctionScan createNewScan(
            FlinkLogicalTableFunctionScan scan, ScalarFunctionSplitter splitter) {
        RexCall rightRexCall = (RexCall) scan.getCall();
        // extract Java funcs from Remote TableFunction or Remote funcs from Java TableFunction.
        List<RexNode> rightCalcProjects =
                rightRexCall.getOperands().stream()
                        .map(x -> x.accept(splitter))
                        .collect(Collectors.toList());

        RexCall newRightRexCall = rightRexCall.clone(rightRexCall.getType(), rightCalcProjects);
        return new FlinkLogicalTableFunctionScan(
                scan.getCluster(),
                scan.getTraitSet(),
                scan.getInputs(),
                newRightRexCall,
                scan.getElementType(),
                scan.getRowType(),
                scan.getColumnMappings());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalCorrelate correlate = call.rel(0);
        RelNode right = ((HepRelVertex) correlate.getRight()).getCurrentRel();
        FlinkLogicalTableFunctionScan tableFunctionScan;
        if (right instanceof FlinkLogicalTableFunctionScan) {
            tableFunctionScan = (FlinkLogicalTableFunctionScan) right;
        } else if (right instanceof FlinkLogicalCalc) {
            tableFunctionScan = StreamPhysicalCorrelateRule.getTableScan((FlinkLogicalCalc) right);
        } else {
            return false;
        }
        RexNode rexNode = tableFunctionScan.getCall();
        if (rexNode instanceof RexCall) {
            return (callFinder.isRemoteCall(rexNode) && callFinder.containsNonRemoteCall(rexNode))
                    || (callFinder.isNonRemoteCall(rexNode)
                            && callFinder.containsRemoteCall(rexNode))
                    || (callFinder.isRemoteCall(rexNode) && RexUtil.containsFieldAccess(rexNode));
        }
        return false;
    }

    private List<String> createNewFieldNames(
            RelDataType rowType,
            RexBuilder rexBuilder,
            int primitiveFieldCount,
            ArrayBuffer<RexNode> extractedRexNodes,
            List<RexNode> calcProjects) {
        for (int i = 0; i < primitiveFieldCount; i++) {
            calcProjects.add(RexInputRef.of(i, rowType));
        }
        // change RexCorrelVariable to RexInputRef.
        RexDefaultVisitor<RexNode> visitor =
                new RexDefaultVisitor<RexNode>() {
                    @Override
                    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                        RexNode expr = fieldAccess.getReferenceExpr();
                        if (expr instanceof RexCorrelVariable) {
                            RelDataTypeField field = fieldAccess.getField();
                            return new RexInputRef(field.getIndex(), field.getType());
                        } else {
                            return rexBuilder.makeFieldAccess(
                                    expr.accept(this), fieldAccess.getField().getIndex());
                        }
                    }

                    @Override
                    public RexNode visitCall(RexCall call) {
                        List<RexNode> newProjects =
                                call.getOperands().stream()
                                        .map(x -> x.accept(this))
                                        .collect(Collectors.toList());
                        return rexBuilder.makeCall(call.getOperator(), newProjects);
                    }

                    @Override
                    public RexNode visitNode(RexNode rexNode) {
                        return rexNode;
                    }
                };
        // add the fields of the extracted rex calls.
        Iterator<RexNode> iterator = extractedRexNodes.iterator();
        while (iterator.hasNext()) {
            RexNode rexNode = iterator.next();
            if (rexNode instanceof RexCall) {
                RexCall rexCall = (RexCall) rexNode;
                List<RexNode> newProjects =
                        rexCall.getOperands().stream()
                                .map(x -> x.accept(visitor))
                                .collect(Collectors.toList());
                RexCall newRexCall = rexCall.clone(rexCall.getType(), newProjects);
                calcProjects.add(newRexCall);
            } else {
                calcProjects.add(rexNode);
            }
        }

        List<String> nameList = new LinkedList<>();
        for (int i = 0; i < primitiveFieldCount; i++) {
            nameList.add(rowType.getFieldNames().get(i));
        }
        Iterator<Object> indicesIterator = extractedRexNodes.indices().iterator();
        while (indicesIterator.hasNext()) {
            nameList.add("f" + indicesIterator.next());
        }
        return SqlValidatorUtil.uniquify(
                nameList, rexBuilder.getTypeFactory().getTypeSystem().isSchemaCaseSensitive());
    }

    private FlinkLogicalCalc createNewLeftCalc(
            RelNode left,
            RexBuilder rexBuilder,
            ArrayBuffer<RexNode> extractedRexNodes,
            FlinkLogicalCorrelate correlate) {
        // add the fields of the primitive left input.
        List<RexNode> leftCalcProjects = new LinkedList<>();
        RelDataType leftRowType = left.getRowType();
        List<String> leftCalcCalcFieldNames =
                createNewFieldNames(
                        leftRowType,
                        rexBuilder,
                        leftRowType.getFieldCount(),
                        extractedRexNodes,
                        leftCalcProjects);

        // create a new calc
        return new FlinkLogicalCalc(
                correlate.getCluster(),
                correlate.getTraitSet(),
                left,
                RexProgram.create(
                        leftRowType, leftCalcProjects, null, leftCalcCalcFieldNames, rexBuilder));
    }

    private FlinkLogicalCalc createTopCalc(
            int primitiveLeftFieldCount,
            RexBuilder rexBuilder,
            ArrayBuffer<RexNode> extractedRexNodes,
            RelDataType calcRowType,
            FlinkLogicalCorrelate newCorrelate) {
        RexProgram rexProgram =
                new RexProgramBuilder(newCorrelate.getRowType(), rexBuilder).getProgram();
        int offset = extractedRexNodes.size() + primitiveLeftFieldCount;

        // extract correlate output RexNode.
        List<RexNode> newTopCalcProjects =
                rexProgram.getExprList().stream()
                        .filter(x -> x instanceof RexInputRef)
                        .filter(
                                x -> {
                                    int index = ((RexInputRef) x).getIndex();
                                    return index < primitiveLeftFieldCount || index >= offset;
                                })
                        .collect(Collectors.toList());

        return new FlinkLogicalCalc(
                newCorrelate.getCluster(),
                newCorrelate.getTraitSet(),
                newCorrelate,
                RexProgram.create(
                        newCorrelate.getRowType(),
                        newTopCalcProjects,
                        null,
                        calcRowType,
                        rexBuilder));
    }

    private ScalarFunctionSplitter createScalarFunctionSplitter(
            RexBuilder rexBuilder,
            int primitiveLeftFieldCount,
            ArrayBuffer<RexNode> extractedRexNodes,
            RexNode tableFunctionNode) {
        return new ScalarFunctionSplitter(
                // The scan should not contain any local references to resolve, so null is passed.
                null,
                rexBuilder,
                primitiveLeftFieldCount,
                extractedRexNodes,
                node -> {
                    if (callFinder.isNonRemoteCall(tableFunctionNode)) {
                        // splits the RexCalls which contain Remote functions into separate node
                        return callFinder.isRemoteCall(node);
                    } else if (callFinder.containsNonRemoteCall(node)) {
                        // splits the RexCalls which contain non-Remote functions into separate node
                        return callFinder.isNonRemoteCall(node);
                    } else {
                        // splits the RexFieldAccesses which contain non-Remote functions into
                        // separate node
                        return node instanceof RexFieldAccess;
                    }
                },
                callFinder);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalCorrelate correlate = call.rel(0);
        RexBuilder rexBuilder = call.builder().getRexBuilder();
        RelNode left = ((HepRelVertex) correlate.getLeft()).getCurrentRel();
        RelNode right = ((HepRelVertex) correlate.getRight()).getCurrentRel();
        int primitiveLeftFieldCount = left.getRowType().getFieldCount();
        ArrayBuffer<RexNode> extractedRexNodes = new ArrayBuffer<>();

        RelNode rightNewInput;
        if (right instanceof FlinkLogicalTableFunctionScan) {
            FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) right;
            rightNewInput =
                    createNewScan(
                            scan,
                            createScalarFunctionSplitter(
                                    rexBuilder,
                                    primitiveLeftFieldCount,
                                    extractedRexNodes,
                                    scan.getCall()));
        } else {
            FlinkLogicalCalc calc = (FlinkLogicalCalc) right;
            FlinkLogicalTableFunctionScan scan = StreamPhysicalCorrelateRule.getTableScan(calc);
            FlinkLogicalCalc mergedCalc = StreamPhysicalCorrelateRule.getMergedCalc(calc);
            FlinkLogicalTableFunctionScan newScan =
                    createNewScan(
                            scan,
                            createScalarFunctionSplitter(
                                    rexBuilder,
                                    primitiveLeftFieldCount,
                                    extractedRexNodes,
                                    scan.getCall()));
            rightNewInput =
                    mergedCalc.copy(mergedCalc.getTraitSet(), newScan, mergedCalc.getProgram());
        }

        FlinkLogicalCorrelate newCorrelate;
        if (!extractedRexNodes.isEmpty()) {
            FlinkLogicalCalc leftCalc =
                    createNewLeftCalc(left, rexBuilder, extractedRexNodes, correlate);

            newCorrelate =
                    new FlinkLogicalCorrelate(
                            correlate.getCluster(),
                            correlate.getTraitSet(),
                            leftCalc,
                            rightNewInput,
                            correlate.getCorrelationId(),
                            correlate.getRequiredColumns(),
                            correlate.getJoinType());
        } else {
            newCorrelate =
                    new FlinkLogicalCorrelate(
                            correlate.getCluster(),
                            correlate.getTraitSet(),
                            left,
                            rightNewInput,
                            correlate.getCorrelationId(),
                            correlate.getRequiredColumns(),
                            correlate.getJoinType());
        }

        FlinkLogicalCalc newTopCalc =
                createTopCalc(
                        primitiveLeftFieldCount,
                        rexBuilder,
                        extractedRexNodes,
                        correlate.getRowType(),
                        newCorrelate);

        call.transformTo(newTopCalc);
    }

    // Consider the rules to be equal if they are the same class and their call finders are the same
    // class.
    @Override
    public boolean equals(Object object) {
        if (object == null || !object.getClass().equals(RemoteCorrelateSplitRule.class)) {
            return false;
        }
        RemoteCorrelateSplitRule rule = (RemoteCorrelateSplitRule) object;
        return callFinder.equals(rule.callFinder);
    }

    @Override
    public int hashCode() {
        return callFinder.hashCode();
    }

    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        public abstract RemoteCalcCallFinder callFinder();

        static RemoteCorrelateSplitRule.Config createDefault(RemoteCalcCallFinder callFinder) {
            return ImmutableRemoteCorrelateSplitRule.Config.builder()
                    .callFinder(callFinder)
                    .build()
                    .withOperandSupplier(b0 -> b0.operand(FlinkLogicalCorrelate.class).anyInputs());
        }

        @Override
        default RelOptRule toRule() {
            return new RemoteCorrelateSplitRule(
                    this.withDescription("RemoteCorrelateSplitRule-" + callFinder().getName())
                            .as(Config.class),
                    callFinder());
        }
    }
}
