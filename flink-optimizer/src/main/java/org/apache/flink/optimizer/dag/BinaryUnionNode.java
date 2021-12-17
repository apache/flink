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

package org.apache.flink.optimizer.dag;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.Union;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.costs.CostEstimator;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.InterestingProperties;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.operators.BinaryUnionOpDescriptor;
import org.apache.flink.optimizer.operators.OperatorDescriptorDual;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.NamedChannel;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/** The Optimizer representation of a binary <i>Union</i>. */
public class BinaryUnionNode extends TwoInputNode {

    private Set<RequestedGlobalProperties> channelProps;

    public BinaryUnionNode(Union<?> union) {
        super(union);
    }

    @Override
    public void addOutgoingConnection(DagConnection connection) {
        // ensure that union nodes have not more than one outgoing connection.
        if (this.getOutgoingConnections() != null && this.getOutgoingConnections().size() > 0) {
            throw new CompilerException(
                    "BinaryUnionNode may only have a single outgoing connection.");
        }
        super.addOutgoingConnection(connection);
    }

    @Override
    public String getOperatorName() {
        return "Union";
    }

    @Override
    protected List<OperatorDescriptorDual> getPossibleProperties() {
        return Collections.emptyList();
    }

    @Override
    protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
        long card1 = getFirstPredecessorNode().getEstimatedNumRecords();
        long card2 = getSecondPredecessorNode().getEstimatedNumRecords();
        this.estimatedNumRecords = (card1 < 0 || card2 < 0) ? -1 : card1 + card2;

        long size1 = getFirstPredecessorNode().getEstimatedOutputSize();
        long size2 = getSecondPredecessorNode().getEstimatedOutputSize();
        this.estimatedOutputSize = (size1 < 0 || size2 < 0) ? -1 : size1 + size2;
    }

    @Override
    public void computeUnionOfInterestingPropertiesFromSuccessors() {
        super.computeUnionOfInterestingPropertiesFromSuccessors();
        // clear all local properties, as they are destroyed anyways
        getInterestingProperties().getLocalProperties().clear();
    }

    @Override
    public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
        final InterestingProperties props = getInterestingProperties();

        // if no other properties exist, add the pruned trivials back
        if (props.getGlobalProperties().isEmpty()) {
            props.addGlobalProperties(new RequestedGlobalProperties());
        }
        props.addLocalProperties(new RequestedLocalProperties());
        this.input1.setInterestingProperties(props.clone());
        this.input2.setInterestingProperties(props.clone());

        this.channelProps = props.getGlobalProperties();
    }

    @Override
    public List<PlanNode> getAlternativePlans(CostEstimator estimator) {

        // check that union has only a single successor
        if (this.getOutgoingConnections().size() > 1) {
            throw new CompilerException("BinaryUnionNode has more than one successor.");
        }

        boolean childrenSkippedDueToReplicatedInput = false;

        // check if we have a cached version
        if (this.cachedPlans != null) {
            return this.cachedPlans;
        }

        // step down to all producer nodes and calculate alternative plans
        final List<? extends PlanNode> subPlans1 =
                getFirstPredecessorNode().getAlternativePlans(estimator);
        final List<? extends PlanNode> subPlans2 =
                getSecondPredecessorNode().getAlternativePlans(estimator);

        List<DagConnection> broadcastConnections = getBroadcastConnections();
        if (broadcastConnections != null && broadcastConnections.size() > 0) {
            throw new CompilerException("Found BroadcastVariables on a Union operation");
        }

        final ArrayList<PlanNode> outputPlans = new ArrayList<PlanNode>();

        final List<Set<? extends NamedChannel>> broadcastPlanChannels = Collections.emptyList();

        final BinaryUnionOpDescriptor operator = new BinaryUnionOpDescriptor();
        final RequestedLocalProperties noLocalProps = new RequestedLocalProperties();

        final ExecutionMode input1Mode = this.input1.getDataExchangeMode();
        final ExecutionMode input2Mode = this.input2.getDataExchangeMode();

        final int parallelism = getParallelism();
        final int inParallelism1 = getFirstPredecessorNode().getParallelism();
        final int inParallelism2 = getSecondPredecessorNode().getParallelism();

        final boolean dopChange1 = parallelism != inParallelism1;
        final boolean dopChange2 = parallelism != inParallelism2;

        final boolean input1breakPipeline = this.input1.isBreakingPipeline();
        final boolean input2breakPipeline = this.input2.isBreakingPipeline();

        // enumerate all pairwise combination of the children's plans together with
        // all possible operator strategy combination

        // create all candidates
        for (PlanNode child1 : subPlans1) {

            if (child1.getGlobalProperties().isFullyReplicated()) {
                // fully replicated input is always locally forwarded if parallelism is not changed
                if (dopChange1) {
                    // can not continue with this child
                    childrenSkippedDueToReplicatedInput = true;
                    continue;
                } else {
                    this.input1.setShipStrategy(ShipStrategyType.FORWARD);
                }
            }

            for (PlanNode child2 : subPlans2) {

                if (child2.getGlobalProperties().isFullyReplicated()) {
                    // fully replicated input is always locally forwarded if parallelism is not
                    // changed
                    if (dopChange2) {
                        // can not continue with this child
                        childrenSkippedDueToReplicatedInput = true;
                        continue;
                    } else {
                        this.input2.setShipStrategy(ShipStrategyType.FORWARD);
                    }
                }

                // check that the children go together. that is the case if they build upon the same
                // candidate at the joined branch plan.
                if (!areBranchCompatible(child1, child2)) {
                    continue;
                }

                for (RequestedGlobalProperties igps : this.channelProps) {
                    // create a candidate channel for the first input. mark it cached, if the
                    // connection says so
                    Channel c1 = new Channel(child1, this.input1.getMaterializationMode());
                    if (this.input1.getShipStrategy() == null) {
                        // free to choose the ship strategy
                        igps.parameterizeChannel(c1, dopChange1, input1Mode, input1breakPipeline);

                        // if the parallelism changed, make sure that we cancel out properties,
                        // unless the
                        // ship strategy preserves/establishes them even under changing parallelisms
                        if (dopChange1 && !c1.getShipStrategy().isNetworkStrategy()) {
                            c1.getGlobalProperties().reset();
                        }
                    } else {
                        // ship strategy fixed by compiler hint
                        ShipStrategyType shipStrategy = this.input1.getShipStrategy();
                        DataExchangeMode exMode =
                                DataExchangeMode.select(
                                        input1Mode, shipStrategy, input1breakPipeline);
                        if (this.keys1 != null) {
                            c1.setShipStrategy(
                                    this.input1.getShipStrategy(),
                                    this.keys1.toFieldList(),
                                    exMode);
                        } else {
                            c1.setShipStrategy(this.input1.getShipStrategy(), exMode);
                        }

                        if (dopChange1) {
                            c1.adjustGlobalPropertiesForFullParallelismChange();
                        }
                    }

                    // create a candidate channel for the second input. mark it cached, if the
                    // connection says so
                    Channel c2 = new Channel(child2, this.input2.getMaterializationMode());
                    if (this.input2.getShipStrategy() == null) {
                        // free to choose the ship strategy
                        igps.parameterizeChannel(c2, dopChange2, input2Mode, input2breakPipeline);

                        // if the parallelism changed, make sure that we cancel out properties,
                        // unless the
                        // ship strategy preserves/establishes them even under changing parallelisms
                        if (dopChange2 && !c2.getShipStrategy().isNetworkStrategy()) {
                            c2.getGlobalProperties().reset();
                        }
                    } else {
                        // ship strategy fixed by compiler hint
                        ShipStrategyType shipStrategy = this.input2.getShipStrategy();
                        DataExchangeMode exMode =
                                DataExchangeMode.select(
                                        input2Mode, shipStrategy, input2breakPipeline);
                        if (this.keys2 != null) {
                            c2.setShipStrategy(
                                    this.input2.getShipStrategy(),
                                    this.keys2.toFieldList(),
                                    exMode);
                        } else {
                            c2.setShipStrategy(this.input2.getShipStrategy(), exMode);
                        }

                        if (dopChange2) {
                            c2.adjustGlobalPropertiesForFullParallelismChange();
                        }
                    }

                    // get the global properties and clear unique fields (not preserved anyways
                    // during the union)
                    GlobalProperties p1 = c1.getGlobalProperties();
                    GlobalProperties p2 = c2.getGlobalProperties();
                    p1.clearUniqueFieldCombinations();
                    p2.clearUniqueFieldCombinations();

                    // adjust the partitioning, if they exist but are not equal. this may happen
                    // when both channels have a
                    // partitioning that fulfills the requirements, but both are incompatible. For
                    // example may a property requirement
                    // be ANY_PARTITIONING on fields (0) and one channel is range partitioned on
                    // that field, the other is hash
                    // partitioned on that field.
                    if (!igps.isTrivial() && !(p1.equals(p2))) {
                        if (c1.getShipStrategy() == ShipStrategyType.FORWARD
                                && c2.getShipStrategy() != ShipStrategyType.FORWARD) {
                            // adjust c2 to c1
                            c2 = c2.clone();
                            p1.parameterizeChannel(c2, dopChange2, input2Mode, input2breakPipeline);
                        } else if (c2.getShipStrategy() == ShipStrategyType.FORWARD
                                && c1.getShipStrategy() != ShipStrategyType.FORWARD) {
                            // adjust c1 to c2
                            c1 = c1.clone();
                            p2.parameterizeChannel(c1, dopChange1, input1Mode, input1breakPipeline);
                        } else if (c1.getShipStrategy() == ShipStrategyType.FORWARD
                                && c2.getShipStrategy() == ShipStrategyType.FORWARD) {
                            boolean adjustC1 =
                                    c1.getEstimatedOutputSize() <= 0
                                            || c2.getEstimatedOutputSize() <= 0
                                            || c1.getEstimatedOutputSize()
                                                    <= c2.getEstimatedOutputSize();
                            if (adjustC1) {
                                c2 = c2.clone();
                                p1.parameterizeChannel(
                                        c2, dopChange2, input2Mode, input2breakPipeline);
                            } else {
                                c1 = c1.clone();
                                p2.parameterizeChannel(
                                        c1, dopChange1, input1Mode, input1breakPipeline);
                            }
                        } else {
                            // this should never happen, as it implies both realize a different
                            // strategy, which is
                            // excluded by the check that the required strategies must match
                            throw new CompilerException("Bug in Plan Enumeration for Union Node.");
                        }
                    }

                    instantiate(
                            operator,
                            c1,
                            c2,
                            broadcastPlanChannels,
                            outputPlans,
                            estimator,
                            igps,
                            igps,
                            noLocalProps,
                            noLocalProps);
                }
            }
        }

        if (outputPlans.isEmpty()) {
            if (childrenSkippedDueToReplicatedInput) {
                throw new CompilerException(
                        "No plan meeting the requirements could be created @ "
                                + this
                                + ". Most likely reason: Invalid use of replicated input.");
            } else {
                throw new CompilerException(
                        "No plan meeting the requirements could be created @ "
                                + this
                                + ". Most likely reason: Too restrictive plan hints.");
            }
        }

        // cost and prune the plans
        for (PlanNode node : outputPlans) {
            estimator.costOperator(node);
        }
        prunePlanAlternatives(outputPlans);
        outputPlans.trimToSize();

        this.cachedPlans = outputPlans;
        return outputPlans;
    }

    @Override
    protected void readStubAnnotations() {}

    @Override
    public SemanticProperties getSemanticProperties() {
        return new UnionSemanticProperties();
    }

    @Override
    public void computeOutputEstimates(DataStatistics statistics) {
        OptimizerNode in1 = getFirstPredecessorNode();
        OptimizerNode in2 = getSecondPredecessorNode();

        this.estimatedNumRecords =
                in1.estimatedNumRecords > 0 && in2.estimatedNumRecords > 0
                        ? in1.estimatedNumRecords + in2.estimatedNumRecords
                        : -1;
        this.estimatedOutputSize =
                in1.estimatedOutputSize > 0 && in2.estimatedOutputSize > 0
                        ? in1.estimatedOutputSize + in2.estimatedOutputSize
                        : -1;
    }

    public static class UnionSemanticProperties implements SemanticProperties {

        private static final long serialVersionUID = 1L;

        @Override
        public FieldSet getForwardingTargetFields(int input, int sourceField) {
            if (input != 0 && input != 1) {
                throw new IndexOutOfBoundsException("Invalid input index for binary union node.");
            }

            return new FieldSet(sourceField);
        }

        @Override
        public int getForwardingSourceField(int input, int targetField) {
            if (input != 0 && input != 1) {
                throw new IndexOutOfBoundsException();
            }

            return targetField;
        }

        @Override
        public FieldSet getReadFields(int input) {
            if (input != 0 && input != 1) {
                throw new IndexOutOfBoundsException();
            }

            return FieldSet.EMPTY_SET;
        }
    }
}
