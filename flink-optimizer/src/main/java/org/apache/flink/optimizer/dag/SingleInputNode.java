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
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.CostEstimator;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.InterestingProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.operators.OperatorDescriptorSingle;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.NamedChannel;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.util.NoOpUnaryUdfOp;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.Visitor;

import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE;
import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE_AND_DAM;
import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.NOT_FOUND;

/**
 * A node in the optimizer's program representation for an operation with a single input.
 *
 * <p>This class contains all the generic logic for handling branching flows, as well as to
 * enumerate candidate execution plans. The subclasses for specific operators simply add logic for
 * cost estimates and specify possible strategies for their execution.
 */
public abstract class SingleInputNode extends OptimizerNode {

    protected final FieldSet keys; // The set of key fields

    protected DagConnection inConn; // the input of the node

    // --------------------------------------------------------------------------------------------

    /**
     * Creates a new node with a single input for the optimizer plan.
     *
     * @param programOperator The PACT that the node represents.
     */
    protected SingleInputNode(SingleInputOperator<?, ?, ?> programOperator) {
        super(programOperator);

        int[] k = programOperator.getKeyColumns(0);
        this.keys = k == null || k.length == 0 ? null : new FieldSet(k);
    }

    protected SingleInputNode(FieldSet keys) {
        super(NoOpUnaryUdfOp.INSTANCE);
        this.keys = keys;
    }

    protected SingleInputNode() {
        super(NoOpUnaryUdfOp.INSTANCE);
        this.keys = null;
    }

    protected SingleInputNode(SingleInputNode toCopy) {
        super(toCopy);

        this.keys = toCopy.keys;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public SingleInputOperator<?, ?, ?> getOperator() {
        return (SingleInputOperator<?, ?, ?>) super.getOperator();
    }

    /**
     * Gets the input of this operator.
     *
     * @return The input.
     */
    public DagConnection getIncomingConnection() {
        return this.inConn;
    }

    /**
     * Sets the connection through which this node receives its input.
     *
     * @param inConn The input connection to set.
     */
    public void setIncomingConnection(DagConnection inConn) {
        this.inConn = inConn;
    }

    /**
     * Gets the predecessor of this node.
     *
     * @return The predecessor of this node.
     */
    public OptimizerNode getPredecessorNode() {
        if (this.inConn != null) {
            return this.inConn.getSource();
        } else {
            return null;
        }
    }

    @Override
    public List<DagConnection> getIncomingConnections() {
        return Collections.singletonList(this.inConn);
    }

    @Override
    public SemanticProperties getSemanticProperties() {
        return getOperator().getSemanticProperties();
    }

    protected SemanticProperties getSemanticPropertiesForLocalPropertyFiltering() {
        return this.getSemanticProperties();
    }

    protected SemanticProperties getSemanticPropertiesForGlobalPropertyFiltering() {
        return this.getSemanticProperties();
    }

    @Override
    public void setInput(
            Map<Operator<?>, OptimizerNode> contractToNode, ExecutionMode defaultExchangeMode)
            throws CompilerException {
        // see if an internal hint dictates the strategy to use
        final Configuration conf = getOperator().getParameters();
        final String shipStrategy = conf.getString(Optimizer.HINT_SHIP_STRATEGY, null);
        final ShipStrategyType preSet;

        if (shipStrategy != null) {
            if (shipStrategy.equalsIgnoreCase(Optimizer.HINT_SHIP_STRATEGY_REPARTITION_HASH)) {
                preSet = ShipStrategyType.PARTITION_HASH;
            } else if (shipStrategy.equalsIgnoreCase(
                    Optimizer.HINT_SHIP_STRATEGY_REPARTITION_RANGE)) {
                preSet = ShipStrategyType.PARTITION_RANGE;
            } else if (shipStrategy.equalsIgnoreCase(Optimizer.HINT_SHIP_STRATEGY_FORWARD)) {
                preSet = ShipStrategyType.FORWARD;
            } else if (shipStrategy.equalsIgnoreCase(Optimizer.HINT_SHIP_STRATEGY_REPARTITION)) {
                preSet = ShipStrategyType.PARTITION_RANDOM;
            } else {
                throw new CompilerException("Unrecognized ship strategy hint: " + shipStrategy);
            }
        } else {
            preSet = null;
        }

        // get the predecessor node
        Operator<?> children = ((SingleInputOperator<?, ?, ?>) getOperator()).getInput();

        OptimizerNode pred;
        DagConnection conn;
        if (children == null) {
            throw new CompilerException(
                    "Error: Node for '" + getOperator().getName() + "' has no input.");
        } else {
            pred = contractToNode.get(children);
            conn = new DagConnection(pred, this, defaultExchangeMode);
            if (preSet != null) {
                conn.setShipStrategy(preSet);
            }
        }

        // create the connection and add it
        setIncomingConnection(conn);
        pred.addOutgoingConnection(conn);
    }

    // --------------------------------------------------------------------------------------------
    //                             Properties and Optimization
    // --------------------------------------------------------------------------------------------

    protected abstract List<OperatorDescriptorSingle> getPossibleProperties();

    @Override
    public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
        // get what we inherit and what is preserved by our user code
        final InterestingProperties props =
                getInterestingProperties().filterByCodeAnnotations(this, 0);

        // add all properties relevant to this node
        for (OperatorDescriptorSingle dps : getPossibleProperties()) {
            for (RequestedGlobalProperties gp : dps.getPossibleGlobalProperties()) {

                if (gp.getPartitioning().isPartitionedOnKey()) {
                    // make sure that among the same partitioning types, we do not push anything
                    // down that has fewer key fields

                    for (RequestedGlobalProperties contained : props.getGlobalProperties()) {
                        if (contained.getPartitioning() == gp.getPartitioning()
                                && gp.getPartitionedFields()
                                        .isValidSubset(contained.getPartitionedFields())) {
                            props.getGlobalProperties().remove(contained);
                            break;
                        }
                    }
                }

                props.addGlobalProperties(gp);
            }

            for (RequestedLocalProperties lp : dps.getPossibleLocalProperties()) {
                props.addLocalProperties(lp);
            }
        }
        this.inConn.setInterestingProperties(props);

        for (DagConnection conn : getBroadcastConnections()) {
            conn.setInterestingProperties(new InterestingProperties());
        }
    }

    @Override
    public List<PlanNode> getAlternativePlans(CostEstimator estimator) {
        // check if we have a cached version
        if (this.cachedPlans != null) {
            return this.cachedPlans;
        }

        boolean childrenSkippedDueToReplicatedInput = false;

        // calculate alternative sub-plans for predecessor
        final List<? extends PlanNode> subPlans =
                getPredecessorNode().getAlternativePlans(estimator);
        final Set<RequestedGlobalProperties> intGlobal =
                this.inConn.getInterestingProperties().getGlobalProperties();

        // calculate alternative sub-plans for broadcast inputs
        final List<Set<? extends NamedChannel>> broadcastPlanChannels =
                new ArrayList<Set<? extends NamedChannel>>();
        List<DagConnection> broadcastConnections = getBroadcastConnections();
        List<String> broadcastConnectionNames = getBroadcastConnectionNames();

        for (int i = 0; i < broadcastConnections.size(); i++) {
            DagConnection broadcastConnection = broadcastConnections.get(i);
            String broadcastConnectionName = broadcastConnectionNames.get(i);
            List<PlanNode> broadcastPlanCandidates =
                    broadcastConnection.getSource().getAlternativePlans(estimator);

            // wrap the plan candidates in named channels
            HashSet<NamedChannel> broadcastChannels =
                    new HashSet<NamedChannel>(broadcastPlanCandidates.size());
            for (PlanNode plan : broadcastPlanCandidates) {
                NamedChannel c = new NamedChannel(broadcastConnectionName, plan);
                DataExchangeMode exMode =
                        DataExchangeMode.select(
                                broadcastConnection.getDataExchangeMode(),
                                ShipStrategyType.BROADCAST,
                                broadcastConnection.isBreakingPipeline());
                c.setShipStrategy(ShipStrategyType.BROADCAST, exMode);
                broadcastChannels.add(c);
            }
            broadcastPlanChannels.add(broadcastChannels);
        }

        final RequestedGlobalProperties[] allValidGlobals;
        {
            Set<RequestedGlobalProperties> pairs = new HashSet<RequestedGlobalProperties>();
            for (OperatorDescriptorSingle ods : getPossibleProperties()) {
                pairs.addAll(ods.getPossibleGlobalProperties());
            }
            allValidGlobals = pairs.toArray(new RequestedGlobalProperties[pairs.size()]);
        }
        final ArrayList<PlanNode> outputPlans = new ArrayList<PlanNode>();

        final ExecutionMode executionMode = this.inConn.getDataExchangeMode();

        final int parallelism = getParallelism();
        final int inParallelism = getPredecessorNode().getParallelism();

        final boolean parallelismChange = inParallelism != parallelism;

        final boolean breaksPipeline = this.inConn.isBreakingPipeline();

        // create all candidates
        for (PlanNode child : subPlans) {

            if (child.getGlobalProperties().isFullyReplicated()) {
                // fully replicated input is always locally forwarded if the parallelism is not
                // changed
                if (parallelismChange) {
                    // can not continue with this child
                    childrenSkippedDueToReplicatedInput = true;
                    continue;
                } else {
                    this.inConn.setShipStrategy(ShipStrategyType.FORWARD);
                }
            }

            if (this.inConn.getShipStrategy() == null) {
                // pick the strategy ourselves
                for (RequestedGlobalProperties igps : intGlobal) {
                    final Channel c = new Channel(child, this.inConn.getMaterializationMode());
                    igps.parameterizeChannel(c, parallelismChange, executionMode, breaksPipeline);

                    // if the parallelism changed, make sure that we cancel out properties, unless
                    // the
                    // ship strategy preserves/establishes them even under changing parallelisms
                    if (parallelismChange && !c.getShipStrategy().isNetworkStrategy()) {
                        c.getGlobalProperties().reset();
                    }

                    // check whether we meet any of the accepted properties
                    // we may remove this check, when we do a check to not inherit
                    // requested global properties that are incompatible with all possible
                    // requested properties
                    for (RequestedGlobalProperties rgps : allValidGlobals) {
                        if (rgps.isMetBy(c.getGlobalProperties())) {
                            c.setRequiredGlobalProps(rgps);
                            addLocalCandidates(
                                    c, broadcastPlanChannels, igps, outputPlans, estimator);
                            break;
                        }
                    }
                }
            } else {
                // hint fixed the strategy
                final Channel c = new Channel(child, this.inConn.getMaterializationMode());
                final ShipStrategyType shipStrategy = this.inConn.getShipStrategy();
                final DataExchangeMode exMode =
                        DataExchangeMode.select(executionMode, shipStrategy, breaksPipeline);

                if (this.keys != null) {
                    c.setShipStrategy(shipStrategy, this.keys.toFieldList(), exMode);
                } else {
                    c.setShipStrategy(shipStrategy, exMode);
                }

                if (parallelismChange) {
                    c.adjustGlobalPropertiesForFullParallelismChange();
                }

                // check whether we meet any of the accepted properties
                for (RequestedGlobalProperties rgps : allValidGlobals) {
                    if (rgps.isMetBy(c.getGlobalProperties())) {
                        addLocalCandidates(c, broadcastPlanChannels, rgps, outputPlans, estimator);
                        break;
                    }
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

    protected void addLocalCandidates(
            Channel template,
            List<Set<? extends NamedChannel>> broadcastPlanChannels,
            RequestedGlobalProperties rgps,
            List<PlanNode> target,
            CostEstimator estimator) {
        for (RequestedLocalProperties ilp :
                this.inConn.getInterestingProperties().getLocalProperties()) {
            final Channel in = template.clone();
            ilp.parameterizeChannel(in);

            // instantiate a candidate, if the instantiated local properties meet one possible local
            // property set
            outer:
            for (OperatorDescriptorSingle dps : getPossibleProperties()) {
                for (RequestedLocalProperties ilps : dps.getPossibleLocalProperties()) {
                    if (ilps.isMetBy(in.getLocalProperties())) {
                        in.setRequiredLocalProps(ilps);
                        instantiateCandidate(
                                dps, in, broadcastPlanChannels, target, estimator, rgps, ilp);
                        break outer;
                    }
                }
            }
        }
    }

    protected void instantiateCandidate(
            OperatorDescriptorSingle dps,
            Channel in,
            List<Set<? extends NamedChannel>> broadcastPlanChannels,
            List<PlanNode> target,
            CostEstimator estimator,
            RequestedGlobalProperties globPropsReq,
            RequestedLocalProperties locPropsReq) {
        final PlanNode inputSource = in.getSource();

        for (List<NamedChannel> broadcastChannelsCombination :
                Sets.cartesianProduct(broadcastPlanChannels)) {

            boolean validCombination = true;
            boolean requiresPipelinebreaker = false;

            // check whether the broadcast inputs use the same plan candidate at the branching point
            for (int i = 0; i < broadcastChannelsCombination.size(); i++) {
                NamedChannel nc = broadcastChannelsCombination.get(i);
                PlanNode bcSource = nc.getSource();

                // check branch compatibility against input
                if (!areBranchCompatible(bcSource, inputSource)) {
                    validCombination = false;
                    break;
                }

                // check branch compatibility against all other broadcast variables
                for (int k = 0; k < i; k++) {
                    PlanNode otherBcSource = broadcastChannelsCombination.get(k).getSource();

                    if (!areBranchCompatible(bcSource, otherBcSource)) {
                        validCombination = false;
                        break;
                    }
                }

                // check if there is a common predecessor and whether there is a dam on the way to
                // all common predecessors
                if (in.isOnDynamicPath() && this.hereJoinedBranches != null) {
                    for (OptimizerNode brancher : this.hereJoinedBranches) {
                        PlanNode candAtBrancher =
                                in.getSource().getCandidateAtBranchPoint(brancher);

                        if (candAtBrancher == null) {
                            // closed branch between two broadcast variables
                            continue;
                        }

                        SourceAndDamReport res = in.getSource().hasDamOnPathDownTo(candAtBrancher);
                        if (res == NOT_FOUND) {
                            throw new CompilerException(
                                    "Bug: Tracing dams for deadlock detection is broken.");
                        } else if (res == FOUND_SOURCE) {
                            requiresPipelinebreaker = true;
                            break;
                        } else if (res == FOUND_SOURCE_AND_DAM) {
                            // good
                        } else {
                            throw new CompilerException();
                        }
                    }
                }
            }

            if (!validCombination) {
                continue;
            }

            if (requiresPipelinebreaker) {
                in.setTempMode(in.getTempMode().makePipelineBreaker());
            }

            final SingleInputPlanNode node = dps.instantiate(in, this);
            node.setBroadcastInputs(broadcastChannelsCombination);

            // compute how the strategy affects the properties
            GlobalProperties gProps = in.getGlobalProperties().clone();
            LocalProperties lProps = in.getLocalProperties().clone();
            gProps = dps.computeGlobalProperties(gProps);
            lProps = dps.computeLocalProperties(lProps);

            // filter by the user code field copies
            gProps =
                    gProps.filterBySemanticProperties(
                            getSemanticPropertiesForGlobalPropertyFiltering(), 0);
            lProps =
                    lProps.filterBySemanticProperties(
                            getSemanticPropertiesForLocalPropertyFiltering(), 0);

            // apply
            node.initProperties(gProps, lProps);
            node.updatePropertiesWithUniqueSets(getUniqueFields());
            target.add(node);
        }
    }

    // --------------------------------------------------------------------------------------------
    //                                     Branch Handling
    // --------------------------------------------------------------------------------------------

    @Override
    public void computeUnclosedBranchStack() {
        if (this.openBranches != null) {
            return;
        }

        addClosedBranches(getPredecessorNode().closedBranchingNodes);
        List<UnclosedBranchDescriptor> fromInput =
                getPredecessorNode().getBranchesForParent(this.inConn);

        // handle the data flow branching for the broadcast inputs
        List<UnclosedBranchDescriptor> result =
                computeUnclosedBranchStackForBroadcastInputs(fromInput);

        this.openBranches =
                (result == null || result.isEmpty())
                        ? Collections.<UnclosedBranchDescriptor>emptyList()
                        : result;
    }

    // --------------------------------------------------------------------------------------------
    //                                     Miscellaneous
    // --------------------------------------------------------------------------------------------

    @Override
    public void accept(Visitor<OptimizerNode> visitor) {
        if (visitor.preVisit(this)) {
            if (getPredecessorNode() != null) {
                getPredecessorNode().accept(visitor);
            } else {
                throw new CompilerException();
            }
            for (DagConnection connection : getBroadcastConnections()) {
                connection.getSource().accept(visitor);
            }
            visitor.postVisit(this);
        }
    }
}
