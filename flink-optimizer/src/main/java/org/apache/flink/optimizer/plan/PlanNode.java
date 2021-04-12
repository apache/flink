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

package org.apache.flink.optimizer.plan;

import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.costs.Costs;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.dag.OptimizerNode.UnclosedBranchDescriptor;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.plandump.DumpableConnection;
import org.apache.flink.optimizer.plandump.DumpableNode;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.util.Visitable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The representation of a data exchange between two operators. The data exchange can realize a
 * shipping strategy, which established global properties, and a local strategy, which establishes
 * local properties.
 *
 * <p>Because we currently deal only with plans where the operator order is fixed, many properties
 * are equal among candidates and are determined prior to the enumeration (such as for example
 * constant/dynamic path membership). Hence, many methods will delegate to the {@code OptimizerNode}
 * that represents the node this candidate was created for.
 */
public abstract class PlanNode implements Visitable<PlanNode>, DumpableNode<PlanNode> {

    protected final OptimizerNode template;

    protected final List<Channel> outChannels;

    private List<NamedChannel> broadcastInputs;

    private final String nodeName;

    private DriverStrategy driverStrategy; // The local strategy (sorting / hashing, ...)

    protected LocalProperties localProps; // local properties of the data produced by this node

    protected GlobalProperties globalProps; // global properties of the data produced by this node

    protected Map<OptimizerNode, PlanNode>
            branchPlan; // the actual plan alternative chosen at a branch point

    protected Costs nodeCosts; // the costs incurred by this node

    protected Costs cumulativeCosts; // the cumulative costs of all operators in the sub-tree

    private double
            relativeMemoryPerSubTask; // the amount of memory dedicated to each task, in bytes

    private int parallelism;

    private boolean pFlag; // flag for the internal pruning algorithm

    // --------------------------------------------------------------------------------------------

    public PlanNode(OptimizerNode template, String nodeName, DriverStrategy strategy) {
        this.outChannels = new ArrayList<Channel>(2);
        this.broadcastInputs = new ArrayList<NamedChannel>();
        this.template = template;
        this.nodeName = nodeName;
        this.driverStrategy = strategy;

        this.parallelism = template.getParallelism();

        // check, if there is branch at this node. if yes, this candidate must be associated with
        // the branching template node.
        if (template.isBranching()) {
            this.branchPlan = new HashMap<OptimizerNode, PlanNode>(6);
            this.branchPlan.put(template, this);
        }
    }

    protected void mergeBranchPlanMaps(PlanNode pred1, PlanNode pred2) {
        mergeBranchPlanMaps(pred1.branchPlan, pred2.branchPlan);
    }

    protected void mergeBranchPlanMaps(
            Map<OptimizerNode, PlanNode> branchPlan1, Map<OptimizerNode, PlanNode> branchPlan2) {
        // merge the branchPlan maps according the template's uncloseBranchesStack
        if (this.template.hasUnclosedBranches()) {
            if (this.branchPlan == null) {
                this.branchPlan = new HashMap<OptimizerNode, PlanNode>(8);
            }

            for (UnclosedBranchDescriptor uc : this.template.getOpenBranches()) {
                OptimizerNode brancher = uc.getBranchingNode();
                PlanNode selectedCandidate = null;

                if (branchPlan1 != null) {
                    // predecessor 1 has branching children, see if it got the branch we are looking
                    // for
                    selectedCandidate = branchPlan1.get(brancher);
                }

                if (selectedCandidate == null && branchPlan2 != null) {
                    // predecessor 2 has branching children, see if it got the branch we are looking
                    // for
                    selectedCandidate = branchPlan2.get(brancher);
                }

                // it may be that the branch candidate is only found once the broadcast variables
                // are set
                if (selectedCandidate != null) {
                    this.branchPlan.put(brancher, selectedCandidate);
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    //                                           Accessors
    // --------------------------------------------------------------------------------------------

    /**
     * Gets the node from the optimizer DAG for which this plan candidate node was created.
     *
     * @return The optimizer's DAG node.
     */
    public OptimizerNode getOriginalOptimizerNode() {
        return this.template;
    }

    /**
     * Gets the program operator that this node represents in the plan.
     *
     * @return The program operator this node represents in the plan.
     */
    public Operator<?> getProgramOperator() {
        return this.template.getOperator();
    }

    /**
     * Gets the name of the plan node.
     *
     * @return The name of the plan node.
     */
    public String getNodeName() {
        return this.nodeName;
    }

    public int getMemoryConsumerWeight() {
        return this.driverStrategy.isMaterializing() ? 1 : 0;
    }

    /**
     * Gets the memory dedicated to each sub-task for this node.
     *
     * @return The memory per task, in bytes.
     */
    public double getRelativeMemoryPerSubTask() {
        return this.relativeMemoryPerSubTask;
    }

    /**
     * Sets the memory dedicated to each task for this node.
     *
     * @param relativeMemoryPerSubtask The relative memory per sub-task
     */
    public void setRelativeMemoryPerSubtask(double relativeMemoryPerSubtask) {
        this.relativeMemoryPerSubTask = relativeMemoryPerSubtask;
    }

    /**
     * Gets the driver strategy from this node. This determines for example for a <i>match</i> Pact
     * whether to use a merge or a hybrid hash strategy.
     *
     * @return The driver strategy.
     */
    public DriverStrategy getDriverStrategy() {
        return this.driverStrategy;
    }

    /**
     * Sets the driver strategy for this node. Usually should not be changed.
     *
     * @param newDriverStrategy The driver strategy.
     */
    public void setDriverStrategy(DriverStrategy newDriverStrategy) {
        this.driverStrategy = newDriverStrategy;
    }

    public void initProperties(GlobalProperties globals, LocalProperties locals) {
        if (this.globalProps != null || this.localProps != null) {
            throw new IllegalStateException();
        }
        this.globalProps = globals;
        this.localProps = locals;
    }

    /**
     * Gets the local properties from this PlanNode.
     *
     * @return The local properties.
     */
    public LocalProperties getLocalProperties() {
        return this.localProps;
    }

    /**
     * Gets the global properties from this PlanNode.
     *
     * @return The global properties.
     */
    public GlobalProperties getGlobalProperties() {
        return this.globalProps;
    }

    /**
     * Gets the costs incurred by this node. The costs reflect also the costs incurred by the
     * shipping strategies of the incoming connections.
     *
     * @return The node-costs, or null, if not yet set.
     */
    public Costs getNodeCosts() {
        return this.nodeCosts;
    }

    /**
     * Gets the cumulative costs of this nose. The cumulative costs are the sum of the costs of this
     * node and of all nodes in the subtree below this node.
     *
     * @return The cumulative costs, or null, if not yet set.
     */
    public Costs getCumulativeCosts() {
        return this.cumulativeCosts;
    }

    public Costs getCumulativeCostsShare() {
        if (this.cumulativeCosts == null) {
            return null;
        } else {
            Costs result = cumulativeCosts.clone();
            if (this.template.getOutgoingConnections() != null) {
                int outDegree = this.template.getOutgoingConnections().size();
                if (outDegree > 0) {
                    result.divideBy(outDegree);
                }
            }

            return result;
        }
    }

    /**
     * Sets the basic cost for this node to the given value, and sets the cumulative costs to those
     * costs plus the cost shares of all inputs (regular and broadcast).
     *
     * @param nodeCosts The already knows costs for this node (this cost a produces by a concrete
     *     {@code OptimizerNode} subclass.
     */
    public void setCosts(Costs nodeCosts) {
        // set the node costs
        this.nodeCosts = nodeCosts;

        // the cumulative costs are the node costs plus the costs of all inputs
        this.cumulativeCosts = nodeCosts.clone();

        // add all the normal inputs
        for (PlanNode pred : getPredecessors()) {

            Costs parentCosts = pred.getCumulativeCostsShare();
            if (parentCosts != null) {
                this.cumulativeCosts.addCosts(parentCosts);
            } else {
                throw new CompilerException(
                        "Trying to set the costs of an operator before the predecessor costs are computed.");
            }
        }

        // add all broadcast variable inputs
        if (this.broadcastInputs != null) {
            for (NamedChannel nc : this.broadcastInputs) {
                Costs bcInputCost = nc.getSource().getCumulativeCostsShare();
                if (bcInputCost != null) {
                    this.cumulativeCosts.addCosts(bcInputCost);
                } else {
                    throw new CompilerException(
                            "Trying to set the costs of an operator before the broadcast input costs are computed.");
                }
            }
        }
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getParallelism() {
        return this.parallelism;
    }

    public ResourceSpec getMinResources() {
        return this.template.getOperator().getMinResources();
    }

    public ResourceSpec getPreferredResources() {
        return this.template.getOperator().getPreferredResources();
    }

    public long getGuaranteedAvailableMemory() {
        return this.template.getMinimalMemoryAcrossAllSubTasks();
    }

    public Map<OptimizerNode, PlanNode> getBranchPlan() {
        return branchPlan;
    }

    // --------------------------------------------------------------------------------------------
    //                               Input, Predecessors, Successors
    // --------------------------------------------------------------------------------------------

    public abstract Iterable<Channel> getInputs();

    @Override
    public abstract Iterable<PlanNode> getPredecessors();

    /** Sets a list of all broadcast inputs attached to this node. */
    public void setBroadcastInputs(List<NamedChannel> broadcastInputs) {
        if (broadcastInputs != null) {
            this.broadcastInputs = broadcastInputs;

            // update the branch map
            for (NamedChannel nc : broadcastInputs) {
                PlanNode source = nc.getSource();

                mergeBranchPlanMaps(branchPlan, source.branchPlan);
            }
        }

        // do a sanity check that if we are branching, we have now candidates for each branch point
        if (this.template.hasUnclosedBranches()) {
            if (this.branchPlan == null) {
                throw new CompilerException(
                        "Branching and rejoining logic did not find a candidate for the branching point.");
            }

            for (UnclosedBranchDescriptor uc : this.template.getOpenBranches()) {
                OptimizerNode brancher = uc.getBranchingNode();
                if (this.branchPlan.get(brancher) == null) {
                    throw new CompilerException(
                            "Branching and rejoining logic did not find a candidate for the branching point.");
                }
            }
        }
    }

    /** Gets a list of all broadcast inputs attached to this node. */
    public List<NamedChannel> getBroadcastInputs() {
        return this.broadcastInputs;
    }

    /**
     * Adds a channel to a successor node to this node.
     *
     * @param channel The channel to the successor.
     */
    public void addOutgoingChannel(Channel channel) {
        this.outChannels.add(channel);
    }

    /**
     * Gets a list of all outgoing channels leading to successors.
     *
     * @return A list of all channels leading to successors.
     */
    public List<Channel> getOutgoingChannels() {
        return this.outChannels;
    }

    // --------------------------------------------------------------------------------------------
    //                                Miscellaneous
    // --------------------------------------------------------------------------------------------

    public void updatePropertiesWithUniqueSets(Set<FieldSet> uniqueFieldCombinations) {
        if (uniqueFieldCombinations == null || uniqueFieldCombinations.isEmpty()) {
            return;
        }
        for (FieldSet fields : uniqueFieldCombinations) {
            this.globalProps.addUniqueFieldCombination(fields);
            this.localProps = this.localProps.addUniqueFields(fields);
        }
    }

    public PlanNode getCandidateAtBranchPoint(OptimizerNode branchPoint) {
        if (branchPlan == null) {
            return null;
        } else {
            return this.branchPlan.get(branchPoint);
        }
    }

    /** Sets the pruning marker to true. */
    public void setPruningMarker() {
        this.pFlag = true;
    }

    /**
     * Checks whether the pruning marker was set.
     *
     * @return True, if the pruning marker was set, false otherwise.
     */
    public boolean isPruneMarkerSet() {
        return this.pFlag;
    }

    public boolean isOnDynamicPath() {
        return this.template.isOnDynamicPath();
    }

    public int getCostWeight() {
        return this.template.getCostWeight();
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Checks whether this node has a dam on the way down to the given source node. This method
     * returns either that (a) the source node is not found as a (transitive) child of this node,
     * (b) the node is found, but no dam is on the path, or (c) the node is found and a dam is on
     * the path.
     *
     * @param source The node on the path to which the dam is sought.
     * @return The result whether the node is found and whether a dam is on the path.
     */
    public abstract SourceAndDamReport hasDamOnPathDownTo(PlanNode source);

    public FeedbackPropertiesMeetRequirementsReport checkPartialSolutionPropertiesMet(
            PlanNode partialSolution,
            GlobalProperties feedbackGlobal,
            LocalProperties feedbackLocal) {
        if (this == partialSolution) {
            return FeedbackPropertiesMeetRequirementsReport.PENDING;
        }

        boolean found = false;
        boolean allMet = true;
        boolean allLocallyMet = true;

        for (Channel input : getInputs()) {
            FeedbackPropertiesMeetRequirementsReport inputState =
                    input.getSource()
                            .checkPartialSolutionPropertiesMet(
                                    partialSolution, feedbackGlobal, feedbackLocal);

            if (inputState == FeedbackPropertiesMeetRequirementsReport.NO_PARTIAL_SOLUTION) {
                continue;
            } else if (inputState == FeedbackPropertiesMeetRequirementsReport.MET) {
                found = true;
                continue;
            } else if (inputState == FeedbackPropertiesMeetRequirementsReport.NOT_MET) {
                return FeedbackPropertiesMeetRequirementsReport.NOT_MET;
            } else {
                found = true;

                // the partial solution was on the path here. check whether the channel requires
                // certain properties that are met, or whether the channel introduces new properties

                // if the plan introduces new global properties, then we can stop looking whether
                // the feedback properties are sufficient to meet the requirements
                if (input.getShipStrategy() != ShipStrategyType.FORWARD
                        && input.getShipStrategy() != ShipStrategyType.NONE) {
                    continue;
                }

                // first check whether this channel requires something that is not met
                if (input.getRequiredGlobalProps() != null
                        && !input.getRequiredGlobalProps().isMetBy(feedbackGlobal)) {
                    return FeedbackPropertiesMeetRequirementsReport.NOT_MET;
                }

                // in general, not everything is met here already
                allMet = false;

                // if the plan introduces new local properties, we can stop checking for matching
                // local properties
                if (inputState != FeedbackPropertiesMeetRequirementsReport.PENDING_LOCAL_MET) {

                    if (input.getLocalStrategy() == LocalStrategy.NONE) {

                        if (input.getRequiredLocalProps() != null
                                && !input.getRequiredLocalProps().isMetBy(feedbackLocal)) {
                            return FeedbackPropertiesMeetRequirementsReport.NOT_MET;
                        }

                        allLocallyMet = false;
                    }
                }
            }
        }

        if (!found) {
            return FeedbackPropertiesMeetRequirementsReport.NO_PARTIAL_SOLUTION;
        } else if (allMet) {
            return FeedbackPropertiesMeetRequirementsReport.MET;
        } else if (allLocallyMet) {
            return FeedbackPropertiesMeetRequirementsReport.PENDING_LOCAL_MET;
        } else {
            return FeedbackPropertiesMeetRequirementsReport.PENDING;
        }
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return this.template.getOperatorName()
                + " \""
                + getProgramOperator().getName()
                + "\" : "
                + this.driverStrategy
                + " [[ "
                + this.globalProps
                + " ]] [[ "
                + this.localProps
                + " ]]";
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public OptimizerNode getOptimizerNode() {
        return this.template;
    }

    @Override
    public PlanNode getPlanNode() {
        return this;
    }

    @Override
    public Iterable<DumpableConnection<PlanNode>> getDumpableInputs() {
        List<DumpableConnection<PlanNode>> allInputs =
                new ArrayList<DumpableConnection<PlanNode>>();

        for (Channel c : getInputs()) {
            allInputs.add(c);
        }

        for (NamedChannel c : getBroadcastInputs()) {
            allInputs.add(c);
        }

        return allInputs;
    }

    // --------------------------------------------------------------------------------------------

    public static enum SourceAndDamReport {
        NOT_FOUND,
        FOUND_SOURCE,
        FOUND_SOURCE_AND_DAM
    }

    public static enum FeedbackPropertiesMeetRequirementsReport {
        /** Indicates that the path is irrelevant */
        NO_PARTIAL_SOLUTION,

        /**
         * Indicates that the question whether the properties are met has been determined pending
         * dependent on global and local properties
         */
        PENDING,

        /**
         * Indicates that the question whether the properties are met has been determined pending
         * dependent on global properties only
         */
        PENDING_LOCAL_MET,

        /** Indicates that the question whether the properties are met has been determined true */
        MET,

        /** Indicates that the question whether the properties are met has been determined false */
        NOT_MET
    }
}
