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
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SemanticProperties.EmptySemanticProperties;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.costs.CostEstimator;
import org.apache.flink.optimizer.dataproperties.InterestingProperties;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.util.Visitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** The Optimizer representation of a data sink. */
public class DataSinkNode extends OptimizerNode {

    protected DagConnection input; // The input edge

    /**
     * Creates a new DataSinkNode for the given sink operator.
     *
     * @param sink The data sink contract object.
     */
    public DataSinkNode(GenericDataSinkBase<?> sink) {
        super(sink);
    }

    // --------------------------------------------------------------------------------------

    /**
     * Gets the input of the sink.
     *
     * @return The input connection.
     */
    public DagConnection getInputConnection() {
        return this.input;
    }

    /**
     * Gets the predecessor of this node.
     *
     * @return The predecessor, or null, if no predecessor has been set.
     */
    public OptimizerNode getPredecessorNode() {
        if (this.input != null) {
            return input.getSource();
        } else {
            return null;
        }
    }

    /**
     * Gets the operator for which this optimizer sink node was created.
     *
     * @return The node's underlying operator.
     */
    @Override
    public GenericDataSinkBase<?> getOperator() {
        return (GenericDataSinkBase<?>) super.getOperator();
    }

    @Override
    public String getOperatorName() {
        return "Data Sink";
    }

    @Override
    public List<DagConnection> getIncomingConnections() {
        return Collections.singletonList(this.input);
    }

    /**
     * Gets all outgoing connections, which is an empty set for the data sink.
     *
     * @return An empty list.
     */
    @Override
    public List<DagConnection> getOutgoingConnections() {
        return Collections.emptyList();
    }

    @Override
    public void setInput(
            Map<Operator<?>, OptimizerNode> contractToNode, ExecutionMode defaultExchangeMode) {
        Operator<?> children = getOperator().getInput();

        final OptimizerNode pred;
        final DagConnection conn;

        pred = contractToNode.get(children);
        conn = new DagConnection(pred, this, defaultExchangeMode);

        // create the connection and add it
        this.input = conn;
        pred.addOutgoingConnection(conn);
    }

    /**
     * Computes the estimated outputs for the data sink. Since the sink does not modify anything, it
     * simply copies the output estimates from its direct predecessor.
     */
    @Override
    protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
        this.estimatedNumRecords = getPredecessorNode().getEstimatedNumRecords();
        this.estimatedOutputSize = getPredecessorNode().getEstimatedOutputSize();
    }

    @Override
    public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
        final InterestingProperties iProps = new InterestingProperties();

        {
            final RequestedGlobalProperties partitioningProps = new RequestedGlobalProperties();
            iProps.addGlobalProperties(partitioningProps);
        }

        {
            final Ordering localOrder = getOperator().getLocalOrder();
            final RequestedLocalProperties orderProps = new RequestedLocalProperties();
            if (localOrder != null) {
                orderProps.setOrdering(localOrder);
            }
            iProps.addLocalProperties(orderProps);
        }

        this.input.setInterestingProperties(iProps);
    }

    // --------------------------------------------------------------------------------------------
    //                                     Branch Handling
    // --------------------------------------------------------------------------------------------

    @Override
    public void computeUnclosedBranchStack() {
        if (this.openBranches != null) {
            return;
        }

        // we need to track open branches even in the sinks, because they get "closed" when
        // we build a single "root" for the data flow plan
        addClosedBranches(getPredecessorNode().closedBranchingNodes);
        this.openBranches = getPredecessorNode().getBranchesForParent(this.input);
    }

    @Override
    protected List<UnclosedBranchDescriptor> getBranchesForParent(DagConnection parent) {
        // return our own stack of open branches, because nothing is added
        return this.openBranches;
    }

    // --------------------------------------------------------------------------------------------
    //                                   Recursive Optimization
    // --------------------------------------------------------------------------------------------

    @Override
    public List<PlanNode> getAlternativePlans(CostEstimator estimator) {
        // check if we have a cached version
        if (this.cachedPlans != null) {
            return this.cachedPlans;
        }

        // calculate alternative sub-plans for predecessor
        List<? extends PlanNode> subPlans = getPredecessorNode().getAlternativePlans(estimator);
        List<PlanNode> outputPlans = new ArrayList<PlanNode>();

        final int parallelism = getParallelism();
        final int inDop = getPredecessorNode().getParallelism();

        final ExecutionMode executionMode = this.input.getDataExchangeMode();
        final boolean dopChange = parallelism != inDop;
        final boolean breakPipeline = this.input.isBreakingPipeline();

        InterestingProperties ips = this.input.getInterestingProperties();
        for (PlanNode p : subPlans) {
            for (RequestedGlobalProperties gp : ips.getGlobalProperties()) {
                for (RequestedLocalProperties lp : ips.getLocalProperties()) {
                    Channel c = new Channel(p);
                    gp.parameterizeChannel(c, dopChange, executionMode, breakPipeline);
                    lp.parameterizeChannel(c);
                    c.setRequiredLocalProps(lp);
                    c.setRequiredGlobalProps(gp);

                    // no need to check whether the created properties meet what we need in case
                    // of ordering or global ordering, because the only interesting properties we
                    // have
                    // are what we require
                    outputPlans.add(
                            new SinkPlanNode(
                                    this, "DataSink (" + this.getOperator().getName() + ")", c));
                }
            }
        }

        // cost and prune the plans
        for (PlanNode node : outputPlans) {
            estimator.costOperator(node);
        }
        prunePlanAlternatives(outputPlans);

        this.cachedPlans = outputPlans;
        return outputPlans;
    }

    // --------------------------------------------------------------------------------------------
    //                                   Function Annotation Handling
    // --------------------------------------------------------------------------------------------

    @Override
    public SemanticProperties getSemanticProperties() {
        return new EmptySemanticProperties();
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
            visitor.postVisit(this);
        }
    }
}
