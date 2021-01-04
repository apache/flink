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

import org.apache.flink.api.common.operators.base.DeltaIterationBase.SolutionSetPlaceHolder;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SolutionSetPlanNode;

import java.util.Collections;
import java.util.List;

/** The optimizer's internal representation of the solution set of a workset iteration. */
public class SolutionSetNode extends AbstractPartialSolutionNode {

    private final WorksetIterationNode iterationNode;

    public SolutionSetNode(SolutionSetPlaceHolder<?> psph, WorksetIterationNode iterationNode) {
        super(psph);
        this.iterationNode = iterationNode;
    }

    // --------------------------------------------------------------------------------------------

    public void setCandidateProperties(
            GlobalProperties gProps, LocalProperties lProps, Channel initialInput) {
        this.cachedPlans =
                Collections.<PlanNode>singletonList(
                        new SolutionSetPlanNode(
                                this,
                                "SolutionSet (" + this.getOperator().getName() + ")",
                                gProps,
                                lProps,
                                initialInput));
    }

    public SolutionSetPlanNode getCurrentSolutionSetPlanNode() {
        if (this.cachedPlans != null) {
            return (SolutionSetPlanNode) this.cachedPlans.get(0);
        } else {
            throw new IllegalStateException();
        }
    }

    public WorksetIterationNode getIterationNode() {
        return this.iterationNode;
    }

    @Override
    public void computeOutputEstimates(DataStatistics statistics) {
        copyEstimates(this.iterationNode.getInitialSolutionSetPredecessorNode());
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Gets the contract object for this data source node.
     *
     * @return The contract.
     */
    @Override
    public SolutionSetPlaceHolder<?> getOperator() {
        return (SolutionSetPlaceHolder<?>) super.getOperator();
    }

    @Override
    public String getOperatorName() {
        return "Solution Set";
    }

    @Override
    public void computeUnclosedBranchStack() {
        if (this.openBranches != null) {
            return;
        }

        DagConnection solutionSetInput = this.iterationNode.getFirstIncomingConnection();
        OptimizerNode solutionSetSource = solutionSetInput.getSource();

        addClosedBranches(solutionSetSource.closedBranchingNodes);
        List<UnclosedBranchDescriptor> fromInput =
                solutionSetSource.getBranchesForParent(solutionSetInput);
        this.openBranches =
                (fromInput == null || fromInput.isEmpty())
                        ? Collections.<UnclosedBranchDescriptor>emptyList()
                        : fromInput;
    }
}
