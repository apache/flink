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

import org.apache.flink.api.common.operators.base.BulkIterationBase.PartialSolutionPlaceHolder;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.plan.BulkPartialSolutionPlanNode;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.PlanNode;

import java.util.Collections;
import java.util.List;

/**
 * The optimizer's internal representation of the partial solution that is input to a bulk
 * iteration.
 */
public class BulkPartialSolutionNode extends AbstractPartialSolutionNode {

    private final BulkIterationNode iterationNode;

    public BulkPartialSolutionNode(
            PartialSolutionPlaceHolder<?> psph, BulkIterationNode iterationNode) {
        super(psph);
        this.iterationNode = iterationNode;
    }

    // --------------------------------------------------------------------------------------------

    public void setCandidateProperties(
            GlobalProperties gProps, LocalProperties lProps, Channel initialInput) {
        if (this.cachedPlans != null) {
            throw new IllegalStateException();
        } else {
            this.cachedPlans =
                    Collections.<PlanNode>singletonList(
                            new BulkPartialSolutionPlanNode(
                                    this,
                                    "PartialSolution (" + this.getOperator().getName() + ")",
                                    gProps,
                                    lProps,
                                    initialInput));
        }
    }

    public BulkPartialSolutionPlanNode getCurrentPartialSolutionPlanNode() {
        if (this.cachedPlans != null) {
            return (BulkPartialSolutionPlanNode) this.cachedPlans.get(0);
        } else {
            throw new IllegalStateException();
        }
    }

    public BulkIterationNode getIterationNode() {
        return this.iterationNode;
    }

    @Override
    public void computeOutputEstimates(DataStatistics statistics) {
        copyEstimates(this.iterationNode.getPredecessorNode());
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Gets the operator (here the {@link PartialSolutionPlaceHolder}) that is represented by this
     * optimizer node.
     *
     * @return The operator represented by this optimizer node.
     */
    @Override
    public PartialSolutionPlaceHolder<?> getOperator() {
        return (PartialSolutionPlaceHolder<?>) super.getOperator();
    }

    @Override
    public String getOperatorName() {
        return "Bulk Partial Solution";
    }

    @Override
    public void computeUnclosedBranchStack() {
        if (this.openBranches != null) {
            return;
        }

        OptimizerNode inputToIteration = this.iterationNode.getPredecessorNode();

        addClosedBranches(inputToIteration.closedBranchingNodes);
        List<UnclosedBranchDescriptor> fromInput =
                inputToIteration.getBranchesForParent(this.iterationNode.getIncomingConnection());
        this.openBranches =
                (fromInput == null || fromInput.isEmpty())
                        ? Collections.<UnclosedBranchDescriptor>emptyList()
                        : fromInput;
    }
}
