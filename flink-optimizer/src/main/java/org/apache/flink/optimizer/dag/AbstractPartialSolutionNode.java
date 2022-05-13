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
import org.apache.flink.api.common.operators.SemanticProperties.EmptySemanticProperties;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.costs.CostEstimator;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.util.Visitor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The optimizer's internal representation of the partial solution that is input to a bulk
 * iteration.
 */
public abstract class AbstractPartialSolutionNode extends OptimizerNode {

    protected AbstractPartialSolutionNode(Operator<?> contract) {
        super(contract);
    }

    // --------------------------------------------------------------------------------------------

    protected void copyEstimates(OptimizerNode node) {
        this.estimatedNumRecords = node.estimatedNumRecords;
        this.estimatedOutputSize = node.estimatedOutputSize;
    }

    public abstract IterationNode getIterationNode();

    // --------------------------------------------------------------------------------------------

    public boolean isOnDynamicPath() {
        return true;
    }

    public void identifyDynamicPath(int costWeight) {
        this.onDynamicPath = true;
        this.costWeight = costWeight;
    }

    @Override
    public List<DagConnection> getIncomingConnections() {
        return Collections.emptyList();
    }

    @Override
    public void setInput(
            Map<Operator<?>, OptimizerNode> contractToNode, ExecutionMode dataExchangeMode) {}

    @Override
    protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
        // we do nothing here, because the estimates can only be copied from the iteration input
    }

    @Override
    public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
        // no children, so nothing to compute
    }

    @Override
    public List<PlanNode> getAlternativePlans(CostEstimator estimator) {
        if (this.cachedPlans != null) {
            return this.cachedPlans;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public SemanticProperties getSemanticProperties() {
        return new EmptySemanticProperties();
    }

    @Override
    protected void readStubAnnotations() {}

    @Override
    public void accept(Visitor<OptimizerNode> visitor) {
        if (visitor.preVisit(this)) {
            visitor.postVisit(this);
        }
    }
}
