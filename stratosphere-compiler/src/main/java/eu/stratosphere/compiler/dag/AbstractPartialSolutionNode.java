/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.compiler.dag;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.costs.CostEstimator;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.util.Visitor;

/**
 * The optimizer's internal representation of the partial solution that is input to a bulk iteration.
 */
public abstract class AbstractPartialSolutionNode extends OptimizerNode {
	
	protected AbstractPartialSolutionNode(Operator contract) {
		super(contract);
	}

	// --------------------------------------------------------------------------------------------
	
	protected void copyEstimates(OptimizerNode node) {
		this.estimatedNumRecords = node.estimatedNumRecords;
		this.estimatedOutputSize = node.estimatedOutputSize;
	}
	
	public abstract IterationNode getIterationNode();
	
	// --------------------------------------------------------------------------------------------

	@Override
	public boolean isMemoryConsumer() {
		return false;
	}
	
	public boolean isOnDynamicPath() {
		return true;
	}
	
	public void identifyDynamicPath(int costWeight) {
		this.onDynamicPath = true;
		this.costWeight = costWeight;
	}

	@Override
	public List<PactConnection> getIncomingConnections() {
		return Collections.<PactConnection>emptyList();
	}

	@Override
	public void setInputs(Map<Operator, OptimizerNode> contractToNode) {}

	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		// we do nothing here, because the estimates can only be copied from the iteration input
	}
	
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// no children, so nothing to compute
	}

	@Override
	public void computeUnclosedBranchStack() {
		// because there are no inputs, there are no unclosed branches.
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
	public boolean isFieldConstant(int input, int fieldNumber) {
		return false;
	}
	
	@Override
	protected void readConstantAnnotation() {}
	
	@Override
	protected void readStubAnnotations() {}

	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		if (visitor.preVisit(this)) {
			visitor.postVisit(this);
		}
	}
}
