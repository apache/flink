/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler.plan;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.generic.contract.BulkIteration.PartialSolutionPlaceHolder;
import eu.stratosphere.pact.generic.contract.Contract;

/**
 * The optimizer's internal representation of the partial solution that is input to a bulk iteration.
 */
public class PartialSolutionNode extends OptimizerNode
{
	private OptimizerNode initialPartialSolutionNode;
	
	
	public PartialSolutionNode(PartialSolutionPlaceHolder psph) {
		super(psph);
	}

	// --------------------------------------------------------------------------------------------
	
	public void setInitialPartialSolutionNode(OptimizerNode node) {
		this.initialPartialSolutionNode = node;
	}
	
	public void copyEstimates(OptimizerNode node) {
		this.estimatedCardinality = node.estimatedCardinality;
		this.estimatedNumRecords = node.estimatedNumRecords;
		this.estimatedOutputSize = node.estimatedOutputSize;
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the contract object for this data source node.
	 * 
	 * @return The contract.
	 */
	@Override
	public PartialSolutionPlaceHolder getPactContract() {
		return (PartialSolutionPlaceHolder) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Partial Solution";
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public boolean isMemoryConsumer() {
		return false;
	}
	
	public boolean isOnDynamicPath() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<PactConnection> getIncomingConnections() {
		return Collections.<PactConnection>emptyList();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
	}

	/**
	 * Causes this node to compute its output estimates (such as number of rows, size in bytes)
	 * based on the file size and the compiler hints. The compiler hints are instantiated with
	 * conservative default values which are used if no other values are provided.
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		// do nothing. we obtain the estimates another way from the enclosing iteration
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingProperties()
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// no children, so nothing to compute
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUnclosedBranchStack()
	 */
	@Override
	public void computeUnclosedBranchStack() {
		// because there are no inputs, there are no unclosed branches.
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeAlternativePlans()
	 */
	@Override
	public List<PlanNode> getAlternativePlans(CostEstimator estimator) {
		return null;
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isFieldConstant(int, int)
	 */
	@Override
	public boolean isFieldConstant(int input, int fieldNumber) {
		return false;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readReadsAnnotation()
	 */
	@Override
	protected void readConstantAnnotation() {
	}
	
	@Override
	protected void readStubAnnotations() {
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		if (visitor.preVisit(this)) {
			visitor.postVisit(this);
		}
	}
}
