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

import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler.InterestingPropertyVisitor;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.dataproperties.InterestingProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.pact.compiler.operators.NoOpDescriptor;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorSingle;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.generic.contract.BulkIteration;

/**
 * A node in the optimizer's program representation for a bulk iteration.
 */
public class BulkIterationNode extends SingleInputNode implements IterationNode
{
	private static final int DEFAULT_COST_WEIGHT = 50;
	
	private PartialSolutionNode partialSolution;
	
	private OptimizerNode nextPartialSolution;
	
	private PactConnection rootConnection;
	
	private int costWeight;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract The PACT that the node represents.
	 */
	public BulkIterationNode(BulkIteration iteration) {
		super(iteration);
		
		this.costWeight = iteration.getNumberOfIterations() > 0 ? 
			iteration.getNumberOfIterations() : DEFAULT_COST_WEIGHT;
	}

	// --------------------------------------------------------------------------------------------
	
	public BulkIteration getIterationContract() {
		return (BulkIteration) getPactContract();
	}
	
	/**
	 * Gets the partialSolution from this BulkIterationNode.
	 *
	 * @return The partialSolution.
	 */
	public PartialSolutionNode getPartialSolution() {
		return partialSolution;
	}
	
	/**
	 * Sets the partialSolution for this BulkIterationNode.
	 *
	 * @param partialSolution The partialSolution to set.
	 */
	public void setPartialSolution(PartialSolutionNode partialSolution) {
		this.partialSolution = partialSolution;
	}

	
	/**
	 * Gets the nextPartialSolution from this BulkIterationNode.
	 *
	 * @return The nextPartialSolution.
	 */
	public OptimizerNode getNextPartialSolution() {
		return nextPartialSolution;
	}

	
	/**
	 * Sets the nextPartialSolution for this BulkIterationNode.
	 *
	 * @param nextPartialSolution The nextPartialSolution to set.
	 */
	public void setNextPartialSolution(OptimizerNode nextPartialSolution, PactConnection rootingConnection) {
		this.nextPartialSolution = nextPartialSolution;
		this.rootConnection = rootingConnection;
	}
	
	public int getCostWeight() {
		return this.costWeight;
	}

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Bulk Iteration";
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.SingleInputNode#isFieldConstant(int)
	 */
	@Override
	public boolean isFieldConstant(int fieldNumber) {
		return false;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isFieldConstant(int, int)
	 */
	@Override
	public boolean isFieldConstant(int input, int fieldNumber) {
		return false;
	}
	
	protected void readStubAnnotations() {
	}
	
	public void computeOutputEstimates(DataStatistics statistics) {
		// simply copy from the inputs
		final OptimizerNode n = this.inConn.getSourcePact();
		this.estimatedCardinality = n.estimatedCardinality;
		this.estimatedOutputSize = n.estimatedOutputSize;
		this.estimatedNumRecords = n.estimatedNumRecords;
	}
	
	// --------------------------------------------------------------------------------------------
	//                             Properties and Optimization
	// --------------------------------------------------------------------------------------------
	
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		return Collections.<OperatorDescriptorSingle>singletonList(new NoOpDescriptor());
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public boolean isMemoryConsumer() {
		return true;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingPropertiesForInputs(eu.stratosphere.pact.compiler.costs.CostEstimator)
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		final InterestingProperties intProps = getInterestingProperties().clone();
		
		// we need to make 2 interesting property passes, because the root of the step function needs also
		// the interesting properties as generated by the partial solution
		
		// give our own interesting properties (as generated by the iterations successors) to the step function and
		// make the first pass
		this.rootConnection.setInterestingProperties(intProps);
		this.nextPartialSolution.accept(new InterestingPropertyVisitor(estimator));
		
		// take the interesting properties of the partial solution and add them to the root interesting properties
		InterestingProperties partialSolutionIntProps = this.partialSolution.getInterestingProperties();
		intProps.getGlobalProperties().addAll(partialSolutionIntProps.getGlobalProperties());
		intProps.getLocalProperties().addAll(partialSolutionIntProps.getLocalProperties());
		
		// clear all interesting properties to prepare the second traversal
		this.rootConnection.clearInterestingProperties();
		this.nextPartialSolution.accept(new InterestingPropertiesClearer());
		
		// 2nd pass
		this.rootConnection.setInterestingProperties(intProps);
		this.nextPartialSolution.accept(new InterestingPropertyVisitor(estimator));
		
		// now add the interesting properties of the partial solution to the input
		final InterestingProperties inProps = this.partialSolution.getInterestingProperties().clone();
		inProps.addGlobalProperties(new RequestedGlobalProperties());
		inProps.addLocalProperties(new RequestedLocalProperties());
		this.inConn.setInterestingProperties(inProps);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans(eu.stratosphere.pact.compiler.costs.CostEstimator)
	 */
	@Override
	public List<PlanNode> getAlternativePlans(CostEstimator estimator) {
		return null;
	}
	
	// --------------------------------------------------------------------------------------------
	//                           Iteration Specific Methods / Classes
	// --------------------------------------------------------------------------------------------
	
	public void acceptForStepFunction(Visitor<OptimizerNode> visitor) {
		this.nextPartialSolution.accept(visitor);
	}
	
	private static final class InterestingPropertiesClearer implements Visitor<OptimizerNode> {

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.plan.Visitor#preVisit(eu.stratosphere.pact.common.plan.Visitable)
		 */
		@Override
		public boolean preVisit(OptimizerNode visitable) {
			if (visitable.getInterestingProperties() != null) {
				visitable.clearInterestingProperties();
				return true;
			} else {
				return false;
			}
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.plan.Visitor#postVisit(eu.stratosphere.pact.common.plan.Visitable)
		 */
		@Override
		public void postVisit(OptimizerNode visitable) {}
	}
}
