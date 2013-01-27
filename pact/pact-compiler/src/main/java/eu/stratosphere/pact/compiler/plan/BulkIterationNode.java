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
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler.InterestingPropertyVisitor;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.InterestingProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.pact.compiler.operators.NoOpDescriptor;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorSingle;
import eu.stratosphere.pact.compiler.plan.candidate.BulkIterationPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.PartialSolutionPlanNode;
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
		final OptimizerNode n = this.inConn.getSource();
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
		this.nextPartialSolution.accept(InterestingPropertiesClearer.INSTANCE);
		
		// 2nd pass
		this.rootConnection.setInterestingProperties(intProps);
		this.nextPartialSolution.accept(new InterestingPropertyVisitor(estimator));
		
		// now add the interesting properties of the partial solution to the input
		final InterestingProperties inProps = this.partialSolution.getInterestingProperties().clone();
		inProps.addGlobalProperties(new RequestedGlobalProperties());
		inProps.addLocalProperties(new RequestedLocalProperties());
		this.inConn.setInterestingProperties(inProps);
	}
	
	
	
	// --------------------------------------------------------------------------------------------
	//                      Iteration Specific Traversals
	// --------------------------------------------------------------------------------------------
	
	@Override
	protected void instantiateCandidate(OperatorDescriptorSingle dps, Channel in, List<PlanNode> target,
			CostEstimator estimator, RequestedGlobalProperties globPropsReq, RequestedLocalProperties locPropsReq)
	{
		// NOTES ON THE ENUMERATION OF THE STEP FUNCTION PLANS:
		// Whenever we instantiate the iteration, we enumerate new candidates for the step function.
		// That way, we make sure we have an appropriate plan for each candidate for the initial partial solution,
		// we have a fitting candidate for the step function (often, work is pushed out of the step function).
		// Among the candidates of the step function, we keep only those that meet the requested properties of the
		// current candidate initial partial solution. That makes sure these properties exist at the beginning of
		// every iteration.
		
		// 1) Because we enumerate multiple times, we may need to clean the cached plans
		//    before starting another enumeration
		if (this.nextPartialSolution.cachedPlans != null) {
			this.nextPartialSolution.accept(PlanCacheCleaner.INSTANCE);
		}
		
		// 2) Give the partial solution the properties of the current candidate for the initial partial solution
		this.partialSolution.setCandidateProperties(in.getGlobalProperties(), in.getLocalProperties());
		final PartialSolutionPlanNode pspn = this.partialSolution.getCurrentPartialSolutionPlanNode();
		
		// 3) Get the alternative plans
		List<PlanNode> candidates = this.nextPartialSolution.getAlternativePlans(estimator);
		
		// 4) Throw away all that are not compatible with the properties currently requested to the
		//    initial partial solution
		for (Iterator<PlanNode> planDeleter = candidates.iterator(); planDeleter.hasNext(); ) {
			PlanNode candidate = planDeleter.next();
			if (!(globPropsReq.isMetBy(candidate.getGlobalProperties()) && locPropsReq.isMetBy(candidate.getLocalProperties()))) {
				planDeleter.remove();
			}
		}
		
		// 5) Create a candidate for the Iteration Node for every remaining plan of the step function.
		for (PlanNode candidate : candidates) {
			BulkIterationPlanNode node = new BulkIterationPlanNode(this, in, pspn, candidate);
			GlobalProperties gProps = candidate.getGlobalProperties().clone();
			LocalProperties lProps = candidate.getLocalProperties().clone();
			node.initProperties(gProps, lProps);
			target.add(node);
		}
	}

	public void acceptForStepFunction(Visitor<OptimizerNode> visitor) {
		this.nextPartialSolution.accept(visitor);
	}
	
	private static final class InterestingPropertiesClearer implements Visitor<OptimizerNode> {
		
		private static final InterestingPropertiesClearer INSTANCE = new InterestingPropertiesClearer();

		@Override
		public boolean preVisit(OptimizerNode visitable) {
			if (visitable.getInterestingProperties() != null) {
				visitable.clearInterestingProperties();
				return true;
			} else {
				return false;
			}
		}

		@Override
		public void postVisit(OptimizerNode visitable) {}
	}
	
	private static final class PlanCacheCleaner implements Visitor<OptimizerNode> {
		
		private static final PlanCacheCleaner INSTANCE = new PlanCacheCleaner();

		@Override
		public boolean preVisit(OptimizerNode visitable) {
			if (visitable.cachedPlans != null) {
				visitable.cachedPlans = null;
				return true;
			} else {
				return false;
			}
		}

		@Override
		public void postVisit(OptimizerNode visitable) {}
	}
}
