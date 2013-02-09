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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler.InterestingPropertyVisitor;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.InterestingProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.dataproperties.PartitioningProperty;
import eu.stratosphere.pact.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorDual;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorDual.LocalPropertiesPair;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SolutionSetPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.WorksetPlanNode;
import eu.stratosphere.pact.generic.contract.WorksetIteration;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

/**
 * A node in the optimizer's program representation for a workset iteration.
 */
public class WorksetIterationNode extends TwoInputNode implements IterationNode
{
	private static final int DEFAULT_COST_WEIGHT = 50;
	
	
	private final FieldList solutionSetKeyFields;
	
	private final GlobalProperties partitionedProperties;
	
	private SolutionSetNode solutionSetNode;
	
	private WorksetNode worksetNode;
	
	private OptimizerNode solutionSetDelta;
	
	private OptimizerNode nextWorkset;
	
	private PactConnection solutionSetDeltaRootConnection;
	
	private PactConnection nextWorksetRootConnection;
	
	private int costWeight;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract The PACT that the node represents.
	 */
	public WorksetIterationNode(WorksetIteration iteration) {
		super(iteration);
		
		final int[] ssKeys = iteration.getSolutionSetKeyFields();
		if (ssKeys == null || ssKeys.length == 0) {
			throw new CompilerException("Invalid WorksetIteration: No key fields defined for the solution set.");
		}
		this.solutionSetKeyFields = new FieldList(ssKeys);
		this.partitionedProperties = new GlobalProperties();
		this.partitionedProperties.setHashPartitioned(this.solutionSetKeyFields);
		
		this.costWeight = iteration.getMaximumNumberOfIterations() > 0 ? 
			iteration.getMaximumNumberOfIterations() : DEFAULT_COST_WEIGHT;
		
		this.possibleProperties.add(new WorksetOpDescriptor(this.solutionSetKeyFields));
	}

	// --------------------------------------------------------------------------------------------
	
	public WorksetIteration getIterationContract() {
		return (WorksetIteration) getPactContract();
	}
	
	public SolutionSetNode getSolutionSetNode() {
		return this.solutionSetNode;
	}
	
	public WorksetNode getWorksetNode() {
		return this.worksetNode;
	}
	
	public OptimizerNode getNextWorkset() {
		return this.nextWorkset;
	}
	
	public OptimizerNode getSolutionSetDelta() {
		return this.solutionSetDelta;
	}

	public void setPartialSolution(SolutionSetNode solutionSetNode, WorksetNode worksetNode) {
		if (this.solutionSetNode != null || this.worksetNode != null) {
			throw new IllegalStateException("Error: Initializing WorksetIterationNode multiple times.");
		}
		this.solutionSetNode = solutionSetNode;
		this.worksetNode = worksetNode;
	}
	
	public void setNextPartialSolution(OptimizerNode solutionSetDelta, OptimizerNode nextWorkset, 
			PactConnection solutionSetDeltaRootConnection, PactConnection nextWorksetRoot) {
		this.solutionSetDelta = solutionSetDelta;
		this.nextWorkset = nextWorkset;
		this.solutionSetDeltaRootConnection = solutionSetDeltaRootConnection;
		this.nextWorksetRootConnection = nextWorksetRoot;
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
		return "Workset Iteration";
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
		// copy from the partial solution input.
		// NOTE: This assumes that the solution set is not inflationary, but constant!
		final OptimizerNode n = this.input1.getSource();
		this.estimatedCardinality = n.estimatedCardinality;
		this.estimatedOutputSize = n.estimatedOutputSize;
		this.estimatedNumRecords = n.estimatedNumRecords;
	}
	
	// --------------------------------------------------------------------------------------------
	//                             Properties and Optimization
	// --------------------------------------------------------------------------------------------
	
//	protected List<OperatorDescriptorSingle> getPossibleProperties() {
//		return Collections.<OperatorDescriptorSingle>singletonList(new NoOpDescriptor());
//	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public boolean isMemoryConsumer() {
		return true;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.TwoInputNode#getPossibleProperties()
	 */
	@Override
	protected List<OperatorDescriptorDual> getPossibleProperties() {
		return new ArrayList<OperatorDescriptorDual>(1);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingPropertiesForInputs(eu.stratosphere.pact.compiler.costs.CostEstimator)
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// our own solution (the solution set) is always partitioned and this cannot be adjusted
		// depending on what the successor to the workset iteration requests. for that reason,
		// we ignore incoming interesting properties.
		
		// in addition, we need to make 2 interesting property passes, because the root of the step function 
		// that computes the next workset needs the interesting properties as generated by the
		// workset source of the step function. the second pass concerns only the workset path.
		// as initial interesting properties, we have the trivial ones for the step function,
		// and partitioned on the solution set key for the solution set delta 
		
		RequestedGlobalProperties partitionedProperties = new RequestedGlobalProperties();
		partitionedProperties.setHashPartitioned(this.solutionSetKeyFields);
		InterestingProperties partitionedIP = new InterestingProperties();
		partitionedIP.addGlobalProperties(partitionedProperties);
		
		this.nextWorksetRootConnection.setInterestingProperties(new InterestingProperties());
		this.solutionSetDeltaRootConnection.setInterestingProperties(partitionedIP.clone());
		
		InterestingPropertyVisitor ipv = new InterestingPropertyVisitor(estimator);
		this.nextWorkset.accept(ipv);
		this.solutionSetDelta.accept(ipv);
		
		// take the interesting properties of the partial solution and add them to the root interesting properties
		InterestingProperties worksetIntProps = this.worksetNode.getInterestingProperties();
		InterestingProperties intProps = new InterestingProperties();
		intProps.getGlobalProperties().addAll(worksetIntProps.getGlobalProperties());
		intProps.getLocalProperties().addAll(worksetIntProps.getLocalProperties());
		
		// clear all interesting properties to prepare the second traversal
		this.nextWorksetRootConnection.clearInterestingProperties();
		this.nextWorkset.accept(InterestingPropertiesClearer.INSTANCE);
		
		// 2nd pass
		this.nextWorksetRootConnection.setInterestingProperties(intProps);
		this.nextWorkset.accept(ipv);
		
		// now add the interesting properties of the workset to the workset input
		final InterestingProperties inProps = this.worksetNode.getInterestingProperties().clone();
		inProps.addGlobalProperties(new RequestedGlobalProperties());
		inProps.addLocalProperties(new RequestedLocalProperties());
		this.input2.setInterestingProperties(inProps);
		
		// the partial solution must be hash partitioned, so it has only that as interesting properties
		this.input1.setInterestingProperties(partitionedIP);
	}
	
	
	@Override
	protected void instantiate(OperatorDescriptorDual operator, Channel solutionSetIn, Channel worksetIn,
			List<PlanNode> target, CostEstimator estimator,
			RequestedGlobalProperties globPropsReqSolutionSet,RequestedGlobalProperties globPropsReqWorkset,
			RequestedLocalProperties locPropsReqSolutionSet, RequestedLocalProperties locPropsReqWorkset)
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
		if (this.nextWorkset.cachedPlans != null) {
			this.nextWorkset.accept(PlanCacheCleaner.INSTANCE);
		}
		if (this.solutionSetDelta.cachedPlans != null) {
			this.solutionSetDelta.accept(PlanCacheCleaner.INSTANCE);
		}
		
		// 2) Give the partial solution the properties of the current candidate for the initial partial solution
		//    This concerns currently only the workset.
		this.worksetNode.setCandidateProperties(worksetIn.getGlobalProperties(), worksetIn.getLocalProperties());
		this.solutionSetNode.setCandidateProperties(this.partitionedProperties, new LocalProperties());
		
		final SolutionSetPlanNode sspn = this.solutionSetNode.getCurrentSolutionSetPlanNode();
		final WorksetPlanNode wspn = this.worksetNode.getCurrentWorksetPlanNode();
		
		// 3) Get the alternative plans
		List<PlanNode> solutionSetDeltaCandidates = this.solutionSetDelta.getAlternativePlans(estimator);
		List<PlanNode> worksetCandidates = this.nextWorkset.getAlternativePlans(estimator);
		
		// 4) Throw away all that are not compatible with the properties currently requested to the
		//    initial partial solution
		
		// check the solution set delta
		for (Iterator<PlanNode> planDeleter = solutionSetDeltaCandidates.iterator(); planDeleter.hasNext(); ) {
			PlanNode candidate = planDeleter.next();
			GlobalProperties gp = candidate.getGlobalProperties();
			if (gp.getPartitioning() != PartitioningProperty.HASH_PARTITIONED || gp.getPartitioningFields() == null ||
					!gp.getPartitioningFields().equals(this.solutionSetKeyFields)) {
				planDeleter.remove();
			}
		}
		if (solutionSetDeltaCandidates.isEmpty()) {
			throw new CompilerException("No viable strategies for solution set delta found during plan enumeration. " +
					"Possible reason: Partitioning is not preserved when matching with the solution set. Missing annotation?");
		}
		
		// ... and on the workset
		for (Iterator<PlanNode> planDeleter = worksetCandidates.iterator(); planDeleter.hasNext(); ) {
			PlanNode candidate = planDeleter.next();
			if (!(globPropsReqWorkset.isMetBy(candidate.getGlobalProperties()) && locPropsReqWorkset.isMetBy(candidate.getLocalProperties()))) {
				planDeleter.remove();
			}
		}
		if (worksetCandidates.isEmpty()) {
			return;
		}
		
		// 5) Create a candidate for the Iteration Node for every remaining plan of the step function.
		
		final GlobalProperties gp = new GlobalProperties();
		gp.setHashPartitioned(this.solutionSetKeyFields);
		gp.addUniqueFieldCombination(this.solutionSetKeyFields);
		
		final LocalProperties lp = new LocalProperties();
		lp.addUniqueFields(this.solutionSetKeyFields);
		
//		for (PlanNode solutionSetCandidate : solutionSetDeltaCandidates) {
//			for (PlanNode worksetCandidate : worksetCandidates) {
//				areBranchCompatible(plan1, plan2)
//				BulkIterationPlanNode node = new BulkIterationPlanNode(this, in, pspn, candidate);
//				node.initProperties(gp, lp);
//				target.add(node);
//			}
//		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                      Iteration Specific Traversals
	// --------------------------------------------------------------------------------------------

	public void acceptForStepFunction(Visitor<OptimizerNode> visitor) {
		this.nextWorkset.accept(visitor);
		this.solutionSetDelta.accept(visitor);
	}
	
	private static final class WorksetOpDescriptor extends OperatorDescriptorDual {
		
		private final FieldList solutionSetKeys;
		
		private WorksetOpDescriptor(FieldList solutionSetKeys) {
			this.solutionSetKeys = solutionSetKeys;
		}

		@Override
		public DriverStrategy getStrategy() {
			return DriverStrategy.NONE;
		}

		@Override
		protected List<GlobalPropertiesPair> createPossibleGlobalProperties() {
			RequestedGlobalProperties partitionedGp = new RequestedGlobalProperties();
			partitionedGp.setHashPartitioned(this.solutionSetKeys);
			return Collections.singletonList(new GlobalPropertiesPair(partitionedGp, new RequestedGlobalProperties()));
		}

		@Override
		protected List<LocalPropertiesPair> createPossibleLocalProperties() {
			// all properties are possible
			return Collections.singletonList(new LocalPropertiesPair(
				new RequestedLocalProperties(), new RequestedLocalProperties()));
		}

		@Override
		public DualInputPlanNode instantiate(Channel in1, Channel in2, TwoInputNode node) {
			throw new UnsupportedOperationException();
		}

		@Override
		public GlobalProperties computeGlobalProperties(GlobalProperties in1, GlobalProperties in2) {
			throw new UnsupportedOperationException();
		}

		@Override
		public LocalProperties computeLocalProperties(LocalProperties in1, LocalProperties in2) {
			throw new UnsupportedOperationException();
		}
	}
}
