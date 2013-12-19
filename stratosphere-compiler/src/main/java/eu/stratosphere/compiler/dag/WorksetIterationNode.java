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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.api.common.operators.DeltaIteration;
import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler.InterestingPropertyVisitor;
import eu.stratosphere.compiler.costs.CostEstimator;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.InterestingProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.compiler.dataproperties.PartitioningProperty;
import eu.stratosphere.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.compiler.operators.OperatorDescriptorDual;
import eu.stratosphere.compiler.operators.SolutionSetDeltaOperator;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plan.SolutionSetPlanNode;
import eu.stratosphere.compiler.plan.WorksetIterationPlanNode;
import eu.stratosphere.compiler.plan.WorksetPlanNode;
import eu.stratosphere.compiler.util.NoOpBinaryUdfOp;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.util.Visitor;

/**
 * A node in the optimizer's program representation for a workset iteration.
 */
public class WorksetIterationNode extends TwoInputNode implements IterationNode {
	
	private static final int DEFAULT_COST_WEIGHT = 20;
	
	
	private final FieldList solutionSetKeyFields;
	
	private final GlobalProperties partitionedProperties;
	
	private SolutionSetNode solutionSetNode;
	
	private WorksetNode worksetNode;
	
	private OptimizerNode solutionSetDelta;
	
	private OptimizerNode nextWorkset;
	
	private PactConnection solutionSetDeltaRootConnection;
	
	private PactConnection nextWorksetRootConnection;
	
	private SingleRootJoiner singleRoot;
	
	private boolean solutionDeltaImmediatelyAfterSolutionJoin;
	
	private int costWeight;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract The PACT that the node represents.
	 */
	public WorksetIterationNode(DeltaIteration iteration) {
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
	
	public DeltaIteration getIterationContract() {
		return (DeltaIteration) getPactContract();
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
	
	public void setNextPartialSolution(OptimizerNode solutionSetDelta, OptimizerNode nextWorkset) {
		// check whether the next partial solution is itself the join with
		// the partial solution (so we can potentially do direct updates)
		if (solutionSetDelta instanceof TwoInputNode) {
			TwoInputNode solutionDeltaTwoInput = (TwoInputNode) solutionSetDelta;
			if (solutionDeltaTwoInput.getFirstPredecessorNode() == this.solutionSetNode ||
				solutionDeltaTwoInput.getSecondPredecessorNode() == this.solutionSetNode)
			{
				this.solutionDeltaImmediatelyAfterSolutionJoin = true;
			}
		}
		
		// attach an extra node to the solution set delta for the cases where we need to repartition
		UnaryOperatorNode solutionSetDeltaUpdateAux = new UnaryOperatorNode("Solution-Set Delta", getSolutionSetKeyFields(),
				new SolutionSetDeltaOperator(getSolutionSetKeyFields()));
		
		PactConnection conn = new PactConnection(solutionSetDelta, solutionSetDeltaUpdateAux, -1);
//		conn.setShipStrategy(ShipStrategyType.PARTITION_HASH);
		solutionSetDeltaUpdateAux.setIncomingConnection(conn);
		solutionSetDelta.addOutgoingConnection(conn);
		
		solutionSetDeltaUpdateAux.setDegreeOfParallelism(getDegreeOfParallelism());
		solutionSetDeltaUpdateAux.setSubtasksPerInstance(getSubtasksPerInstance());
		
		this.solutionSetDelta = solutionSetDeltaUpdateAux;
		this.nextWorkset = nextWorkset;
		
		this.singleRoot = new SingleRootJoiner();
		this.solutionSetDeltaRootConnection = new PactConnection(solutionSetDeltaUpdateAux, this.singleRoot, -1);
		this.nextWorksetRootConnection = new PactConnection(nextWorkset, this.singleRoot, -1);
		this.singleRoot.setInputs(this.solutionSetDeltaRootConnection, this.nextWorksetRootConnection);
		
		solutionSetDeltaUpdateAux.addOutgoingConnection(this.solutionSetDeltaRootConnection);
		nextWorkset.addOutgoingConnection(this.nextWorksetRootConnection);
	}
	
	public int getCostWeight() {
		return this.costWeight;
	}
	
	public TwoInputNode getSingleRootOfStepFunction() {
		return this.singleRoot;
	}
	
	public FieldList getSolutionSetKeyFields() {
		return this.solutionSetKeyFields;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public String getName() {
		return "Workset Iteration";
	}
	
	@Override
	public boolean isFieldConstant(int input, int fieldNumber) {
		return false;
	}
	
	protected void readStubAnnotations() {}
	
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
	
	@Override
	public boolean isMemoryConsumer() {
		return true;
	}
	
	@Override
	protected List<OperatorDescriptorDual> getPossibleProperties() {
		return new ArrayList<OperatorDescriptorDual>(1);
	}
	
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
		partitionedIP.addLocalProperties(new RequestedLocalProperties());
		
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
		// check for pipeline breaking using hash join with build on the solution set side
		placePipelineBreakersIfNecessary(DriverStrategy.HYBRIDHASH_BUILD_FIRST, solutionSetIn, worksetIn);
		
		// NOTES ON THE ENUMERATION OF THE STEP FUNCTION PLANS:
		// Whenever we instantiate the iteration, we enumerate new candidates for the step function.
		// That way, we make sure we have an appropriate plan for each candidate for the initial partial solution,
		// we have a fitting candidate for the step function (often, work is pushed out of the step function).
		// Among the candidates of the step function, we keep only those that meet the requested properties of the
		// current candidate initial partial solution. That makes sure these properties exist at the beginning of
		// every iteration.
		
		// 1) Because we enumerate multiple times, we may need to clean the cached plans
		//    before starting another enumeration
		this.nextWorkset.accept(PlanCacheCleaner.INSTANCE);
		this.solutionSetDelta.accept(PlanCacheCleaner.INSTANCE);
		
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
		
		// Make sure that the workset candidates fulfill the input requirements
		for (Iterator<PlanNode> planDeleter = worksetCandidates.iterator(); planDeleter.hasNext(); ) {
			PlanNode candidate = planDeleter.next();
			if (!(globPropsReqWorkset.isMetBy(candidate.getGlobalProperties()) && locPropsReqWorkset.isMetBy(candidate.getLocalProperties()))) {
				planDeleter.remove();
			}
		}
		if (worksetCandidates.isEmpty()) {
			return;
		}
		
		// sanity check the solution set delta and cancel out the delta node, if it is not needed
		for (Iterator<PlanNode> deltaPlans = solutionSetDeltaCandidates.iterator(); deltaPlans.hasNext(); ) {
			SingleInputPlanNode candidate = (SingleInputPlanNode) deltaPlans.next();
			GlobalProperties gp = candidate.getGlobalProperties();
			
			if (gp.getPartitioning() != PartitioningProperty.HASH_PARTITIONED || gp.getPartitioningFields() == null ||
					!gp.getPartitioningFields().equals(this.solutionSetKeyFields))
			{
				throw new CompilerException("Bug: The solution set delta is not partitioned.");
			}
		}
		
		// 5) Create a candidate for the Iteration Node for every remaining plan of the step function.
		
		final GlobalProperties gp = new GlobalProperties();
		gp.setHashPartitioned(this.solutionSetKeyFields);
		gp.addUniqueFieldCombination(this.solutionSetKeyFields);
		
		final LocalProperties lp = new LocalProperties();
		lp.addUniqueFields(this.solutionSetKeyFields);
		
		// take all combinations of solution set delta and workset plans
		for (PlanNode solutionSetCandidate : solutionSetDeltaCandidates) {
			for (PlanNode worksetCandidate : worksetCandidates) {
				// check whether they have the same operator at their latest branching point
				if (this.singleRoot.areBranchCompatible(solutionSetCandidate, worksetCandidate)) {
					
					SingleInputPlanNode siSolutionDeltaCandidate = (SingleInputPlanNode) solutionSetCandidate;
					boolean immediateDeltaUpdate;
					
					// check whether we need a dedicated solution set delta operator, or whether we can update on the fly
					if (siSolutionDeltaCandidate.getInput().getShipStrategy() == ShipStrategyType.FORWARD && this.solutionDeltaImmediatelyAfterSolutionJoin) {
						// we do not need this extra node. we can make the predecessor the delta
						// sanity check the node and connection
						if (siSolutionDeltaCandidate.getDriverStrategy() != DriverStrategy.UNARY_NO_OP || siSolutionDeltaCandidate.getInput().getLocalStrategy() != LocalStrategy.NONE) {
							throw new CompilerException("Invalid Solution set delta node.");
						}
						
						solutionSetCandidate = siSolutionDeltaCandidate.getInput().getSource();
						immediateDeltaUpdate = true;
					} else {
						// was not partitioned, we need to keep this node.
						// mark that we materialize the input
						siSolutionDeltaCandidate.getInput().setTempMode(TempMode.PIPELINE_BREAKER);
						immediateDeltaUpdate = false;
					}
					
					WorksetIterationPlanNode wsNode = new WorksetIterationPlanNode(
						this, "WorksetIteration ("+this.getPactContract().getName()+")", solutionSetIn, worksetIn, sspn, wspn, worksetCandidate, solutionSetCandidate);
					wsNode.setImmediateSolutionSetUpdate(immediateDeltaUpdate);
					wsNode.initProperties(gp, lp);
					target.add(wsNode);
				}
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                      Iteration Specific Traversals
	// --------------------------------------------------------------------------------------------

	public void acceptForStepFunction(Visitor<OptimizerNode> visitor) {
		this.singleRoot.accept(visitor);
	}
	
	// --------------------------------------------------------------------------------------------
	//                             Utility Classes
	// --------------------------------------------------------------------------------------------
	
	private static final class WorksetOpDescriptor extends OperatorDescriptorDual {
		
		private WorksetOpDescriptor(FieldList solutionSetKeys) {
			super(solutionSetKeys, null);
		}

		@Override
		public DriverStrategy getStrategy() {
			return DriverStrategy.NONE;
		}

		@Override
		protected List<GlobalPropertiesPair> createPossibleGlobalProperties() {
			RequestedGlobalProperties partitionedGp = new RequestedGlobalProperties();
			partitionedGp.setHashPartitioned(this.keys1);
			return Collections.singletonList(new GlobalPropertiesPair(partitionedGp, new RequestedGlobalProperties()));
		}

		@Override
		protected List<LocalPropertiesPair> createPossibleLocalProperties() {
			// all properties are possible
			return Collections.singletonList(new LocalPropertiesPair(
				new RequestedLocalProperties(), new RequestedLocalProperties()));
		}
		
		@Override
		public boolean areCoFulfilled(RequestedLocalProperties requested1, RequestedLocalProperties requested2,
				LocalProperties produced1, LocalProperties produced2) {
			return true;
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
	
	private static class SingleRootJoiner extends TwoInputNode {
		
		private SingleRootJoiner() {
			super(NoOpBinaryUdfOp.INSTANCE);
			
			setDegreeOfParallelism(1);
			setSubtasksPerInstance(1);
		}
		
		public void setInputs(PactConnection input1, PactConnection input2) {
			this.input1 = input1;
			this.input2 = input2;
		}
		
		@Override
		public String getName() {
			return "Internal Utility Node";
		}

		@Override
		protected List<OperatorDescriptorDual> getPossibleProperties() {
			return Collections.emptyList();
		}
	}
}
