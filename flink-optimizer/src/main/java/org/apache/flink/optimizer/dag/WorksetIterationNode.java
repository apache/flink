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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SemanticProperties.EmptySemanticProperties;
import org.apache.flink.api.common.operators.base.DeltaIterationBase;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.typeinfo.NothingTypeInfo;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.traversals.InterestingPropertyVisitor;
import org.apache.flink.optimizer.costs.CostEstimator;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.InterestingProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.PartitioningProperty;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.operators.OperatorDescriptorDual;
import org.apache.flink.optimizer.operators.SolutionSetDeltaOperator;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.NamedChannel;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SolutionSetPlanNode;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.optimizer.plan.WorksetPlanNode;
import org.apache.flink.optimizer.plan.PlanNode.FeedbackPropertiesMeetRequirementsReport;
import org.apache.flink.optimizer.util.NoOpBinaryUdfOp;
import org.apache.flink.optimizer.util.NoOpUnaryUdfOp;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.types.Nothing;
import org.apache.flink.util.Visitor;

/**
 * A node in the optimizer's program representation for a workset iteration.
 */
public class WorksetIterationNode extends TwoInputNode implements IterationNode {
	
	private static final int DEFAULT_COST_WEIGHT = 20;
	
	
	private final FieldList solutionSetKeyFields;
	
	private final GlobalProperties partitionedProperties;
	
	private final List<OperatorDescriptorDual> dataProperties;
	
	private SolutionSetNode solutionSetNode;
	
	private WorksetNode worksetNode;
	
	private OptimizerNode solutionSetDelta;
	
	private OptimizerNode nextWorkset;
	
	private DagConnection solutionSetDeltaRootConnection;
	
	private DagConnection nextWorksetRootConnection;
	
	private SingleRootJoiner singleRoot;
	
	private boolean solutionDeltaImmediatelyAfterSolutionJoin;
	
	private final int costWeight;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param iteration The iteration operator that the node represents.
	 */
	public WorksetIterationNode(DeltaIterationBase<?, ?> iteration) {
		super(iteration);
		
		final int[] ssKeys = iteration.getSolutionSetKeyFields();
		if (ssKeys == null || ssKeys.length == 0) {
			throw new CompilerException("Invalid WorksetIteration: No key fields defined for the solution set.");
		}
		this.solutionSetKeyFields = new FieldList(ssKeys);
		this.partitionedProperties = new GlobalProperties();
		this.partitionedProperties.setHashPartitioned(this.solutionSetKeyFields);
		
		int weight = iteration.getMaximumNumberOfIterations() > 0 ? 
			iteration.getMaximumNumberOfIterations() : DEFAULT_COST_WEIGHT;
			
		if (weight > OptimizerNode.MAX_DYNAMIC_PATH_COST_WEIGHT) {
			weight = OptimizerNode.MAX_DYNAMIC_PATH_COST_WEIGHT;
		}
		this.costWeight = weight; 
		
		this.dataProperties = Collections.<OperatorDescriptorDual>singletonList(new WorksetOpDescriptor(this.solutionSetKeyFields));
	}

	// --------------------------------------------------------------------------------------------
	
	public DeltaIterationBase<?, ?> getIterationContract() {
		return (DeltaIterationBase<?, ?>) getOperator();
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
										ExecutionMode executionMode) {

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
		
		// there needs to be at least one node in the workset path, so
		// if the next workset is equal to the workset, we need to inject a no-op node
		if (nextWorkset == worksetNode || nextWorkset instanceof BinaryUnionNode) {
			NoOpNode noop = new NoOpNode();
			noop.setParallelism(getParallelism());

			DagConnection noOpConn = new DagConnection(nextWorkset, noop, executionMode);
			noop.setIncomingConnection(noOpConn);
			nextWorkset.addOutgoingConnection(noOpConn);
			
			nextWorkset = noop;
		}
		
		// attach an extra node to the solution set delta for the cases where we need to repartition
		UnaryOperatorNode solutionSetDeltaUpdateAux = new UnaryOperatorNode("Solution-Set Delta", getSolutionSetKeyFields(),
				new SolutionSetDeltaOperator(getSolutionSetKeyFields()));
		solutionSetDeltaUpdateAux.setParallelism(getParallelism());

		DagConnection conn = new DagConnection(solutionSetDelta, solutionSetDeltaUpdateAux, executionMode);
		solutionSetDeltaUpdateAux.setIncomingConnection(conn);
		solutionSetDelta.addOutgoingConnection(conn);
		
		this.solutionSetDelta = solutionSetDeltaUpdateAux;
		this.nextWorkset = nextWorkset;
		
		this.singleRoot = new SingleRootJoiner();
		this.solutionSetDeltaRootConnection = new DagConnection(solutionSetDeltaUpdateAux,
													this.singleRoot, executionMode);

		this.nextWorksetRootConnection = new DagConnection(nextWorkset, this.singleRoot, executionMode);
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
	
	public OptimizerNode getInitialSolutionSetPredecessorNode() {
		return getFirstPredecessorNode();
	}
	
	public OptimizerNode getInitialWorksetPredecessorNode() {
		return getSecondPredecessorNode();
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public String getOperatorName() {
		return "Workset Iteration";
	}

	@Override
	public SemanticProperties getSemanticProperties() {
		return new EmptySemanticProperties();
	}

	protected void readStubAnnotations() {}
	
	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		this.estimatedOutputSize = getFirstPredecessorNode().getEstimatedOutputSize();
		this.estimatedNumRecords = getFirstPredecessorNode().getEstimatedNumRecords();
	}
	
	// --------------------------------------------------------------------------------------------
	//                             Properties and Optimization
	// --------------------------------------------------------------------------------------------
	
	@Override
	protected List<OperatorDescriptorDual> getPossibleProperties() {
		return this.dataProperties;
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
	public void clearInterestingProperties() {
		super.clearInterestingProperties();
		
		this.nextWorksetRootConnection.clearInterestingProperties();
		this.solutionSetDeltaRootConnection.clearInterestingProperties();
		
		this.nextWorkset.accept(InterestingPropertiesClearer.INSTANCE);
		this.solutionSetDelta.accept(InterestingPropertiesClearer.INSTANCE);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void instantiate(OperatorDescriptorDual operator, Channel solutionSetIn, Channel worksetIn,
			List<Set<? extends NamedChannel>> broadcastPlanChannels, List<PlanNode> target, CostEstimator estimator,
			RequestedGlobalProperties globPropsReqSolutionSet, RequestedGlobalProperties globPropsReqWorkset,
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
		this.worksetNode.setCandidateProperties(worksetIn.getGlobalProperties(), worksetIn.getLocalProperties(), worksetIn);
		this.solutionSetNode.setCandidateProperties(this.partitionedProperties, new LocalProperties(), solutionSetIn);
		
		final SolutionSetPlanNode sspn = this.solutionSetNode.getCurrentSolutionSetPlanNode();
		final WorksetPlanNode wspn = this.worksetNode.getCurrentWorksetPlanNode();
		
		// 3) Get the alternative plans
		List<PlanNode> solutionSetDeltaCandidates = this.solutionSetDelta.getAlternativePlans(estimator);
		List<PlanNode> worksetCandidates = this.nextWorkset.getAlternativePlans(estimator);
		
		// 4) Throw away all that are not compatible with the properties currently requested to the
		//    initial partial solution
		
		// Make sure that the workset candidates fulfill the input requirements
		{
			List<PlanNode> newCandidates = new ArrayList<PlanNode>();
			
			for (Iterator<PlanNode> planDeleter = worksetCandidates.iterator(); planDeleter.hasNext(); ) {
				PlanNode candidate = planDeleter.next();
				
				GlobalProperties atEndGlobal = candidate.getGlobalProperties();
				LocalProperties atEndLocal = candidate.getLocalProperties();
				
				FeedbackPropertiesMeetRequirementsReport report = candidate.checkPartialSolutionPropertiesMet(wspn,
																							atEndGlobal, atEndLocal);

				if (report == FeedbackPropertiesMeetRequirementsReport.NO_PARTIAL_SOLUTION) {
					// depends only through broadcast variable on the workset solution
				}
				else if (report == FeedbackPropertiesMeetRequirementsReport.NOT_MET) {
					// attach a no-op node through which we create the properties of the original input
					Channel toNoOp = new Channel(candidate);
					globPropsReqWorkset.parameterizeChannel(toNoOp, false,
															nextWorksetRootConnection.getDataExchangeMode(), false);
					locPropsReqWorkset.parameterizeChannel(toNoOp);

					NoOpUnaryUdfOp noOpUnaryUdfOp = new NoOpUnaryUdfOp<>();
					noOpUnaryUdfOp.setInput(candidate.getProgramOperator());

					UnaryOperatorNode rebuildWorksetPropertiesNode = new UnaryOperatorNode(
						"Rebuild Workset Properties",
						noOpUnaryUdfOp,
						true);
					
					rebuildWorksetPropertiesNode.setParallelism(candidate.getParallelism());
					
					SingleInputPlanNode rebuildWorksetPropertiesPlanNode = new SingleInputPlanNode(
												rebuildWorksetPropertiesNode, "Rebuild Workset Properties",
												toNoOp, DriverStrategy.UNARY_NO_OP);
					rebuildWorksetPropertiesPlanNode.initProperties(toNoOp.getGlobalProperties(),
																	toNoOp.getLocalProperties());
					estimator.costOperator(rebuildWorksetPropertiesPlanNode);
						
					GlobalProperties atEndGlobalModified = rebuildWorksetPropertiesPlanNode.getGlobalProperties();
					LocalProperties atEndLocalModified = rebuildWorksetPropertiesPlanNode.getLocalProperties();
						
					if (!(atEndGlobalModified.equals(atEndGlobal) && atEndLocalModified.equals(atEndLocal))) {
						FeedbackPropertiesMeetRequirementsReport report2 = candidate.checkPartialSolutionPropertiesMet(
																		wspn, atEndGlobalModified, atEndLocalModified);
						if (report2 != FeedbackPropertiesMeetRequirementsReport.NOT_MET) {
							newCandidates.add(rebuildWorksetPropertiesPlanNode);
						}
					}
					
					// remove the original operator and add the modified candidate
					planDeleter.remove();
					
				}
			}
			
			worksetCandidates.addAll(newCandidates);
		}
		
		if (worksetCandidates.isEmpty()) {
			return;
		}
		
		// sanity check the solution set delta
		for (PlanNode solutionSetDeltaCandidate : solutionSetDeltaCandidates) {
			SingleInputPlanNode candidate = (SingleInputPlanNode) solutionSetDeltaCandidate;
			GlobalProperties gp = candidate.getGlobalProperties();

			if (gp.getPartitioning() != PartitioningProperty.HASH_PARTITIONED || gp.getPartitioningFields() == null ||
					!gp.getPartitioningFields().equals(this.solutionSetKeyFields)) {
				throw new CompilerException("Bug: The solution set delta is not partitioned.");
			}
		}
		
		// 5) Create a candidate for the Iteration Node for every remaining plan of the step function.
		
		final GlobalProperties gp = new GlobalProperties();
		gp.setHashPartitioned(this.solutionSetKeyFields);
		gp.addUniqueFieldCombination(this.solutionSetKeyFields);
		
		LocalProperties lp = LocalProperties.EMPTY.addUniqueFields(this.solutionSetKeyFields);
		
		// take all combinations of solution set delta and workset plans
		for (PlanNode worksetCandidate : worksetCandidates) {
			for (PlanNode solutionSetCandidate : solutionSetDeltaCandidates) {
				// check whether they have the same operator at their latest branching point
				if (this.singleRoot.areBranchCompatible(solutionSetCandidate, worksetCandidate)) {
					
					SingleInputPlanNode siSolutionDeltaCandidate = (SingleInputPlanNode) solutionSetCandidate;
					boolean immediateDeltaUpdate;
					
					// check whether we need a dedicated solution set delta operator, or whether we can update on the fly
					if (siSolutionDeltaCandidate.getInput().getShipStrategy() == ShipStrategyType.FORWARD &&
							this.solutionDeltaImmediatelyAfterSolutionJoin)
					{
						// we do not need this extra node. we can make the predecessor the delta
						// sanity check the node and connection
						if (siSolutionDeltaCandidate.getDriverStrategy() != DriverStrategy.UNARY_NO_OP ||
								siSolutionDeltaCandidate.getInput().getLocalStrategy() != LocalStrategy.NONE)
						{
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
					
					WorksetIterationPlanNode wsNode = new WorksetIterationPlanNode(this,
							this.getOperator().getName(), solutionSetIn,
							worksetIn, sspn, wspn, worksetCandidate, solutionSetCandidate);
					wsNode.setImmediateSolutionSetUpdate(immediateDeltaUpdate);
					wsNode.initProperties(gp, lp);
					target.add(wsNode);
				}
			}
		}
	}

	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}
		
		// IMPORTANT: First compute closed branches from the two inputs
		// we need to do this because the runtime iteration head effectively joins
		addClosedBranches(getFirstPredecessorNode().closedBranchingNodes);
		addClosedBranches(getSecondPredecessorNode().closedBranchingNodes);

		List<UnclosedBranchDescriptor> result1 = getFirstPredecessorNode().getBranchesForParent(getFirstIncomingConnection());
		List<UnclosedBranchDescriptor> result2 = getSecondPredecessorNode().getBranchesForParent(getSecondIncomingConnection());

		ArrayList<UnclosedBranchDescriptor> inputsMerged1 = new ArrayList<UnclosedBranchDescriptor>();
		mergeLists(result1, result2, inputsMerged1, true); // this method also sets which branches are joined here (in the head)
		
		addClosedBranches(getSingleRootOfStepFunction().closedBranchingNodes);

		ArrayList<UnclosedBranchDescriptor> inputsMerged2 = new ArrayList<UnclosedBranchDescriptor>();
		List<UnclosedBranchDescriptor> result3 = getSingleRootOfStepFunction().openBranches;
		mergeLists(inputsMerged1, result3, inputsMerged2, true);

		// handle the data flow branching for the broadcast inputs
		List<UnclosedBranchDescriptor> result = computeUnclosedBranchStackForBroadcastInputs(inputsMerged2);

		this.openBranches = (result == null || result.isEmpty()) ? Collections.<UnclosedBranchDescriptor>emptyList() : result;
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
		public boolean areCompatible(RequestedGlobalProperties requested1, RequestedGlobalProperties requested2,
				GlobalProperties produced1, GlobalProperties produced2) {
			return true;
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
	
	public static class SingleRootJoiner extends TwoInputNode {
		
		SingleRootJoiner() {
			super(new NoOpBinaryUdfOp<Nothing>(new NothingTypeInfo()));
			
			setParallelism(1);
		}
		
		public void setInputs(DagConnection input1, DagConnection input2) {
			this.input1 = input1;
			this.input2 = input2;
		}
		
		@Override
		public String getOperatorName() {
			return "Internal Utility Node";
		}

		@Override
		protected List<OperatorDescriptorDual> getPossibleProperties() {
			return Collections.emptyList();
		}

		@Override
		protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
			// no estimates are needed here
		}
	}
}
