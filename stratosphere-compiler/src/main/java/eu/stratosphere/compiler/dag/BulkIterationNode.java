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
import java.util.Set;

import eu.stratosphere.api.common.operators.base.BulkIterationBase;
import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.api.common.operators.util.FieldSet;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler.InterestingPropertyVisitor;
import eu.stratosphere.compiler.costs.CostEstimator;
import eu.stratosphere.compiler.dag.WorksetIterationNode.SingleRootJoiner;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.InterestingProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.compiler.operators.NoOpDescriptor;
import eu.stratosphere.compiler.operators.OperatorDescriptorSingle;
import eu.stratosphere.compiler.plan.BulkIterationPlanNode;
import eu.stratosphere.compiler.plan.BulkPartialSolutionPlanNode;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.NamedChannel;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plan.PlanNode.FeedbackPropertiesMeetRequirementsReport;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.util.Visitor;

/**
 * A node in the optimizer's program representation for a bulk iteration.
 */
public class BulkIterationNode extends SingleInputNode implements IterationNode {
	
	private BulkPartialSolutionNode partialSolution;
	
	private OptimizerNode terminationCriterion;
	
	private OptimizerNode nextPartialSolution;
	
	private PactConnection rootConnection;
	
	private PactConnection terminationCriterionRootConnection;
	
	private OptimizerNode singleRoot;
	
	private final int costWeight;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new node for the bulk iteration.
	 * 
	 * @param iteration The bulk iteration the node represents.
	 */
	public BulkIterationNode(BulkIterationBase<?> iteration) {
		super(iteration);
		
		if (iteration.getMaximumNumberOfIterations() <= 0) {
			throw new CompilerException("BulkIteration must have a maximum number of iterations specified.");
		}
		
		int numIters = iteration.getMaximumNumberOfIterations();
		
		this.costWeight = (numIters > 0 && numIters < OptimizerNode.MAX_DYNAMIC_PATH_COST_WEIGHT) ?
			numIters : OptimizerNode.MAX_DYNAMIC_PATH_COST_WEIGHT; 
	}

	// --------------------------------------------------------------------------------------------
	
	public BulkIterationBase<?> getIterationContract() {
		return (BulkIterationBase<?>) getPactContract();
	}
	
	/**
	 * Gets the partialSolution from this BulkIterationNode.
	 *
	 * @return The partialSolution.
	 */
	public BulkPartialSolutionNode getPartialSolution() {
		return partialSolution;
	}
	
	/**
	 * Sets the partialSolution for this BulkIterationNode.
	 *
	 * @param partialSolution The partialSolution to set.
	 */
	public void setPartialSolution(BulkPartialSolutionNode partialSolution) {
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
	public void setNextPartialSolution(OptimizerNode nextPartialSolution, OptimizerNode terminationCriterion) {
		
		// check if the root of the step function has the same DOP as the iteration
		if (nextPartialSolution.getDegreeOfParallelism() != getDegreeOfParallelism())
		{
			// add a no-op to the root to express the re-partitioning
			NoOpNode noop = new NoOpNode();
			noop.setDegreeOfParallelism(getDegreeOfParallelism());

			PactConnection noOpConn = new PactConnection(nextPartialSolution, noop);
			noop.setIncomingConnection(noOpConn);
			nextPartialSolution.addOutgoingConnection(noOpConn);
			
			nextPartialSolution = noop;
		}
		
		this.nextPartialSolution = nextPartialSolution;
		this.terminationCriterion = terminationCriterion;
		
		if (terminationCriterion == null) {
			this.singleRoot = nextPartialSolution;
			this.rootConnection = new PactConnection(nextPartialSolution);
		}
		else {
			// we have a termination criterion
			SingleRootJoiner singleRootJoiner = new SingleRootJoiner();
			this.rootConnection = new PactConnection(nextPartialSolution, singleRootJoiner);
			this.terminationCriterionRootConnection = new PactConnection(terminationCriterion, singleRootJoiner);
			singleRootJoiner.setInputs(this.rootConnection, this.terminationCriterionRootConnection);
			
			this.singleRoot = singleRootJoiner;
			
			// add connection to terminationCriterion for interesting properties visitor
			terminationCriterion.addOutgoingConnection(terminationCriterionRootConnection);
		
		}
		
		nextPartialSolution.addOutgoingConnection(rootConnection);
	}
	
	public int getCostWeight() {
		return this.costWeight;
	}
	
	public OptimizerNode getSingleRootOfStepFunction() {
		return this.singleRoot;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public String getName() {
		return "Bulk Iteration";
	}
	
	@Override
	public boolean isFieldConstant(int input, int fieldNumber) {
		return false;
	}

	@Override
	public FieldSet getForwardField(int input, int fieldNumber) {
		return null;
	}

	protected void readStubAnnotations() {}
	
	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		this.estimatedOutputSize = getPredecessorNode().getEstimatedOutputSize();
		this.estimatedNumRecords = getPredecessorNode().getEstimatedNumRecords();
	}
	
	// --------------------------------------------------------------------------------------------
	//                             Properties and Optimization
	// --------------------------------------------------------------------------------------------
	
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		return Collections.<OperatorDescriptorSingle>singletonList(new NoOpDescriptor());
	}

	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		final InterestingProperties intProps = getInterestingProperties().clone();
		
		if (this.terminationCriterion != null) {
			// first propagate through termination Criterion. since it has no successors, it has no
			// interesting properties
			this.terminationCriterionRootConnection.setInterestingProperties(new InterestingProperties());
			this.terminationCriterion.accept(new InterestingPropertyVisitor(estimator));
		}
		
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
		// this clears only the path down from the next partial solution. The paths down
		// from the termination criterion (before they meet the paths down from the next partial solution)
		// remain unaffected by this step
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

	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}

		// the resulting branches are those of the step function
		// because the BulkPartialSolution takes the input's branches
		addClosedBranches(getSingleRootOfStepFunction().closedBranchingNodes);
		List<UnclosedBranchDescriptor> result = getSingleRootOfStepFunction().openBranches;

		this.openBranches = (result == null || result.isEmpty()) ? Collections.<UnclosedBranchDescriptor>emptyList() : result;
	}


	@Override
	protected void instantiateCandidate(OperatorDescriptorSingle dps, Channel in, List<Set<? extends NamedChannel>> broadcastPlanChannels, 
			List<PlanNode> target, CostEstimator estimator, RequestedGlobalProperties globPropsReq, RequestedLocalProperties locPropsReq)
	{
		// NOTES ON THE ENUMERATION OF THE STEP FUNCTION PLANS:
		// Whenever we instantiate the iteration, we enumerate new candidates for the step function.
		// That way, we make sure we have an appropriate plan for each candidate for the initial partial solution,
		// we have a fitting candidate for the step function (often, work is pushed out of the step function).
		// Among the candidates of the step function, we keep only those that meet the requested properties of the
		// current candidate initial partial solution. That makes sure these properties exist at the beginning of
		// the successive iteration.
		
		// 1) Because we enumerate multiple times, we may need to clean the cached plans
		//    before starting another enumeration
		this.nextPartialSolution.accept(PlanCacheCleaner.INSTANCE);
		if (this.terminationCriterion != null) {
			this.terminationCriterion.accept(PlanCacheCleaner.INSTANCE);
		}
		
		// 2) Give the partial solution the properties of the current candidate for the initial partial solution
		this.partialSolution.setCandidateProperties(in.getGlobalProperties(), in.getLocalProperties(), in);
		final BulkPartialSolutionPlanNode pspn = this.partialSolution.getCurrentPartialSolutionPlanNode();
		
		// 3) Get the alternative plans
		List<PlanNode> candidates = this.nextPartialSolution.getAlternativePlans(estimator);
		
		// 4) Make sure that the beginning of the step function does not assume properties that 
		//    are not also produced by the end of the step function.

		{
			List<PlanNode> newCandidates = new ArrayList<PlanNode>();
			
			for (Iterator<PlanNode> planDeleter = candidates.iterator(); planDeleter.hasNext(); ) {
				PlanNode candidate = planDeleter.next();
				
				GlobalProperties atEndGlobal = candidate.getGlobalProperties();
				LocalProperties atEndLocal = candidate.getLocalProperties();
				
				FeedbackPropertiesMeetRequirementsReport report = candidate.checkPartialSolutionPropertiesMet(pspn, atEndGlobal, atEndLocal);
				if (report == FeedbackPropertiesMeetRequirementsReport.NO_PARTIAL_SOLUTION) {
					; // depends only through broadcast variable on the partial solution
				}
				else if (report == FeedbackPropertiesMeetRequirementsReport.NOT_MET) {
					// attach a no-op node through which we create the properties of the original input
					Channel toNoOp = new Channel(candidate);
					globPropsReq.parameterizeChannel(toNoOp, false);
					locPropsReq.parameterizeChannel(toNoOp);
					
					UnaryOperatorNode rebuildPropertiesNode = new UnaryOperatorNode("Rebuild Partial Solution Properties", FieldList.EMPTY_LIST);
					rebuildPropertiesNode.setDegreeOfParallelism(candidate.getDegreeOfParallelism());
					
					SingleInputPlanNode rebuildPropertiesPlanNode = new SingleInputPlanNode(rebuildPropertiesNode, "Rebuild Partial Solution Properties", toNoOp, DriverStrategy.UNARY_NO_OP);
					rebuildPropertiesPlanNode.initProperties(toNoOp.getGlobalProperties(), toNoOp.getLocalProperties());
					estimator.costOperator(rebuildPropertiesPlanNode);
						
					GlobalProperties atEndGlobalModified = rebuildPropertiesPlanNode.getGlobalProperties();
					LocalProperties atEndLocalModified = rebuildPropertiesPlanNode.getLocalProperties();
						
					if (!(atEndGlobalModified.equals(atEndGlobal) && atEndLocalModified.equals(atEndLocal))) {
						FeedbackPropertiesMeetRequirementsReport report2 = candidate.checkPartialSolutionPropertiesMet(pspn, atEndGlobalModified, atEndLocalModified);
						
						if (report2 != FeedbackPropertiesMeetRequirementsReport.NOT_MET) {
							newCandidates.add(rebuildPropertiesPlanNode);
						}
					}
					
					planDeleter.remove();
				}
			}
		}
		
		if (candidates.isEmpty()) {
			return;
		}
		
		// 5) Create a candidate for the Iteration Node for every remaining plan of the step function.
		if (terminationCriterion == null) {
			for (PlanNode candidate : candidates) {
				BulkIterationPlanNode node = new BulkIterationPlanNode(this, "BulkIteration ("+this.getPactContract().getName()+")", in, pspn, candidate);
				GlobalProperties gProps = candidate.getGlobalProperties().clone();
				LocalProperties lProps = candidate.getLocalProperties().clone();
				node.initProperties(gProps, lProps);
				target.add(node);
			}
		}
		else if (candidates.size() > 0) {
			List<PlanNode> terminationCriterionCandidates = this.terminationCriterion.getAlternativePlans(estimator);

			SingleRootJoiner singleRoot = (SingleRootJoiner) this.singleRoot;
			
			for (PlanNode candidate : candidates) {
				for (PlanNode terminationCandidate : terminationCriterionCandidates) {
					if (singleRoot.areBranchCompatible(candidate, terminationCandidate)) {
						BulkIterationPlanNode node = new BulkIterationPlanNode(this, "BulkIteration ("+this.getPactContract().getName()+")", in, pspn, candidate, terminationCandidate);
						GlobalProperties gProps = candidate.getGlobalProperties().clone();
						LocalProperties lProps = candidate.getLocalProperties().clone();
						node.initProperties(gProps, lProps);
						target.add(node);
						
					}
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
}
