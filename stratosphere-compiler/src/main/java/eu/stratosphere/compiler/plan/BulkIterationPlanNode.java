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

package eu.stratosphere.compiler.plan;

import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE;
import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE_AND_DAM;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.costs.Costs;
import eu.stratosphere.compiler.dag.BulkIterationNode;
import eu.stratosphere.compiler.dag.OptimizerNode;
import eu.stratosphere.compiler.dag.TwoInputNode;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.util.Visitor;

public class BulkIterationPlanNode extends SingleInputPlanNode implements IterationPlanNode {
	
	private final BulkPartialSolutionPlanNode partialSolutionPlanNode;
	
	private final PlanNode rootOfStepFunction;
	
	private PlanNode rootOfTerminationCriterion;
	
	private TypeSerializerFactory<?> serializerForIterationChannel;
	
	// --------------------------------------------------------------------------------------------

	public BulkIterationPlanNode(BulkIterationNode template, String nodeName, Channel input,
			BulkPartialSolutionPlanNode pspn, PlanNode rootOfStepFunction)
	{
		super(template, nodeName, input, DriverStrategy.NONE);
		this.partialSolutionPlanNode = pspn;
		this.rootOfStepFunction = rootOfStepFunction;

		mergeBranchPlanMaps();
	}
	
	public BulkIterationPlanNode(BulkIterationNode template, String nodeName, Channel input,
			BulkPartialSolutionPlanNode pspn, PlanNode rootOfStepFunction, PlanNode rootOfTerminationCriterion)
	{
		this(template, nodeName, input, pspn, rootOfStepFunction);
		this.rootOfTerminationCriterion = rootOfTerminationCriterion;
	}

	// --------------------------------------------------------------------------------------------
	
	public BulkIterationNode getIterationNode() {
		if (this.template instanceof BulkIterationNode) {
			return (BulkIterationNode) this.template;
		} else {
			throw new RuntimeException();
		}
	}
	
	public BulkPartialSolutionPlanNode getPartialSolutionPlanNode() {
		return this.partialSolutionPlanNode;
	}
	
	public PlanNode getRootOfStepFunction() {
		return this.rootOfStepFunction;
	}
	
	public PlanNode getRootOfTerminationCriterion() {
		return this.rootOfTerminationCriterion;
	}
	
	// --------------------------------------------------------------------------------------------

	
	public TypeSerializerFactory<?> getSerializerForIterationChannel() {
		return serializerForIterationChannel;
	}
	
	public void setSerializerForIterationChannel(TypeSerializerFactory<?> serializerForIterationChannel) {
		this.serializerForIterationChannel = serializerForIterationChannel;
	}

	public void setCosts(Costs nodeCosts) {
		// add the costs from the step function
		nodeCosts.addCosts(this.rootOfStepFunction.getCumulativeCosts());
		
		if (rootOfTerminationCriterion != null) {
			// add the costs for the termination criterion
			nodeCosts.addCosts(this.rootOfTerminationCriterion.getCumulativeCosts());
		
			// subtract the costs that were counted twice (there must be some, since both the
			// next partial solution and the termination criterion depend on the partial solution,
			// i.e. have a common subexpression)ranches
			TwoInputNode auxJoiner = (TwoInputNode) getIterationNode().getSingleRootOfStepFunction();
			if (auxJoiner.getJoinedBranchers() == null || auxJoiner.getJoinedBranchers().isEmpty()) {
				throw new CompilerException("Error: No branch in step function between Solution Set Delta and Next Workset.");
			}

			// get the cumulative costs of the last joined branching node
			for (OptimizerNode joinedBrancher : auxJoiner.getJoinedBranchers()) {
				PlanNode lastCommonChild = this.rootOfStepFunction.branchPlan.get(joinedBrancher);
				Costs doubleCounted = lastCommonChild.getCumulativeCosts();
				nodeCosts.subtractCosts(doubleCounted);
			}
		}
		
		super.setCosts(nodeCosts);
	}
	
	public int getMemoryConsumerWeight() {
		return 1;
	}
	

	@Override
	public SourceAndDamReport hasDamOnPathDownTo(PlanNode source) {
		SourceAndDamReport fromOutside = super.hasDamOnPathDownTo(source);

		if (fromOutside == FOUND_SOURCE_AND_DAM) {
			return FOUND_SOURCE_AND_DAM;
		}
		else if (fromOutside == FOUND_SOURCE) {
			// we always have a dam in the back channel
			return FOUND_SOURCE_AND_DAM;
		} else {
			// check the step function for dams
			SourceAndDamReport fromStepFunction = this.rootOfStepFunction.hasDamOnPathDownTo(source);
			return fromStepFunction;
		}
	}


	@Override
	public void acceptForStepFunction(Visitor<PlanNode> visitor) {
		this.rootOfStepFunction.accept(visitor);
		
		if(this.rootOfTerminationCriterion != null) {
			this.rootOfTerminationCriterion.accept(visitor);
		}
		
	}

	private void mergeBranchPlanMaps() {
		for(OptimizerNode.UnclosedBranchDescriptor desc: template.getOpenBranches()){
			OptimizerNode brancher = desc.getBranchingNode();

			if(!branchPlan.containsKey(brancher)){
				PlanNode selectedCandidate = null;

				if(rootOfStepFunction.branchPlan != null){
					selectedCandidate = rootOfStepFunction.branchPlan.get(brancher);
				}

				if (selectedCandidate == null) {
					throw new CompilerException(
							"Candidates for a node with open branches are missing information about the selected candidate ");
				}

				this.branchPlan.put(brancher, selectedCandidate);
			}
		}
	}
}
