/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.compiler.plan;

import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE;
import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE_AND_DAM;

import eu.stratosphere.api.typeutils.TypeComparatorFactory;
import eu.stratosphere.api.typeutils.TypeSerializerFactory;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.costs.Costs;
import eu.stratosphere.compiler.dag.OptimizerNode;
import eu.stratosphere.compiler.dag.TwoInputNode;
import eu.stratosphere.compiler.dag.WorksetIterationNode;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.util.Visitor;

/**
 * 
 */
public class WorksetIterationPlanNode extends DualInputPlanNode implements IterationPlanNode {

	private final SolutionSetPlanNode solutionSetPlanNode;
	
	private final WorksetPlanNode worksetPlanNode;
	
	private final PlanNode solutionSetDeltaPlanNode;
	
	private final PlanNode nextWorkSetPlanNode;
	
	private TypeSerializerFactory<?> worksetSerializer;
	
	private TypeSerializerFactory<?> solutionSetSerializer;
	
	private TypeComparatorFactory<?> solutionSetComparator;
	
	private boolean immediateSolutionSetUpdate;
	
	public Object postPassHelper;
	
	// --------------------------------------------------------------------------------------------

	public WorksetIterationPlanNode(WorksetIterationNode template, String nodeName, Channel initialSolutionSet, Channel initialWorkset,
			SolutionSetPlanNode solutionSetPlanNode, WorksetPlanNode worksetPlanNode,
			PlanNode nextWorkSetPlanNode, PlanNode solutionSetDeltaPlanNode)
	{
		super(template, nodeName, initialSolutionSet, initialWorkset, DriverStrategy.NONE);
		this.solutionSetPlanNode = solutionSetPlanNode;
		this.worksetPlanNode = worksetPlanNode;
		this.solutionSetDeltaPlanNode = solutionSetDeltaPlanNode;
		this.nextWorkSetPlanNode = nextWorkSetPlanNode;
	}

	// --------------------------------------------------------------------------------------------
	
	public WorksetIterationNode getIterationNode() {
		if (this.template instanceof WorksetIterationNode) {
			return (WorksetIterationNode) this.template;
		} else {
			throw new RuntimeException();
		}
	}
	
	public SolutionSetPlanNode getSolutionSetPlanNode() {
		return this.solutionSetPlanNode;
	}
	
	public WorksetPlanNode getWorksetPlanNode() {
		return this.worksetPlanNode;
	}
	
	public PlanNode getSolutionSetDeltaPlanNode() {
		return this.solutionSetDeltaPlanNode;
	}
	
	public PlanNode getNextWorkSetPlanNode() {
		return this.nextWorkSetPlanNode;
	}
	
	public Channel getInitialSolutionSetInput() {
		return getInput1();
	}
	
	public Channel getInitialWorksetInput() {
		return getInput2();
	}
	
	public void setImmediateSolutionSetUpdate(boolean immediateUpdate) {
		this.immediateSolutionSetUpdate = immediateUpdate;
	}
	
	public boolean isImmediateSolutionSetUpdate() {
		return this.immediateSolutionSetUpdate;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public TypeSerializerFactory<?> getWorksetSerializer() {
		return worksetSerializer;
	}
	
	public void setWorksetSerializer(TypeSerializerFactory<?> worksetSerializer) {
		this.worksetSerializer = worksetSerializer;
	}
	
	public TypeSerializerFactory<?> getSolutionSetSerializer() {
		return solutionSetSerializer;
	}
	
	public void setSolutionSetSerializer(TypeSerializerFactory<?> solutionSetSerializer) {
		this.solutionSetSerializer = solutionSetSerializer;
	}
	
	public TypeComparatorFactory<?> getSolutionSetComparator() {
		return solutionSetComparator;
	}
	
	public void setSolutionSetComparator(TypeComparatorFactory<?> solutionSetComparator) {
		this.solutionSetComparator = solutionSetComparator;
	}

	// --------------------------------------------------------------------------------------------
	
	public void setCosts(Costs nodeCosts) {
		// add the costs from the step function
		nodeCosts.addCosts(this.solutionSetDeltaPlanNode.getCumulativeCosts());
		nodeCosts.addCosts(this.nextWorkSetPlanNode.getCumulativeCosts());
		
		// we have to subtract that which is double. sanity check that there are branches
		TwoInputNode auxJoiner = getIterationNode().getSingleRootOfStepFunction();
		if (auxJoiner.getJoinedBranchers() == null || auxJoiner.getJoinedBranchers().isEmpty()) {
			throw new CompilerException("Error: No branch in step function between Solution Set Delta and Next Workset.");
		}

		// get the cumulative costs of the last joined branching node
		for (OptimizerNode joinedBrancher : auxJoiner.getJoinedBranchers()) {
			PlanNode lastCommonChild = this.solutionSetDeltaPlanNode.branchPlan.get(joinedBrancher);
			Costs douleCounted = lastCommonChild.getCumulativeCosts();
			nodeCosts.subtractCosts(douleCounted);
		}
		super.setCosts(nodeCosts);
	}
	
	public int getMemoryConsumerWeight() {
		// solution set index and workset back channel
		return 2;
	}
	
	@Override
	public SourceAndDamReport hasDamOnPathDownTo(PlanNode source) {
		SourceAndDamReport fromOutside = super.hasDamOnPathDownTo(source);
		
		if (fromOutside == FOUND_SOURCE_AND_DAM) {
			return FOUND_SOURCE_AND_DAM;
		}
		else if (fromOutside == FOUND_SOURCE) {
			// we always have a dam in the solution set index
			return FOUND_SOURCE_AND_DAM;
		} else {
			return fromOutside;
		}
	}

	@Override
	public void acceptForStepFunction(Visitor<PlanNode> visitor) {
		this.solutionSetDeltaPlanNode.accept(visitor);
		this.nextWorkSetPlanNode.accept(visitor);
	}
}
