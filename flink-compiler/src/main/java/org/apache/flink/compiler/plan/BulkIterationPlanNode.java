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


package org.apache.flink.compiler.plan;

import static org.apache.flink.compiler.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE;
import static org.apache.flink.compiler.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE_AND_DAM;

import java.util.HashMap;

import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.compiler.CompilerException;
import org.apache.flink.compiler.costs.Costs;
import org.apache.flink.compiler.dag.BulkIterationNode;
import org.apache.flink.compiler.dag.OptimizerNode;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.util.Visitor;

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
		
		// add the costs for the termination criterion, if it exists
		// the costs are divided at branches, so we can simply add them up
		if (rootOfTerminationCriterion != null) {
			nodeCosts.addCosts(this.rootOfTerminationCriterion.getCumulativeCosts());
		}
		
		super.setCosts(nodeCosts);
	}
	
	public int getMemoryConsumerWeight() {
		return 1;
	}
	

	@Override
	public SourceAndDamReport hasDamOnPathDownTo(PlanNode source) {
		if (source == this) {
			return FOUND_SOURCE;
		}
		
		SourceAndDamReport fromOutside = super.hasDamOnPathDownTo(source);

		if (fromOutside == FOUND_SOURCE_AND_DAM) {
			return FOUND_SOURCE_AND_DAM;
		}
		else if (fromOutside == FOUND_SOURCE) {
			// we always have a dam in the back channel
			return FOUND_SOURCE_AND_DAM;
		} else {
			// check the step function for dams
			return this.rootOfStepFunction.hasDamOnPathDownTo(source);
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

			if(branchPlan == null) {
				branchPlan = new HashMap<OptimizerNode, PlanNode>(6);
			}
			
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
