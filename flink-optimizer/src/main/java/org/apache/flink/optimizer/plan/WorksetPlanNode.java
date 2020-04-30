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


package org.apache.flink.optimizer.plan;

import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE;
import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE_AND_DAM;
import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.NOT_FOUND;

import java.util.Collections;
import java.util.HashMap;

import org.apache.flink.optimizer.costs.Costs;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.dag.WorksetNode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.runtime.operators.DamBehavior;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.util.Visitor;

/**
 * Plan candidate node for partial solution of a bulk iteration.
 */
public class WorksetPlanNode extends PlanNode {
	
	private static final Costs NO_COSTS = new Costs();
	
	private WorksetIterationPlanNode containingIterationNode;
	
	private final Channel initialInput;
	
	public Object postPassHelper;
	
	
	public WorksetPlanNode(WorksetNode template, String nodeName,
			GlobalProperties gProps, LocalProperties lProps,
			Channel initialInput)
	{
		super(template, nodeName, DriverStrategy.NONE);
		
		this.globalProps = gProps;
		this.localProps = lProps;
		this.initialInput = initialInput;
		
		// the node incurs no cost
		this.nodeCosts = NO_COSTS;
		this.cumulativeCosts = NO_COSTS;
		
		if (initialInput.getSource().branchPlan != null && initialInput.getSource().branchPlan.size() > 0) {
			if (this.branchPlan == null) {
				this.branchPlan = new HashMap<OptimizerNode, PlanNode>();
			}
			
			this.branchPlan.putAll(initialInput.getSource().branchPlan);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public WorksetNode getWorksetNode() {
		return (WorksetNode) this.template;
	}
	
	public WorksetIterationPlanNode getContainingIterationNode() {
		return this.containingIterationNode;
	}
	
	public void setContainingIterationNode(WorksetIterationPlanNode containingIterationNode) {
		this.containingIterationNode = containingIterationNode;
	}

	// --------------------------------------------------------------------------------------------
	

	@Override
	public void accept(Visitor<PlanNode> visitor) {
		if (visitor.preVisit(this)) {
			visitor.postVisit(this);
		}
	}


	@Override
	public Iterable<PlanNode> getPredecessors() {
		return Collections.<PlanNode>emptyList();
	}


	@Override
	public Iterable<Channel> getInputs() {
		return Collections.<Channel>emptyList();
	}


	@Override
	public SourceAndDamReport hasDamOnPathDownTo(PlanNode source) {
		if (source == this) {
			return FOUND_SOURCE;
		}
		SourceAndDamReport res = this.initialInput.getSource().hasDamOnPathDownTo(source);
		if (res == FOUND_SOURCE_AND_DAM) {
			return FOUND_SOURCE_AND_DAM;
		}
		else if (res == FOUND_SOURCE) {
			return (this.initialInput.getLocalStrategy().dams() || 
					this.initialInput.getTempMode().breaksPipeline() ||
					getDriverStrategy().firstDam() == DamBehavior.FULL_DAM) ?
				FOUND_SOURCE_AND_DAM : FOUND_SOURCE;
		}
		else {
			return NOT_FOUND;
		}
	}
}
