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

package eu.stratosphere.pact.compiler.plan.candidate;

import java.util.Collection;

import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.util.Visitable;
import eu.stratosphere.pact.common.util.Visitor;

/**
 * The optimizer representation of a plan. The optimizer creates this from the user defined PACT job plan.
 * It works on this representation during its optimization. Finally, this plan is translated to a schedule
 * for the nephele runtime system.
 */
public class OptimizedPlan implements Visitable<PlanNode> {
	
	/**
	 * The data sources in the plan.
	 */
	private final Collection<SourcePlanNode> dataSources;

	/**
	 * The data sinks in the plan.
	 */
	private final Collection<SinkPlanNode> dataSinks;

	/**
	 * All nodes in the optimizer plan.
	 */
	private final Collection<PlanNode> allNodes;
	
	/**
	 * The original pact plan.
	 */
	private final Plan pactPlan;

	/**
	 * Name of the PACT job
	 */
	private final String jobName;

	/**
	 * The name of the instance type that is to be used.
	 */
	private String instanceTypeName;
	
	
	/**
	 * Creates a new instance of this optimizer plan container. The plan is given and fully
	 * described by the data sources, sinks and the collection of all nodes.
	 * 
	 * @param sources
	 *        The nodes describing the data sources.
	 * @param sinks
	 *        The nodes describing the data sinks.
	 * @param allNodes
	 *        A collection containing all nodes in the plan.
	 * @param jobName
	 *        The name of the PACT job
	 */
	public OptimizedPlan(Collection<SourcePlanNode> sources, Collection<SinkPlanNode> sinks,
			Collection<PlanNode> allNodes, String jobName, Plan pactPlan)
	{
		this.dataSources = sources;
		this.dataSinks = sinks;
		this.allNodes = allNodes;
		this.jobName = jobName;
		this.pactPlan = pactPlan;
	}

	/**
	 * Gets the data sources from this OptimizedPlan.
	 * 
	 * @return The data sources.
	 */
	public Collection<SourcePlanNode> getDataSources() {
		return dataSources;
	}

	/**
	 * Gets the data sinks from this OptimizedPlan.
	 * 
	 * @return The data sinks.
	 */
	public Collection<SinkPlanNode> getDataSinks() {
		return dataSinks;
	}

	/**
	 * Gets all the nodes from this OptimizedPlan.
	 * 
	 * @return All nodes.
	 */
	public Collection<PlanNode> getAllNodes() {
		return allNodes;
	}

	/**
	 * Returns the name of the optimized PACT job.
	 * 
	 * @return The name of the optimized PACT job.
	 */
	public String getJobName() {
		return this.jobName;
	}
	
	/**
	 * Gets the original pact plan from which this optimized plan was created.
	 * 
	 * @return The original pact plan.
	 */
	public Plan getOriginalPactPlan() {
		return this.pactPlan;
	}

	/**
	 * Gets the name of the instance type that should be used for this PACT job.
	 * 
	 * @return The instance-type name.
	 */
	public String getInstanceTypeName() {
		return instanceTypeName;
	}

	/**
	 * Sets the name of the instance type that should be used for this PACT job.
	 * 
	 * @param instanceTypeName
	 *        The name of the instance type.
	 */
	public void setInstanceTypeName(String instanceTypeName) {
		this.instanceTypeName = instanceTypeName;
	}

	// ------------------------------------------------------------------------

	/**
	 * Takes the given visitor and applies it top down to all nodes, starting at the sinks.
	 * 
	 * @param visitor
	 *        The visitor to apply to the nodes in this plan.
	 * @see eu.stratosphere.pact.common.util.Visitable#accept(eu.stratosphere.pact.common.util.Visitor)
	 */
	@Override
	public void accept(Visitor<PlanNode> visitor) {
		for (SinkPlanNode node : this.dataSinks) {
			node.accept(visitor);
		}
	}

}
