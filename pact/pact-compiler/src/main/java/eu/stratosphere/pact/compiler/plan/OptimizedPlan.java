/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.util.Collection;

import eu.stratosphere.pact.common.plan.PlanConfiguration;
import eu.stratosphere.pact.common.plan.Visitable;
import eu.stratosphere.pact.common.plan.Visitor;

/**
 * The optimizer representation of a plan. The optimizer creates this from the user defined PACT job plan.
 * It works on this representation during its optimization. Finally, this plan is translated to a schedule
 * for the nephele runtime system.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 * @author Fabian Hüske (fabian.hueske@tu-berlin.de)
 */
public class OptimizedPlan implements Visitable<OptimizerNode> {
	/**
	 * The data sources in the plan.
	 */
	private final Collection<DataSourceNode> dataSources;

	/**
	 * The data sinks in the plan.
	 */
	private final Collection<DataSinkNode> dataSinks;

	/**
	 * All nodes in the optimizer plan.
	 */
	private final Collection<OptimizerNode> allNodes;

	/**
	 * Name of the PACT job
	 */
	private final String jobName;
	
	/**
	 * Configuration of the PACT job
	 */
	private PlanConfiguration planConfig;

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
	public OptimizedPlan(Collection<DataSourceNode> sources, Collection<DataSinkNode> sinks,
			Collection<OptimizerNode> allNodes, String jobName) {
		this.dataSources = sources;
		this.dataSinks = sinks;
		this.allNodes = allNodes;
		this.jobName = jobName;
	}

	/**
	 * Gets the data sources from this OptimizedPlan.
	 * 
	 * @return The data sources.
	 */
	public Collection<DataSourceNode> getDataSources() {
		return dataSources;
	}

	/**
	 * Gets the data sinks from this OptimizedPlan.
	 * 
	 * @return The data sinks.
	 */
	public Collection<DataSinkNode> getDataSinks() {
		return dataSinks;
	}

	/**
	 * Gets all the nodes from this OptimizedPlan.
	 * 
	 * @return All nodes.
	 */
	public Collection<OptimizerNode> getAllNodes() {
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
	 * Returns the configuration of the PACT job.
	 * 
	 * @return The configuration of the PACT job.
	 */
	public PlanConfiguration getPlanConfiguration() {
		return this.planConfig;
	}
	
	/**
	 * Sets the configuration of the PACT job.
	 * 
	 * @param planConfig The configuration of the PACT job.
	 */
	public void setPlanConfiguration(PlanConfiguration planConfig) {
		this.planConfig = planConfig;
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
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		for (DataSinkNode node : dataSinks) {
			node.accept(visitor);
		}
	}

}
