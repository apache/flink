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

package eu.stratosphere.pact.common.plan;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;

import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.generic.contract.Contract;


/**
 * This class encapsulates a Pact program (which is a form of data flow), together with some parameters, like e.g.
 * a name and a default degree of parallelism.
 * The program (data flow) is references by this plan by holding it sinks, from which a traversal reaches all connected
 * nodes. 
 */
public class Plan implements Visitable<Contract>
{
	/**
	 * A collection of all sinks in the plan. Since the plan is traversed from the sinks to the sources, this
	 * collection must contain all the sinks.
	 */
	protected final Collection<GenericDataSink> sinks;

	/**
	 * The name of the pact job.
	 */
	protected final String jobName;

	/**
	 * The default parallelism to use for nodes that have no explicitly specified parallelism.
	 */
	protected int defaultParallelism = -1;
	
	/**
	 * The maximal number of machines to use in the job.
	 */
	protected int maxNumberMachines;
	
	/**
	 * The plan's configuration 
	 */
	protected PlanConfiguration planConfiguration;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new Pact plan with the given name, describing the Pact data flow that ends at the
	 * given data sinks.
	 * <p>
	 * If not all of the sinks of a data flow are given to the plan, the flow might
	 * not be translated entirely. 
	 *  
	 * @param sinks The collection will the sinks of the plan.
	 * @param jobName The name to display for the job.
	 */
	public Plan(Collection<GenericDataSink> sinks, String jobName)
	{
		this.sinks = sinks;
		this.jobName = jobName;
		this.planConfiguration = new PlanConfiguration();
	}

	/**
	 * Creates a new Pact plan with the given name, containing initially a single data sink.
	 * <p>
	 * If not all of the sinks of a data flow are given to the plan, the flow might
	 * not be translated entirely. 
	 * 
	 * @param sink The data sink of the data flow.
	 * @param jobName The name to display for the job.
	 */
	public Plan(GenericDataSink sink, String jobName)
	{
		this.sinks = new ArrayList<GenericDataSink>();
		this.sinks.add(sink);
		this.jobName = jobName;
		this.planConfiguration = new PlanConfiguration();
	}

	/**
	 * Creates a new Pact plan, describing the Pact data flow that ends at the
	 * given data sinks. The display name for the job is generated using a timestamp.
	 * <p>
	 * If not all of the sinks of a data flow are given to the plan, the flow might
	 * not be translated entirely. 
	 *  
	 * @param sinks The collection will the sinks of the plan.
	 */
	public Plan(Collection<GenericDataSink> sinks)
	{
		this(sinks, "PACT Job at " + Calendar.getInstance().getTime());
	}

	/**
	 * Creates a new Pact plan with the given name, containing initially a single data sink.
	 * The display name for the job is generated using a timestamp.
	 * <p>
	 * If not all of the sinks of a data flow are given to the plan, the flow might
	 * not be translated entirely. 
	 * 
	 * @param sink The data sink of the data flow.
	 */
	public Plan(GenericDataSink sink)
	{
		this(sink, "PACT Job at " + Calendar.getInstance().getTime());
	}

	// ------------------------------------------------------------------------

	/**
	 * Adds a data sink to the set of sinks in this program.
	 * 
	 * @param sink The data sink to add.
	 */
	public void addDataSink(GenericDataSink sink) {
		if (!this.sinks.contains(sink)) {
			this.sinks.add(sink);
		}
	}

	/**
	 * Gets all the data sinks of this PACT program.
	 * 
	 * @return All sinks of the program.
	 */
	public Collection<GenericDataSink> getDataSinks() {
		return this.sinks;
	}

	/**
	 * Gets the name of this PACT program.
	 * 
	 * @return The name of the program.
	 */
	public String getJobName() {
		return this.jobName;
	}
	
	/**
	 * Gets the plans configuration
	 */
	public PlanConfiguration getPlanConfiguration() {
		return this.planConfiguration;
	}

	/**
	 * Gets the maximum number of machines to be used for this job.
	 * 
	 * @return The maximum number of machines to be used for this job.
	 */
	public int getMaxNumberMachines() {
		return this.maxNumberMachines;
	}

	/**
	 * Sets the maximum number of machines to be used for this job.
	 * 
	 * @param maxNumberMachines The the maximum number to set.
	 */
	public void setMaxNumberMachines(int maxNumberMachines) {
		this.maxNumberMachines = maxNumberMachines;
	}
	
	/**
	 * Gets the default degree of parallelism for this plan. That degree is always used when a Pact
	 * is not explicitly given a degree of parallelism,
	 *
	 * @return The default parallelism for the plan.
	 */
	public int getDefaultParallelism() {
		return this.defaultParallelism;
	}
	
	/**
	 * Sets the default degree of parallelism for this plan. That degree is always used when a Pact
	 * is not explicitly given a degree of parallelism,
	 *
	 * @param defaultParallelism The default parallelism for the plan.
	 */
	public void setDefaultParallelism(int defaultParallelism) {
		this.defaultParallelism = defaultParallelism;
	}
	
	/**
	 * Gets the postPassClassName from this Plan.
	 *
	 * @return The postPassClassName.
	 */
	public String getPostPassClassName() {
		return "eu.stratosphere.pact.compiler.postpass.PactRecordPostPass";
	}
	
	// ------------------------------------------------------------------------

	/**
	 * Traverses the pact plan depth first from all data sinks on towards the sources.
	 * 
	 * @see Visitable#accept(Visitor)
	 */
	@Override
	public void accept(Visitor<Contract> visitor) {
		for (GenericDataSink sink : this.sinks) {
			sink.accept(visitor);
		}
	}
}
