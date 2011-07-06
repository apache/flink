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

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;

/**
 * 
 */
public class Plan implements Visitable<Contract> {
	/**
	 * A collection of all sinks in the plan. Since the plan is traversed from the sinks to the sources, this
	 * collection must contain all the sinks.
	 */
	protected final Collection<FileDataSinkContract<?, ?>> sinks;

	/**
	 * The name of the pact job.
	 */
	protected final String jobName;

	/**
	 * The maximal number of machines to use in the job.
	 */
	protected int maxNumberMachines;

	// ------------------------------------------------------------------------

	public Plan(Collection<FileDataSinkContract<?, ?>> sinks, String jobName) {
		this.sinks = sinks;
		this.jobName = jobName;
	}

	public Plan(FileDataSinkContract<?, ?> sink, String jobName) {
		this.sinks = new ArrayList<FileDataSinkContract<?, ?>>();
		this.sinks.add(sink);
		this.jobName = jobName;
	}

	public Plan(Collection<FileDataSinkContract<?, ?>> sinks) {
		this(sinks, "PACT Job at " + Calendar.getInstance().getTime());
	}

	public Plan(FileDataSinkContract<?, ?> sink) {
		this(sink, "PACT Job at " + Calendar.getInstance().getTime());
	}

	// ------------------------------------------------------------------------

	/**
	 * Adds a data sink to the set of sinks in this program.
	 * 
	 * @param sink
	 *        The data sink to add.
	 */
	public void addDataSink(FileDataSinkContract<?, ?> sink) {
		if (!sinks.contains(sink)) {
			sinks.add(sink);
		}
	}

	/**
	 * Gets all the data sinks of this PACT program.
	 * 
	 * @return All sinks of the program.
	 */
	public Collection<FileDataSinkContract<?, ?>> getDataSinks() {
		return sinks;
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
	 * Gets the maximum number of machines to be used for this job.
	 * 
	 * @return The maximum number of machines to be used for this job.
	 */
	public int getMaxNumberMachines() {
		return maxNumberMachines;
	}

	/**
	 * Sets the maximum number of machines to be used for this job.
	 * 
	 * @param maxNumberMachines
	 *        The the maximum number to set.
	 */
	public void setMaxNumberMachines(int maxNumberMachines) {
		this.maxNumberMachines = maxNumberMachines;
	}

	// ------------------------------------------------------------------------

	/**
	 * Traverses the pact plan, starting from the data sinks that were added to this program.
	 * 
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<Contract> visitor) {
		for (FileDataSinkContract<?, ?> sink : sinks) {
			sink.accept(visitor);
		}
	}

}
