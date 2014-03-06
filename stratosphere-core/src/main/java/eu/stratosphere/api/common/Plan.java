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

package eu.stratosphere.api.common;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.util.Visitable;
import eu.stratosphere.util.Visitor;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class encapsulates a single stratosphere job (an instantiated data flow), together with some parameters.
 * Parameters include the name and a default degree of parallelism. The job is referenced by the data sinks,
 * from which a traversal reaches all connected nodes of the job.
 */
public class Plan implements Visitable<Operator> {

	private static final int DEFAULT_PARALELLISM = -1;
	
	/**
	 * A collection of all sinks in the plan. Since the plan is traversed from the sinks to the sources, this
	 * collection must contain all the sinks.
	 */
	protected final List<GenericDataSink> sinks = new ArrayList<GenericDataSink>(4);

	/**
	 * The name of the job.
	 */
	protected String jobName;

	/**
	 * The default parallelism to use for nodes that have no explicitly specified parallelism.
	 */
	protected int defaultParallelism = DEFAULT_PARALELLISM;
	
	/**
	 * The maximal number of machines to use in the job.
	 */
	protected int maxNumberMachines;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new Stratosphere job with the given name, describing the data flow that ends at the
	 * given data sinks.
	 * <p>
	 * If not all of the sinks of a data flow are given to the plan, the flow might
	 * not be translated entirely. 
	 *  
	 * @param sinks The collection will the sinks of the job's data flow.
	 * @param jobName The name to display for the job.
	 */
	public Plan(Collection<GenericDataSink> sinks, String jobName) {
		this(sinks, jobName, DEFAULT_PARALELLISM);
	}

	/**
	 * Creates a new Stratosphere job with the given name and default parallelism, describing the data flow that ends
	 * at the given data sinks.
	 * <p>
	 * If not all of the sinks of a data flow are given to the plan, the flow might
	 * not be translated entirely.
	 *
	 * @param sinks The collection will the sinks of the job's data flow.
	 * @param jobName The name to display for the job.
	 * @param defaultParallelism The default degree of parallelism for the job.
	 */
	public Plan(Collection<GenericDataSink> sinks, String jobName, int defaultParallelism) {
		this.sinks.addAll(sinks);
		this.jobName = jobName;
		this.defaultParallelism = defaultParallelism;
	}

	/**
	 * Creates a new Stratosphere job with the given name, containing initially a single data sink.
	 * <p>
	 * If not all of the sinks of a data flow are given, the flow might
	 * not be translated entirely, but only the parts of the flow reachable by traversing backwards
	 * from the given data sinks.
	 * 
	 * @param sink The data sink of the data flow.
	 * @param jobName The name to display for the job.
	 */
	public Plan(GenericDataSink sink, String jobName) {
		this(sink, jobName, DEFAULT_PARALELLISM);
	}

	/**
	 * Creates a new Stratosphere job with the given name and default parallelism, containing initially a single data
	 * sink.
	 * <p>
	 * If not all of the sinks of a data flow are given, the flow might
	 * not be translated entirely, but only the parts of the flow reachable by traversing backwards
	 * from the given data sinks.
	 *
	 * @param sink The data sink of the data flow.
	 * @param jobName The name to display for the job.
	 * @param defaultParallelism The default degree of parallelism for the job.
	 */
	public Plan(GenericDataSink sink, String jobName, int defaultParallelism) {
		this(Collections.singletonList(sink), jobName, defaultParallelism);
	}

	/**
	 * Creates a new Stratosphere job, describing the data flow that ends at the
	 * given data sinks. The display name for the job is generated using a timestamp.
	 * <p>
	 * If not all of the sinks of a data flow are given, the flow might
	 * not be translated entirely, but only the parts of the flow reachable by traversing backwards
	 * from the given data sinks. 
	 *  
	 * @param sinks The collection will the sinks of the data flow.
	 */
	public Plan(Collection<GenericDataSink> sinks) {
		this(sinks, DEFAULT_PARALELLISM);
	}

	/**
	 * Creates a new Stratosphere job with the given default parallelism, describing the data flow that ends at the
	 * given data sinks. The display name for the job is generated using a timestamp.
	 * <p>
	 * If not all of the sinks of a data flow are given, the flow might
	 * not be translated entirely, but only the parts of the flow reachable by traversing backwards
	 * from the given data sinks.
	 *
	 * @param sinks The collection will the sinks of the data flow.
	 * @param defaultParallelism The default degree of parallelism for the job.
	 */
	public Plan(Collection<GenericDataSink> sinks, int defaultParallelism) {
		this(sinks, "Stratosphere Job at " + Calendar.getInstance().getTime(), defaultParallelism);
	}

	/**
	 * Creates a new Stratosphere Job with single data sink.
	 * The display name for the job is generated using a timestamp.
	 * <p>
	 * If not all of the sinks of a data flow are given to the plan, the flow might
	 * not be translated entirely. 
	 * 
	 * @param sink The data sink of the data flow.
	 */
	public Plan(GenericDataSink sink) {
		this(sink, DEFAULT_PARALELLISM);
	}

	/**
	 * Creates a new Stratosphere Job with single data sink and the given default parallelism.
	 * The display name for the job is generated using a timestamp.
	 * <p>
	 * If not all of the sinks of a data flow are given to the plan, the flow might
	 * not be translated entirely.
	 *
	 * @param sink The data sink of the data flow.
	 * @param defaultParallelism The default degree of parallelism for the job.
	 */
	public Plan(GenericDataSink sink, int defaultParallelism) {
		this(sink, "Stratosphere Job at " + Calendar.getInstance().getTime(), defaultParallelism);
	}

	// ------------------------------------------------------------------------

	/**
	 * Adds a data sink to the set of sinks in this program.
	 * 
	 * @param sink The data sink to add.
	 */
	public void addDataSink(GenericDataSink sink) {
		checkNotNull(jobName, "The data sink must not be null.");
		
		if (!this.sinks.contains(sink)) {
			this.sinks.add(sink);
		}
	}

	/**
	 * Gets all the data sinks of this job.
	 * 
	 * @return All sinks of the program.
	 */
	public Collection<GenericDataSink> getDataSinks() {
		return this.sinks;
	}

	/**
	 * Gets the name of this job.
	 * 
	 * @return The name of the job.
	 */
	public String getJobName() {
		return this.jobName;
	}
	
	/**
	 * Sets the jobName for this Plan.
	 *
	 * @param jobName The jobName to set.
	 */
	public void setJobName(String jobName) {
		checkNotNull(jobName, "The job name must not be null.");
		this.jobName = jobName;
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
		if (maxNumberMachines == 0 || maxNumberMachines < -1) {
			throw new IllegalArgumentException("The maximum number of machines must be positive, or -1 if no limit is imposed.");
		}
		
		this.maxNumberMachines = maxNumberMachines;
	}
	
	/**
	 * Gets the default degree of parallelism for this job. That degree is always used when an operator
	 * is not explicitly given a degree of parallelism.
	 *
	 * @return The default parallelism for the plan.
	 */
	public int getDefaultParallelism() {
		return this.defaultParallelism;
	}
	
	/**
	 * Sets the default degree of parallelism for this plan. That degree is always used when an operator
	 * is not explicitly given a degree of parallelism.
	 *
	 * @param defaultParallelism The default parallelism for the plan.
	 */
	public void setDefaultParallelism(int defaultParallelism) {
		checkArgument(defaultParallelism >= 1 || defaultParallelism == -1,
			"The default degree of parallelism must be positive, or -1 if the system should use the globally comfigured default.");
		
		this.defaultParallelism = defaultParallelism;
	}
	
	/**
	 * Gets the optimizer post-pass class for this job. The post-pass typically creates utility classes
	 * for data types and is specific to a particular data model (record, tuple, Scala, ...)
	 *
	 * @return The name of the class implementing the optimizer post-pass.
	 */
	public String getPostPassClassName() {
		return "eu.stratosphere.compiler.postpass.RecordModelPostPass";
	}
	
	// ------------------------------------------------------------------------

	/**
	 * Traverses the job depth first from all data sinks on towards the sources.
	 * 
	 * @see Visitable#accept(Visitor)
	 */
	@Override
	public void accept(Visitor<Operator> visitor) {
		for (GenericDataSink sink : this.sinks) {
			sink.accept(visitor);
		}
	}
}
