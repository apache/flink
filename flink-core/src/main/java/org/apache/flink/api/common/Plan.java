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


package org.apache.flink.api.common;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.flink.api.common.cache.DistributedCache.DistributedCacheEntry;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Visitable;
import org.apache.flink.util.Visitor;

/**
 * This class encapsulates a single job (an instantiated data flow), together with some parameters.
 * Parameters include the name and a default degree of parallelism. The job is referenced by the data sinks,
 * from which a traversal reaches all connected nodes of the job.
 */
public class Plan implements Visitable<Operator<?>> {

	private static final int DEFAULT_PARALELLISM = -1;
	
	/**
	 * A collection of all sinks in the plan. Since the plan is traversed from the sinks to the sources, this
	 * collection must contain all the sinks.
	 */
	protected final List<GenericDataSinkBase<?>> sinks = new ArrayList<GenericDataSinkBase<?>>(4);

	/**
	 * The name of the job.
	 */
	protected String jobName;

	/**
	 * The default parallelism to use for nodes that have no explicitly specified parallelism.
	 */
	protected int defaultParallelism = DEFAULT_PARALELLISM;

	/**
	 * Hash map for files in the distributed cache: registered name to cache entry.
	 */
	protected HashMap<String, DistributedCacheEntry> cacheFile = new HashMap<String, DistributedCacheEntry>();

	// ------------------------------------------------------------------------

	/**
	 * Creates a new program plan with the given name, describing the data flow that ends at the
	 * given data sinks.
	 * <p>
	 * If not all of the sinks of a data flow are given to the plan, the flow might
	 * not be translated entirely. 
	 *  
	 * @param sinks The collection will the sinks of the job's data flow.
	 * @param jobName The name to display for the job.
	 */
	public Plan(Collection<? extends GenericDataSinkBase<?>> sinks, String jobName) {
		this(sinks, jobName, DEFAULT_PARALELLISM);
	}

	/**
	 * Creates a new program plan with the given name and default parallelism, describing the data flow that ends
	 * at the given data sinks.
	 * <p>
	 * If not all of the sinks of a data flow are given to the plan, the flow might
	 * not be translated entirely.
	 *
	 * @param sinks The collection will the sinks of the job's data flow.
	 * @param jobName The name to display for the job.
	 * @param defaultParallelism The default degree of parallelism for the job.
	 */
	public Plan(Collection<? extends GenericDataSinkBase<?>> sinks, String jobName, int defaultParallelism) {
		this.sinks.addAll(sinks);
		this.jobName = jobName;
		this.defaultParallelism = defaultParallelism;
	}

	/**
	 * Creates a new program plan with the given name, containing initially a single data sink.
	 * <p>
	 * If not all of the sinks of a data flow are given, the flow might
	 * not be translated entirely, but only the parts of the flow reachable by traversing backwards
	 * from the given data sinks.
	 * 
	 * @param sink The data sink of the data flow.
	 * @param jobName The name to display for the job.
	 */
	public Plan(GenericDataSinkBase<?> sink, String jobName) {
		this(sink, jobName, DEFAULT_PARALELLISM);
	}

	/**
	 * Creates a new program plan with the given name and default parallelism, containing initially a single data
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
	public Plan(GenericDataSinkBase<?> sink, String jobName, int defaultParallelism) {
		this(Collections.<GenericDataSinkBase<?>>singletonList(sink), jobName, defaultParallelism);
	}

	/**
	 * Creates a new program plan, describing the data flow that ends at the
	 * given data sinks. The display name for the job is generated using a timestamp.
	 * <p>
	 * If not all of the sinks of a data flow are given, the flow might
	 * not be translated entirely, but only the parts of the flow reachable by traversing backwards
	 * from the given data sinks. 
	 *  
	 * @param sinks The collection will the sinks of the data flow.
	 */
	public Plan(Collection<? extends GenericDataSinkBase<?>> sinks) {
		this(sinks, DEFAULT_PARALELLISM);
	}

	/**
	 * Creates a new program plan with the given default parallelism, describing the data flow that ends at the
	 * given data sinks. The display name for the job is generated using a timestamp.
	 * <p>
	 * If not all of the sinks of a data flow are given, the flow might
	 * not be translated entirely, but only the parts of the flow reachable by traversing backwards
	 * from the given data sinks.
	 *
	 * @param sinks The collection will the sinks of the data flow.
	 * @param defaultParallelism The default degree of parallelism for the job.
	 */
	public Plan(Collection<? extends GenericDataSinkBase<?>> sinks, int defaultParallelism) {
		this(sinks, "Flink Job at " + Calendar.getInstance().getTime(), defaultParallelism);
	}

	/**
	 * Creates a new program plan with single data sink.
	 * The display name for the job is generated using a timestamp.
	 * <p>
	 * If not all of the sinks of a data flow are given to the plan, the flow might
	 * not be translated entirely. 
	 * 
	 * @param sink The data sink of the data flow.
	 */
	public Plan(GenericDataSinkBase<?> sink) {
		this(sink, DEFAULT_PARALELLISM);
	}

	/**
	 * Creates a new program plan with single data sink and the given default parallelism.
	 * The display name for the job is generated using a timestamp.
	 * <p>
	 * If not all of the sinks of a data flow are given to the plan, the flow might
	 * not be translated entirely.
	 *
	 * @param sink The data sink of the data flow.
	 * @param defaultParallelism The default degree of parallelism for the job.
	 */
	public Plan(GenericDataSinkBase<?> sink, int defaultParallelism) {
		this(sink, "Flink Job at " + Calendar.getInstance().getTime(), defaultParallelism);
	}

	// ------------------------------------------------------------------------

	/**
	 * Adds a data sink to the set of sinks in this program.
	 * 
	 * @param sink The data sink to add.
	 */
	public void addDataSink(GenericDataSinkBase<?> sink) {
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
	public Collection<? extends GenericDataSinkBase<?>> getDataSinks() {
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
		return "org.apache.flink.compiler.postpass.RecordModelPostPass";
	}
	
	// ------------------------------------------------------------------------

	/**
	 * Traverses the job depth first from all data sinks on towards the sources.
	 * 
	 * @see Visitable#accept(Visitor)
	 */
	@Override
	public void accept(Visitor<Operator<?>> visitor) {
		for (GenericDataSinkBase<?> sink : this.sinks) {
			sink.accept(visitor);
		}
	}
	
	/**
	 *  register cache files in program level
	 * @param entry contains all relevant information
	 * @param name user defined name of that file
	 * @throws java.io.IOException
	 */
	public void registerCachedFile(String name, DistributedCacheEntry entry) throws IOException {
		if (!this.cacheFile.containsKey(name)) {
			try {
				URI u = new URI(entry.filePath);
				if (!u.getPath().startsWith("/")) {
					u = new File(entry.filePath).toURI();
				}
				FileSystem fs = FileSystem.get(u);
				if (fs.exists(new Path(u.getPath()))) {
					this.cacheFile.put(name, new DistributedCacheEntry(u.toString(), entry.isExecutable));
				} else {
					throw new IOException("File " + u.toString() + " doesn't exist.");
				}
			} catch (URISyntaxException ex) {
				throw new IOException("Invalid path: " + entry.filePath, ex);
			}
		} else {
			throw new IOException("cache file " + name + "already exists!");
		}
	}

	/**
	 * return the registered caches files
	 * @return Set of (name, filePath) pairs
	 */
	public Set<Entry<String,DistributedCacheEntry>> getCachedFiles() {
		return this.cacheFile.entrySet();
	}
	
	public int getMaximumParallelism() {
		MaxDopVisitor visitor = new MaxDopVisitor();
		accept(visitor);
		return Math.max(visitor.maxDop, this.defaultParallelism);
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class MaxDopVisitor implements Visitor<Operator<?>> {

		private int maxDop = -1;
		
		@Override
		public boolean preVisit(Operator<?> visitable) {
			this.maxDop = Math.max(this.maxDop, visitable.getDegreeOfParallelism());
			return true;
		}

		@Override
		public void postVisit(Operator<?> visitable) {}
	}
}
