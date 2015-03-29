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

package org.apache.flink.graph.spargel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * This class is used to configure a vertex-centric iteration.
 *
 * An IterationConfiguration object can be used to set the iteration name and
 * degree of parallelism, to register aggregators and use broadcast sets in
 * the {@link VertexUpdateFunction} and {@link MessagingFunction}.
 *
 * The IterationConfiguration object is passed as an argument to
 * {@link org.apache.flink.graph.Graph#runVertexCentricIteration(
 * VertexUpdateFunction, MessagingFunction, int, IterationConfiguration)}.
 *
 */
public class IterationConfiguration {

	/** the iteration name **/
	private String name;

	/** the iteration parallelism **/
	private int parallelism = -1;

	/** the iteration aggregators **/
	private Map<String, Aggregator<?>> aggregators = new HashMap<String, Aggregator<?>>();

	/** the broadcast variables for the update function **/
	private List<Tuple2<String, DataSet<?>>> bcVarsUpdate = new ArrayList<Tuple2<String,DataSet<?>>>();

	/** the broadcast variables for the messaging function **/
	private List<Tuple2<String, DataSet<?>>> bcVarsMessaging = new ArrayList<Tuple2<String,DataSet<?>>>();

	/** flag that defines whether the solution set is kept in managed memory **/
	private boolean unmanagedSolutionSet = false;
	
	public IterationConfiguration() {}


	/**
	 * Sets the name for the vertex-centric iteration. The name is displayed in logs and messages.
	 * 
	 * @param name The name for the iteration.
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Gets the name of the vertex-centric iteration.
	 * @param defaultName 
	 * 
	 * @return The name of the iteration.
	 */
	public String getName(String defaultName) {
		if (name != null) {
			return name;			
		}
		else {
			return defaultName;
		}
	}

	/**
	 * Sets the parallelism for the iteration.
	 * 
	 * @param parallelism The parallelism.
	 */
	public void setParallelism(int parallelism) {
		Validate.isTrue(parallelism > 0 || parallelism == -1, "The parallelism must be positive, or -1 (use default).");
		this.parallelism = parallelism;
	}
	
	/**
	 * Gets the iteration's parallelism.
	 * 
	 * @return The iterations parallelism, or -1, if not set.
	 */
	public int getParallelism() {
		return parallelism;
	}

	/**
	 * Defines whether the solution set is kept in managed memory (Flink's internal way of keeping object
	 * in serialized form) or as a simple object map.
	 * By default, the solution set runs in managed memory.
	 * 
	 * @param unmanaged True, to keep the solution set in unmanaged memory, false otherwise.
	 */
	public void setSolutionSetUnmanagedMemory(boolean unmanaged) {
		this.unmanagedSolutionSet = unmanaged;
	}
	
	/**
	 * Gets whether the solution set is kept in managed memory (Flink's internal way of keeping object
	 * in serialized form) or as a simple object map.
	 * By default, the solution set runs in managed memory.
	 * 
	 * @return True, if the solution set is in unmanaged memory, false otherwise.
	 */
	public boolean isSolutionSetUnmanagedMemory() {
		return this.unmanagedSolutionSet;
	}

	/**
	 * Registers a new aggregator. Aggregators registered here are available during the execution of the vertex updates
	 * via {@link VertexUpdateFunction#getIterationAggregator(String)} and
	 * {@link VertexUpdateFunction#getPreviousIterationAggregate(String)}.
	 * 
	 * @param name The name of the aggregator, used to retrieve it and its aggregates during execution. 
	 * @param aggregator The aggregator.
	 */
	public void registerAggregator(String name, Aggregator<?> aggregator) {
		this.aggregators.put(name, aggregator);
	}
	
	/**
	 * Adds a data set as a broadcast set to the messaging function.
	 * 
	 * @param name The name under which the broadcast data is available in the messaging function.
	 * @param data The data set to be broadcasted.
	 */
	public void addBroadcastSetForMessagingFunction(String name, DataSet<?> data) {
		this.bcVarsMessaging.add(new Tuple2<String, DataSet<?>>(name, data));
	}

	/**
	 * Adds a data set as a broadcast set to the vertex update function.
	 * 
	 * @param name The name under which the broadcast data is available in the vertex update function.
	 * @param data The data set to be broadcasted.
	 */
	public void addBroadcastSetForUpdateFunction(String name, DataSet<?> data) {
		this.bcVarsUpdate.add(new Tuple2<String, DataSet<?>>(name, data));
	}

	/**
	 * Gets the set of aggregators that are registered for this vertex-centric iteration.
	 *
	 * @return a Map of the registered aggregators, where the key is the aggregator name
	 * and the value is the Aggregator object
	 */
	public Map<String, Aggregator<?>> getAggregators() {
		return this.aggregators;
	}

	/**
	 * Get the broadcast variables of the VertexUpdateFunction.
	 *
	 * @return a List of Tuple2, where the first field is the broadcast variable name
	 * and the second field is the broadcast DataSet.
	 */
	public List<Tuple2<String, DataSet<?>>> getUpdateBcastVars() {
		return this.bcVarsUpdate;
	}

	/**
	 * Get the broadcast variables of the MessagingFunction.
	 *
	 * @return a List of Tuple2, where the first field is the broadcast variable name
	 * and the second field is the broadcast DataSet.
	 */
	public List<Tuple2<String, DataSet<?>>> getMessagingBcastVars() {
		return this.bcVarsMessaging;
	}
}
