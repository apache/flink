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

package org.apache.flink.graph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * This is used as a base class for vertex-centric iteration or gather-sum-apply iteration configuration.
 */
public abstract class IterationConfiguration {

	// the iteration name
	private String name;

	// the iteration parallelism
	private int parallelism = -1;

	// the iteration aggregators
	private Map<String, Aggregator<?>> aggregators = new HashMap<>();

	// flag that defines whether the solution set is kept in managed memory
	private boolean unmanagedSolutionSet = false;

	// flag that defines whether the number of vertices option is set
	private boolean optNumVertices = false;

	public IterationConfiguration() {}

	/**
	 * Sets the name for the iteration. The name is displayed in logs and messages.
	 *
	 * @param name The name for the iteration.
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Gets the name of the iteration.
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
		Preconditions.checkArgument(
				parallelism > 0 || parallelism == ExecutionConfig.PARALLELISM_DEFAULT,
				"The parallelism must be at least one, or ExecutionConfig.PARALLELISM_DEFAULT (use system default).");
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
	 * Gets whether the number of vertices option is set.
	 * By default, the number of vertices option is not set.
	 *
	 * @return True, if the number of vertices option is set, false otherwise.
	 */
	public boolean isOptNumVertices() {
		return optNumVertices;
	}

	/**
	 * Sets the number of vertices option.
	 * By default, the number of vertices option is not set.
	 *
	 * @param optNumVertices True, to set this option, false otherwise.
	 */
	public void setOptNumVertices(boolean optNumVertices) {
		this.optNumVertices = optNumVertices;
	}

	/**
	 * Registers a new aggregator. Aggregators registered here are available during the execution of the vertex updates
	 * via {@link GatherFunction#getIterationAggregator(String)} and
	 * {@link GatherFunction#getPreviousIterationAggregate(String)}.
	 *
	 * @param name The name of the aggregator, used to retrieve it and its aggregates during execution.
	 * @param aggregator The aggregator.
	 */
	public void registerAggregator(String name, Aggregator<?> aggregator) {
		this.aggregators.put(name, aggregator);
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
}
