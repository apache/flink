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

package org.apache.flink.graph.gsa;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Collection;

/**
 * The base class for the third and last step of a {@link GatherSumApplyIteration}.
 *
 * @param <K> the vertex ID type
 * @param <VV> the vertex value type
 * @param <M> the input type (produced by the Sum phase)
 */
@SuppressWarnings("serial")
public abstract class ApplyFunction<K, VV, M> implements Serializable {

	// --------------------------------------------------------------------------------------------
	//  Attribute that allows access to the total number of vertices inside an iteration.
	// --------------------------------------------------------------------------------------------

	private long numberOfVertices = -1L;

	/**
	 * Retrieves the number of vertices in the graph.
	 * @return the number of vertices if the {@link org.apache.flink.graph.IterationConfiguration#setOptNumVertices(boolean)}
	 * option has been set; -1 otherwise.
	 */
	public long getNumberOfVertices() {
		return numberOfVertices;
	}

	void setNumberOfVertices(long numberOfVertices) {
		this.numberOfVertices = numberOfVertices;
	}

	//---------------------------------------------------------------------------------------------

	/**
	 * This method is invoked once per superstep, after the {@link SumFunction} 
	 * in a {@link GatherSumApplyIteration}.
	 * It updates the Vertex values.
	 * 
	 * @param newValue the value computed during the current superstep.
	 * @param currentValue the current Vertex value.
	 */
	public abstract void apply(M newValue, VV currentValue);

	/**
	 * Sets the result for the apply function
	 *
	 * @param result the result of the apply phase
	 */
	public void setResult(VV result) {
		outVal.f1 = result;
		out.collect(outVal);
	}

	/**
	 * This method is executed once per superstep before the vertex update function is invoked for each vertex.
	 */
	public void preSuperstep() {}

	/**
	 * This method is executed once per superstep after the vertex update function has been invoked for each vertex.
	 */
	public void postSuperstep() {}

	/**
	 * Gets the number of the superstep, starting at <tt>1</tt>.
	 *
	 * @return The number of the current superstep.
	 */
	public int getSuperstepNumber() {
		return this.runtimeContext.getSuperstepNumber();
	}

	/**
	 * Gets the iteration aggregator registered under the given name. The iteration aggregator combines
	 * all aggregates globally once per superstep and makes them available in the next superstep.
	 *
	 * @param name The name of the aggregator.
	 * @return The aggregator registered under this name, or null, if no aggregator was registered.
	 */
	public <T extends Aggregator<?>> T getIterationAggregator(String name) {
		return this.runtimeContext.getIterationAggregator(name);
	}

	/**
	 * Get the aggregated value that an aggregator computed in the previous iteration.
	 *
	 * @param name The name of the aggregator.
	 * @return The aggregated value of the previous iteration.
	 */
	public <T extends Value> T getPreviousIterationAggregate(String name) {
		return this.runtimeContext.getPreviousIterationAggregate(name);
	}

	/**
	 * Gets the broadcast data set registered under the given name. Broadcast data sets
	 * are available on all parallel instances of a function.
	 *
	 * @param name The name under which the broadcast set is registered.
	 * @return The broadcast data set.
	 */
	public <T> Collection<T> getBroadcastSet(String name) {
		return this.runtimeContext.getBroadcastVariable(name);
	}

	// --------------------------------------------------------------------------------------------
	//  Internal methods
	// --------------------------------------------------------------------------------------------

	private IterationRuntimeContext runtimeContext;

	private Collector<Vertex<K, VV>> out;

	// use a local vertex instance so that the user does not overwrite a system
	// instance used by JoinDriver
	private Vertex<K, VV> outVal = new Vertex<>();

	public void init(IterationRuntimeContext iterationRuntimeContext) {
		this.runtimeContext = iterationRuntimeContext;
	}

	public void setOutput(Vertex<K, VV> vertex, Collector<Vertex<K, VV>> out) {
		this.out = out;
		this.outVal.f0 = vertex.f0;
	}

}
