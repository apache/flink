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

import java.io.Serializable;
import java.util.Collection;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

/**
 * This class must be extended by functions that compute the state of the vertex depending on the old state and the
 * incoming messages. The central method is {@link #updateVertex(Comparable, Object, MessageIterator)}, which is
 * invoked once per vertex per superstep.
 * 
 * <VertexKey> The vertex key type.
 * <VertexValue> The vertex value type.
 * <Message> The message type.
 */
public abstract class VertexUpdateFunction<VertexKey extends Comparable<VertexKey> & Serializable, 
	VertexValue extends Serializable, Message> implements Serializable {

	private static final long serialVersionUID = 1L;
	
	// --------------------------------------------------------------------------------------------
	//  Public API Methods
	// --------------------------------------------------------------------------------------------
	
	/**
	 * This method is invoked once per vertex per superstep. It receives the current state of the vertex, as well as
	 * the incoming messages. It may set a new vertex state via {@link #setNewVertexValue(Object)}. If the vertex
	 * state is changed, it will trigger the sending of messages via the {@link MessagingFunction}.
	 * 
	 * @param vertexKey The key (identifier) of the vertex.
	 * @param vertexValue The value (state) of the vertex.
	 * @param inMessages The incoming messages to this vertex.
	 * 
	 * @throws Exception The computation may throw exceptions, which causes the superstep to fail.
	 */
	public abstract void updateVertex(VertexKey vertexKey, VertexValue vertexValue, MessageIterator<Message> inMessages) throws Exception;
	
	/**
	 * This method is executed one per superstep before the vertex update function is invoked for each vertex.
	 * 
	 * @throws Exception Exceptions in the pre-superstep phase cause the superstep to fail.
	 */
	public void preSuperstep() throws Exception {}
	
	/**
	 * This method is executed one per superstep after the vertex update function has been invoked for each vertex.
	 * 
	 * @throws Exception Exceptions in the post-superstep phase cause the superstep to fail.
	 */
	public void postSuperstep() throws Exception {}
	
	/**
	 * Sets the new value of this vertex. Setting a new value triggers the sending of outgoing messages from this vertex.
	 * 
	 * @param newValue The new vertex value.
	 */
	public void setNewVertexValue(VertexValue newValue) {
		outVal.f1 = newValue;
		out.collect(outVal);
	}
	
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
		return this.runtimeContext.<T>getIterationAggregator(name);
	}
	
	/**
	 * Get the aggregated value that an aggregator computed in the previous iteration.
	 * 
	 * @param name The name of the aggregator.
	 * @return The aggregated value of the previous iteration.
	 */
	public <T extends Value> T getPreviousIterationAggregate(String name) {
		return this.runtimeContext.<T>getPreviousIterationAggregate(name);
	}
	
	/**
	 * Gets the broadcast data set registered under the given name. Broadcast data sets
	 * are available on all parallel instances of a function. They can be registered via
	 * {@link VertexCentricIteration#addBroadcastSetForUpdateFunction(String, org.apache.flink.api.java.DataSet)}.
	 * 
	 * @param name The name under which the broadcast set is registered.
	 * @return The broadcast data set.
	 */
	public <T> Collection<T> getBroadcastSet(String name) {
		return this.runtimeContext.<T>getBroadcastVariable(name);
	}
	
	// --------------------------------------------------------------------------------------------
	//  internal methods
	// --------------------------------------------------------------------------------------------
	
	private IterationRuntimeContext runtimeContext;
	
	private Collector<Vertex<VertexKey, VertexValue>> out;
	
	private Vertex<VertexKey, VertexValue> outVal;
	
	
	void init(IterationRuntimeContext context) {
		this.runtimeContext = context;
	}
	
	void setOutput(Vertex<VertexKey, VertexValue> val, Collector<Vertex<VertexKey, VertexValue>> out) {
		this.out = out;
		this.outVal = val;
	}
}
