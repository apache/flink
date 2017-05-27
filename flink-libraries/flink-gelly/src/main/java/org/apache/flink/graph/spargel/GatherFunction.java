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

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Collection;

/**
 * This class must be extended by functions that compute the state of the vertex depending on the old state and the
 * incoming messages. The central method is {@link #updateVertex(Vertex, MessageIterator)}, which is
 * invoked once per vertex per superstep.
 *
 * {@code <K>} The vertex key type.
 * {@code <VV>} The vertex value type.
 * {@code <Message>} The message type.
 */
public abstract class GatherFunction<K, VV, Message> implements Serializable {

	private static final long serialVersionUID = 1L;

	// --------------------------------------------------------------------------------------------
	//  Attributes that allow vertices to access their in/out degrees and the total number of vertices
	//  inside an iteration.
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

	private boolean optDegrees;

	boolean isOptDegrees() {
		return optDegrees;
	}

	void setOptDegrees(boolean optDegrees) {
		this.optDegrees = optDegrees;
	}

	// --------------------------------------------------------------------------------------------
	//  Public API Methods
	// --------------------------------------------------------------------------------------------

	/**
	 * This method is invoked once per vertex per superstep. It receives the current state of the vertex, as well as
	 * the incoming messages. It may set a new vertex state via {@link #setNewVertexValue(Object)}. If the vertex
	 * state is changed, it will trigger the sending of messages via the {@link ScatterFunction}.
	 *
	 * @param vertex The vertex.
	 * @param inMessages The incoming messages to this vertex.
	 *
	 * @throws Exception The computation may throw exceptions, which causes the superstep to fail.
	 */
	public abstract void updateVertex(Vertex<K, VV> vertex, MessageIterator<Message> inMessages) throws Exception;

	/**
	 * This method is executed once per superstep before the gather function is invoked for each vertex.
	 *
	 * @throws Exception Exceptions in the pre-superstep phase cause the superstep to fail.
	 */
	public void preSuperstep() throws Exception {}

	/**
	 * This method is executed once per superstep after the gather function has been invoked for each vertex.
	 *
	 * @throws Exception Exceptions in the post-superstep phase cause the superstep to fail.
	 */
	public void postSuperstep() throws Exception {}

	/**
	 * Sets the new value of this vertex. Setting a new value triggers the sending of outgoing messages from this vertex.
	 *
	 * <p>This should be called at most once per updateVertex.
	 *
	 * @param newValue The new vertex value.
	 */
	public void setNewVertexValue(VV newValue) {
		if (setNewVertexValueCalled) {
			throw new IllegalStateException("setNewVertexValue should only be called at most once per updateVertex");
		}
		setNewVertexValueCalled = true;
		if (isOptDegrees()) {
			outValWithDegrees.f1.f0 = newValue;
			outWithDegrees.collect(outValWithDegrees);
		} else {
			outVal.setValue(newValue);
			out.collect(outVal);
		}
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
	 * are available on all parallel instances of a function. They can be registered via
	 * {@link org.apache.flink.graph.spargel.ScatterGatherConfiguration#addBroadcastSetForGatherFunction(String, org.apache.flink.api.java.DataSet)}.
	 *
	 * @param name The name under which the broadcast set is registered.
	 * @return The broadcast data set.
	 */
	public <T> Collection<T> getBroadcastSet(String name) {
		return this.runtimeContext.getBroadcastVariable(name);
	}

	// --------------------------------------------------------------------------------------------
	//  internal methods
	// --------------------------------------------------------------------------------------------

	private IterationRuntimeContext runtimeContext;

	private Collector<Vertex<K, VV>> out;

	private Collector<Vertex<K, Tuple3<VV, Long, Long>>> outWithDegrees;

	private Vertex<K, VV> outVal;

	private Vertex<K, Tuple3<VV, Long, Long>> outValWithDegrees;

	private long inDegree = -1;

	private long outDegree = -1;

	private boolean setNewVertexValueCalled;

	void init(IterationRuntimeContext context) {
		this.runtimeContext = context;
	}

	void setOutput(Vertex<K, VV> outVal, Collector<Vertex<K, VV>> out) {
		this.outVal = outVal;
		this.out = out;
		setNewVertexValueCalled = false;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	<ValueWithDegree> void setOutputWithDegrees(Vertex<K, ValueWithDegree> outVal,
			Collector out) {
		this.outValWithDegrees = (Vertex<K, Tuple3<VV, Long, Long>>) outVal;
		this.outWithDegrees = out;
		setNewVertexValueCalled = false;
	}

	/**
	 * Retrieves the vertex in-degree (number of in-coming edges).
	 * @return The in-degree of this vertex
	 */
	public long getInDegree() {
		return inDegree;
	}

	void setInDegree(long inDegree) {
		this.inDegree = inDegree;
	}

	/**
	 * Retrieve the vertex out-degree (number of out-going edges).
	 * @return The out-degree of this vertex
	 */
	public long getOutDegree() {
		return outDegree;
	}

	void setOutDegree(long outDegree) {
		this.outDegree = outDegree;
	}

	/**
	 * In order to hide the Tuple3(actualValue, inDegree, OutDegree) vertex value from the user,
	 * another function will be called from {@link org.apache.flink.graph.spargel.ScatterGatherIteration}.
	 *
	 * <p>This function will retrieve the vertex from the vertexState and will set its degrees, afterwards calling
	 * the regular updateVertex function.
	 *
	 * @param vertexState
	 * @param inMessages
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	<VertexWithDegree> void updateVertexFromScatterGatherIteration(Vertex<K, VertexWithDegree> vertexState,
												MessageIterator<Message> inMessages) throws Exception {

		Vertex<K, VV> vertex = new Vertex<>(vertexState.f0,
			((Tuple3<VV, Long, Long>) vertexState.getValue()).f0);

		updateVertex(vertex, inMessages);
	}
}
