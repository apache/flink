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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.InaccessibleMethodException;
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
public abstract class VertexUpdateFunction<VertexKey, VertexValue, Message> implements Serializable {

	private static final long serialVersionUID = 1L;

	// --------------------------------------------------------------------------------------------
	//  Attributes that allow vertices to access their in/out degrees and the total number of vertices
	//  inside an iteration.
	// --------------------------------------------------------------------------------------------

	private long numberOfVertices = -1L;

	public long getNumberOfVertices() throws Exception{
		if (numberOfVertices == -1) {
			throw new InaccessibleMethodException("The number of vertices option is not set. " +
					"To access the number of vertices, call iterationConfiguration.setOptNumVertices(true).");
		}
		return numberOfVertices;
	}

	void setNumberOfVertices(long numberOfVertices) {
		this.numberOfVertices = numberOfVertices;
	}

	//---------------------------------------------------------------------------------------------

	private boolean optDegrees;

	public boolean isOptDegrees() {
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
	 * state is changed, it will trigger the sending of messages via the {@link MessagingFunction}.
	 * 
	 * @param vertex The vertex.
	 * @param inMessages The incoming messages to this vertex.
	 * 
	 * @throws Exception The computation may throw exceptions, which causes the superstep to fail.
	 */
	public abstract void updateVertex(Vertex<VertexKey, VertexValue> vertex, MessageIterator<Message> inMessages) throws Exception;
	
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
		if(isOptDegrees()) {
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
	 * {@link org.apache.flink.graph.spargel.VertexCentricConfiguration#addBroadcastSetForUpdateFunction(String, org.apache.flink.api.java.DataSet)}.
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
	
	private Collector<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> outWithDegrees;

	private Vertex<VertexKey, VertexValue> outVal;

	private Vertex<VertexKey, Tuple3<VertexValue, Long, Long>> outValWithDegrees;


	void init(IterationRuntimeContext context) {
		this.runtimeContext = context;
	}



	void setOutputWithDegrees(Vertex<VertexKey, Tuple3<VertexValue, Long, Long>> outValWithDegrees,
							Collector<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> outWithDegrees) {
		this.outValWithDegrees = outValWithDegrees;
		this.outWithDegrees = outWithDegrees;
	}

	void setOutput(Vertex<VertexKey, VertexValue> outVal, Collector<Vertex<VertexKey, VertexValue>> out) {
		this.outVal = outVal;
		this.out = out;
	}

	/**
	 * In order to hide the Tuple3(actualValue, inDegree, OutDegree) vertex value from the user,
	 * another function will be called from {@link org.apache.flink.graph.spargel.VertexCentricIteration}.
	 *
	 * This function will retrieve the vertex from the vertexState and will set its degrees, afterwards calling
	 * the regular updateVertex function.
	 *
	 * @param vertexState
	 * @param inMessages
	 * @throws Exception
	 */
	void updateVertexFromVertexCentricIteration(Vertex<VertexKey, Tuple3<VertexValue, Long, Long>> vertexState,
												MessageIterator<Message> inMessages) throws Exception {
		Vertex<VertexKey, VertexValue> vertex = new Vertex<VertexKey, VertexValue>(vertexState.getId(),
				vertexState.getValue().f0);
		vertex.setInDegree(vertexState.getValue().f1);
		vertex.setOutDegree(vertexState.getValue().f2);

		updateVertex(vertex, inMessages);
	}
}
