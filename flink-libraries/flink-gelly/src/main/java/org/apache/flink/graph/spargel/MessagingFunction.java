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
import java.util.Iterator;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

/**
 * The base class for functions that produce messages between vertices as a part of a {@link VertexCentricIteration}.
 * 
 * @param <K> The type of the vertex key (the vertex identifier).
 * @param <VV> The type of the vertex value (the state of the vertex).
 * @param <Message> The type of the message sent between vertices along the edges.
 * @param <EV> The type of the values that are associated with the edges.
 */
public abstract class MessagingFunction<K, VV, Message, EV> implements Serializable {

	private static final long serialVersionUID = 1L;

	// --------------------------------------------------------------------------------------------
	//  Attributes that allow vertices to access their in/out degrees and the total number of vertices
	//  inside an iteration.
	// --------------------------------------------------------------------------------------------

	private long numberOfVertices = -1L;

	/**
	 * Retrieves the number of vertices in the graph.
	 * @return the number of vertices if the {@link IterationConfiguration#setOptNumVertices(boolean)}
	 * option has been set; -1 otherwise.
	 */
	public long getNumberOfVertices() {
		return numberOfVertices;
	}

	void setNumberOfVertices(long numberOfVertices) {
		this.numberOfVertices = numberOfVertices;
	}

	// --------------------------------------------------------------------------------------------
	//  Attribute that allows the user to choose the neighborhood type(in/out/all) on which to run
	//  the vertex centric iteration.
	// --------------------------------------------------------------------------------------------

	private EdgeDirection direction;

	/**
	 * Retrieves the edge direction in which messages are propagated in the vertex-centric iteration.
	 * @return the messaging {@link EdgeDirection}
	 */
	public EdgeDirection getDirection() {
		return direction;
	}

	void setDirection(EdgeDirection direction) {
		this.direction = direction;
	}

	// --------------------------------------------------------------------------------------------
	//  Public API Methods
	// --------------------------------------------------------------------------------------------

	/**
	 * This method is invoked once per superstep for each vertex that was changed in that superstep.
	 * It needs to produce the messages that will be received by vertices in the next superstep.
	 * 
	 * @param vertex The vertex that was changed.
	 * 
	 * @throws Exception The computation may throw exceptions, which causes the superstep to fail.
	 */
	public abstract void sendMessages(Vertex<K, VV> vertex) throws Exception;
	
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
	 * Gets an {@link java.lang.Iterable} with all edges. This method is mutually exclusive with
	 * {@link #sendMessageToAllNeighbors(Object)} and may be called only once.
	 * 
	 * @return An iterator with all outgoing edges.
	 */
	@SuppressWarnings("unchecked")
	public Iterable<Edge<K, EV>> getEdges() {
		if (edgesUsed) {
			throw new IllegalStateException("Can use either 'getEdges()' or 'sendMessageToAllTargets()' exactly once.");
		}
		edgesUsed = true;
		this.edgeIterator.set((Iterator<Edge<K, EV>>) edges);
		return this.edgeIterator;
	}

	/**
	 * Sends the given message to all vertices that are targets of an outgoing edge of the changed vertex.
	 * This method is mutually exclusive to the method {@link #getEdges()} and may be called only once.
	 * 
	 * @param m The message to send.
	 */
	public void sendMessageToAllNeighbors(Message m) {
		if (edgesUsed) {
			throw new IllegalStateException("Can use either 'getEdges()' or 'sendMessageToAllTargets()' exactly once.");
		}
		
		edgesUsed = true;
		
		outValue.f1 = m;
		
		while (edges.hasNext()) {
			Tuple next = (Tuple) edges.next();
			K k = next.getField(1);
			outValue.f0 = k;
			out.collect(outValue);
		}
	}
	
	/**
	 * Sends the given message to the vertex identified by the given key. If the target vertex does not exist,
	 * the next superstep will cause an exception due to a non-deliverable message.
	 * 
	 * @param target The key (id) of the target vertex to message.
	 * @param m The message.
	 */
	public void sendMessageTo(K target, Message m) {
		outValue.f0 = target;
		outValue.f1 = m;
		out.collect(outValue);
	}

	// --------------------------------------------------------------------------------------------
	
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
	 * {@link org.apache.flink.graph.spargel.VertexCentricConfiguration#addBroadcastSetForMessagingFunction(String, org.apache.flink.api.java.DataSet)}.
	 * 
	 * @param name The name under which the broadcast set is registered.
	 * @return The broadcast data set.
	 */
	public <T> Collection<T> getBroadcastSet(String name) {
		return this.runtimeContext.<T>getBroadcastVariable(name);
	}

	// --------------------------------------------------------------------------------------------
	//  internal methods and state
	// --------------------------------------------------------------------------------------------
	
	private Tuple2<K, Message> outValue;
	
	private IterationRuntimeContext runtimeContext;
	
	private Iterator<?> edges;
	
	private Collector<Tuple2<K, Message>> out;
	
	private EdgesIterator<K, EV> edgeIterator;
	
	private boolean edgesUsed;

	private long inDegree = -1;

	private long outDegree = -1;
	
	void init(IterationRuntimeContext context) {
		this.runtimeContext = context;
		this.outValue = new Tuple2<K, Message>();
		this.edgeIterator = new EdgesIterator<K, EV>();
	}
	
	void set(Iterator<?> edges, Collector<Tuple2<K, Message>> out) {
		this.edges = edges;
		this.out = out;
		this.edgesUsed = false;
	}
	
	private static final class EdgesIterator<K, EV> 
		implements Iterator<Edge<K, EV>>, Iterable<Edge<K, EV>>
	{
		private Iterator<Edge<K, EV>> input;
		
		private Edge<K, EV> edge = new Edge<K, EV>();
		
		void set(Iterator<Edge<K, EV>> input) {
			this.input = input;
		}
		
		@Override
		public boolean hasNext() {
			return input.hasNext();
		}

		@Override
		public Edge<K, EV> next() {
			Edge<K, EV> next = input.next();
			edge.setSource(next.f0);
			edge.setTarget(next.f1);
			edge.setValue(next.f2);
			return edge;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		@Override
		public Iterator<Edge<K, EV>> iterator() {
			return this;
		}
	}

	/**
	 * Retrieves the vertex in-degree (number of in-coming edges).
	 * @return The in-degree of this vertex if the {@link IterationConfiguration#setOptDegrees(boolean)}
	 * option has been set; -1 otherwise. 
	 */
	public long getInDegree() {
		return inDegree;
	}

	void setInDegree(long inDegree) {
		this.inDegree = inDegree;
	}

	/**
	 * Retrieve the vertex out-degree (number of out-going edges).
	 * @return The out-degree of this vertex if the {@link IterationConfiguration#setOptDegrees(boolean)}
	 * option has been set; -1 otherwise. 
	 */
	public long getOutDegree() {
		return outDegree;
	}

	void setOutDegree(long outDegree) {
		this.outDegree = outDegree;
	}
}
