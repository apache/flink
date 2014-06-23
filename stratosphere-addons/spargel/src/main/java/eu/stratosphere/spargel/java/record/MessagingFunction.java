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
package eu.stratosphere.spargel.java.record;

import java.io.Serializable;
import java.util.Iterator;

import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.api.common.functions.IterationRuntimeContext;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

public abstract class MessagingFunction<VertexKey extends Key<VertexKey>, VertexValue extends Value, Message extends Value, EdgeValue extends Value> implements Serializable {

	// --------------------------------------------------------------------------------------------
	//  Public API Methods
	// --------------------------------------------------------------------------------------------
	
	public abstract void sendMessages(VertexKey vertexKey, VertexValue vertexValue) throws Exception;
	
	public void setup(Configuration config) throws Exception {}
	
	public void preSuperstep() throws Exception {}
	
	public void postSuperstep() throws Exception {}
	
	
	public Iterator<Edge<VertexKey, EdgeValue>> getOutgoingEdges() {
		if (edgesUsed) {
			throw new IllegalStateException("Can use either 'getOutgoingEdges()' or 'sendMessageToAllTargets()'.");
		}
		
		edgesUsed = true;
		edgeIter.set(edges);
		return edgeIter;
	}
	
	public void sendMessageToAllNeighbors(Message m) {
		if (edgesUsed) {
			throw new IllegalStateException("Can use either 'getOutgoingEdges()' or 'sendMessageToAllTargets()'.");
		}
		
		edgesUsed = true;
		while (edges.hasNext()) {
			Record next = edges.next();
			VertexKey k = next.getField(1, this.keyClass);
			outValue.setField(0, k);
			outValue.setField(1, m);
			out.collect(outValue);
		}
	}
	
	public void sendMessageTo(VertexKey target, Message m) {
		outValue.setField(0, target);
		outValue.setField(1, m);
		out.collect(outValue);
	}

	// --------------------------------------------------------------------------------------------
	
	public int getSuperstep() {
		return this.runtimeContext.getSuperstepNumber();
	}
	
	public <V, A> void addIterationAccumulator(String name, Accumulator<V, A> accumulator) {
		this.runtimeContext.addIterationAccumulator(name, accumulator);
	}
	
	public <T extends Accumulator<?, ?>> T getPreviousIterationAccumulator(String name) {
		return this.runtimeContext.<T>getPreviousIterationAccumulator(name);
	}
	

	// --------------------------------------------------------------------------------------------
	//  internal methods and state
	// --------------------------------------------------------------------------------------------
	
	private Record outValue;
	
	private IterationRuntimeContext runtimeContext;
	
	private Iterator<Record> edges;
	
	private Collector<Record> out;
	
	private EdgesIterator<VertexKey, EdgeValue> edgeIter;
	
	private Class<VertexKey> keyClass;
	
	private boolean edgesUsed;
	
	
	@SuppressWarnings("unchecked")
	void init(IterationRuntimeContext context, VertexKey keyHolder, EdgeValue edgeValueHolder) {
		this.runtimeContext = context;
		this.edgeIter = new EdgesIterator<VertexKey, EdgeValue>(keyHolder, edgeValueHolder);
		this.outValue = new Record();
		this.keyClass = (Class<VertexKey>) keyHolder.getClass();
	}
	
	void set(Iterator<Record> edges, Collector<Record> out) {
		this.edges = edges;
		this.out = out;
		this.edgesUsed = false;
	}
	
	private static final long serialVersionUID = 1L;
	
	private static final class EdgesIterator<VertexKey extends Key<VertexKey>, EdgeValue extends Value> implements Iterator<Edge<VertexKey, EdgeValue>> {

		private Iterator<Record> input;
		private VertexKey keyHolder;
		private EdgeValue edgeValueHolder;
		
		private Edge<VertexKey, EdgeValue> edge = new Edge<VertexKey, EdgeValue>();
		
		EdgesIterator(VertexKey keyHolder, EdgeValue edgeValueHolder) {
			this.keyHolder = keyHolder;
			this.edgeValueHolder = edgeValueHolder;
		}
		
		void set(Iterator<Record> input) {
			this.input = input;
		}
		
		@Override
		public boolean hasNext() {
			return input.hasNext();
		}

		@Override
		public Edge<VertexKey, EdgeValue> next() {
			Record next = input.next();
			next.getFieldInto(0, keyHolder);
			next.getFieldInto(1, edgeValueHolder);
			edge.set(keyHolder, edgeValueHolder);
			return edge;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
