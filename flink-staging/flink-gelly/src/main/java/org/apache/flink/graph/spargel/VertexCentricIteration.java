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

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

/**
 * This class represents iterative graph computations, programmed in a vertex-centric perspective.
 * It is a special case of <i>Bulk Synchronous Parallel<i> computation. The paradigm has also been
 * implemented by Google's <i>Pregel</i> system and by <i>Apache Giraph</i>.
 * <p>
 * Vertex centric algorithms operate on graphs, which are defined through vertices and edges. The 
 * algorithms send messages along the edges and update the state of vertices based on
 * the old state and the incoming messages. All vertices have an initial state.
 * The computation terminates once no vertex updates it state any more.
 * Additionally, a maximum number of iterations (supersteps) may be specified.
 * <p>
 * The computation is here represented by two functions:
 * <ul>
 *   <li>The {@link VertexUpdateFunction} receives incoming messages and may updates the state for
 *   the vertex. If a state is updated, messages are sent from this vertex. Initially, all vertices are
 *   considered updated.</li>
 *   <li>The {@link MessagingFunction} takes the new vertex state and sends messages along the outgoing
 *   edges of the vertex. The outgoing edges may optionally have an associated value, such as a weight.</li>
 * </ul>
 * <p>
 * Vertex-centric graph iterations are instantiated by the
 * {@link #withPlainEdges(DataSet, VertexUpdateFunction, MessagingFunction, int)} method, or the
 * {@link #withValuedEdges(DataSet, VertexUpdateFunction, MessagingFunction, int)} method, depending on whether
 * the graph's edges are carrying values.
 *
 * @param <VertexKey> The type of the vertex key (the vertex identifier).
 * @param <VertexValue> The type of the vertex value (the state of the vertex).
 * @param <Message> The type of the message sent between vertices along the edges.
 * @param <EdgeValue> The type of the values that are associated with the edges.
 */
public class VertexCentricIteration<VertexKey, VertexValue,	Message, EdgeValue> 
	implements CustomUnaryOperation<Vertex<VertexKey, VertexValue>, Vertex<VertexKey, VertexValue>>
{
	private final VertexUpdateFunction<VertexKey, VertexValue, Message> updateFunction;
	
	private final MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> messagingFunction;
	
	private final DataSet<Edge<VertexKey, EdgeValue>> edgesWithValue;
	
	private final int maximumNumberOfIterations;
	
	private final TypeInformation<Message> messageType;
	
	private DataSet<Vertex<VertexKey, VertexValue>> initialVertices;

	private IterationConfiguration configuration;
	
	// ----------------------------------------------------------------------------------
	
	private VertexCentricIteration(VertexUpdateFunction<VertexKey, VertexValue, Message> uf,
			MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> mf,
			DataSet<Edge<VertexKey, EdgeValue>> edgesWithValue, 
			int maximumNumberOfIterations)
	{
		Validate.notNull(uf);
		Validate.notNull(mf);
		Validate.notNull(edgesWithValue);
		Validate.isTrue(maximumNumberOfIterations > 0, "The maximum number of iterations must be at least one.");

		this.updateFunction = uf;
		this.messagingFunction = mf;
		this.edgesWithValue = edgesWithValue;
		this.maximumNumberOfIterations = maximumNumberOfIterations;		
		this.messageType = getMessageType(mf);
	}
	
	private TypeInformation<Message> getMessageType(MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> mf) {
		return TypeExtractor.createTypeInfo(MessagingFunction.class, mf.getClass(), 2, null, null);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Custom Operator behavior
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets the input data set for this operator. In the case of this operator this input data set represents
	 * the set of vertices with their initial state.
	 * 
	 * @param inputData The input data set, which in the case of this operator represents the set of
	 *                  vertices with their initial state.
	 * 
	 * @see org.apache.flink.api.java.operators.CustomUnaryOperation#setInput(org.apache.flink.api.java.DataSet)
	 */
	@Override
	public void setInput(DataSet<Vertex<VertexKey, VertexValue>> inputData) {
		this.initialVertices = inputData;
	}
	
	/**
	 * Creates the operator that represents this vertex-centric graph computation.
	 * 
	 * @return The operator that represents this vertex-centric graph computation.
	 */
	@Override
	public DataSet<Vertex<VertexKey, VertexValue>> createResult() {
		if (this.initialVertices == null) {
			throw new IllegalStateException("The input data set has not been set.");
		}
		
		// prepare some type information
		TypeInformation<Vertex<VertexKey, VertexValue>> vertexTypes = initialVertices.getType();
		TypeInformation<VertexKey> keyType = ((TupleTypeInfo<?>) initialVertices.getType()).getTypeAt(0);
		TypeInformation<Tuple2<VertexKey, Message>> messageTypeInfo = new TupleTypeInfo<Tuple2<VertexKey,Message>>(keyType, messageType);

		final int[] zeroKeyPos = new int[] {0};
	
		final DeltaIteration<Vertex<VertexKey, VertexValue>, Vertex<VertexKey, VertexValue>> iteration =
			this.initialVertices.iterateDelta(this.initialVertices, this.maximumNumberOfIterations, zeroKeyPos);

		// set up the iteration operator
		if (this.configuration != null) {

			iteration.name(this.configuration.getName(
					"Vertex-centric iteration (" + updateFunction + " | " + messagingFunction + ")"));
			iteration.parallelism(this.configuration.getParallelism());
			iteration.setSolutionSetUnManaged(this.configuration.isSolutionSetUnmanagedMemory());

			// register all aggregators
			for (Map.Entry<String, Aggregator<?>> entry : this.configuration.getAggregators().entrySet()) {
				iteration.registerAggregator(entry.getKey(), entry.getValue());
			}
		}
		else {
			// no configuration provided; set default name
			iteration.name("Vertex-centric iteration (" + updateFunction + " | " + messagingFunction + ")");
		}
		
		// build the messaging function (co group)
		CoGroupOperator<?, ?, Tuple2<VertexKey, Message>> messages;
		MessagingUdfWithEdgeValues<VertexKey, VertexValue, Message, EdgeValue> messenger = new MessagingUdfWithEdgeValues<VertexKey, VertexValue, Message, EdgeValue>(messagingFunction, messageTypeInfo);
		messages = this.edgesWithValue.coGroup(iteration.getWorkset()).where(0).equalTo(0).with(messenger);
		
		// configure coGroup message function with name and broadcast variables
		messages = messages.name("Messaging");

		if (this.configuration != null) {
			for (Tuple2<String, DataSet<?>> e : this.configuration.getMessagingBcastVars()) {
				messages = messages.withBroadcastSet(e.f1, e.f0);
			}			
		}
		
		VertexUpdateUdf<VertexKey, VertexValue, Message> updateUdf = new VertexUpdateUdf<VertexKey, VertexValue, Message>(updateFunction, vertexTypes);
		
		// build the update function (co group)
		CoGroupOperator<?, ?, Vertex<VertexKey, VertexValue>> updates =
				messages.coGroup(iteration.getSolutionSet()).where(0).equalTo(0).with(updateUdf);
		
		// configure coGroup update function with name and broadcast variables
		updates = updates.name("Vertex State Updates");

		if (this.configuration != null) {
			for (Tuple2<String, DataSet<?>> e : this.configuration.getUpdateBcastVars()) {
				updates = updates.withBroadcastSet(e.f1, e.f0);
			}			
		}

		// let the operator know that we preserve the key field
		updates.withForwardedFieldsFirst("0").withForwardedFieldsSecond("0");
		
		return iteration.closeWith(updates, updates);
		
	}

	/**
	 * Creates a new vertex-centric iteration operator for graphs where the edges are associated with a value (such as
	 * a weight or distance).
	 * 
	 * @param edgesWithValue The data set containing edges.
	 * @param uf The function that updates the state of the vertices from the incoming messages.
	 * @param mf The function that turns changed vertex states into messages along the edges.
	 * 
	 * @param <VertexKey> The type of the vertex key (the vertex identifier).
	 * @param <VertexValue> The type of the vertex value (the state of the vertex).
	 * @param <Message> The type of the message sent between vertices along the edges.
	 * @param <EdgeValue> The type of the values that are associated with the edges.
	 * 
	 * @return An in stance of the vertex-centric graph computation operator.
	 */
	public static final <VertexKey, VertexValue, Message, EdgeValue>
			VertexCentricIteration<VertexKey, VertexValue, Message, EdgeValue> withEdges(
					DataSet<Edge<VertexKey, EdgeValue>> edgesWithValue,
					VertexUpdateFunction<VertexKey, VertexValue, Message> uf,
					MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> mf,
					int maximumNumberOfIterations)
	{
		return new VertexCentricIteration<VertexKey, VertexValue, Message, EdgeValue>(uf, mf, edgesWithValue, maximumNumberOfIterations);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Wrapping UDFs
	// --------------------------------------------------------------------------------------------
	
	private static final class VertexUpdateUdf<VertexKey, VertexValue, Message> 
		extends RichCoGroupFunction<Tuple2<VertexKey, Message>, Vertex<VertexKey, VertexValue>, Vertex<VertexKey, VertexValue>>
		implements ResultTypeQueryable<Vertex<VertexKey, VertexValue>>
	{
		private static final long serialVersionUID = 1L;
		
		private final VertexUpdateFunction<VertexKey, VertexValue, Message> vertexUpdateFunction;

		private final MessageIterator<Message> messageIter = new MessageIterator<Message>();
		
		private transient TypeInformation<Vertex<VertexKey, VertexValue>> resultType;
		
		
		private VertexUpdateUdf(VertexUpdateFunction<VertexKey, VertexValue, Message> vertexUpdateFunction,
				TypeInformation<Vertex<VertexKey, VertexValue>> resultType)
		{
			this.vertexUpdateFunction = vertexUpdateFunction;
			this.resultType = resultType;
		}

		@Override
		public void coGroup(Iterable<Tuple2<VertexKey, Message>> messages, Iterable<Vertex<VertexKey, VertexValue>> vertex,
				Collector<Vertex<VertexKey, VertexValue>> out)
			throws Exception
		{
			final Iterator<Vertex<VertexKey, VertexValue>> vertexIter = vertex.iterator();
			
			if (vertexIter.hasNext()) {
				Vertex<VertexKey, VertexValue> vertexState = vertexIter.next();
				
				@SuppressWarnings("unchecked")
				Iterator<Tuple2<?, Message>> downcastIter = (Iterator<Tuple2<?, Message>>) (Iterator<?>) messages.iterator();
				messageIter.setSource(downcastIter);
				
				vertexUpdateFunction.setOutput(vertexState, out);
				vertexUpdateFunction.updateVertex(vertexState.f0, vertexState.f1, messageIter);
			}
			else {
				final Iterator<Tuple2<VertexKey, Message>> messageIter = messages.iterator();
				if (messageIter.hasNext()) {
					String message = "Target vertex does not exist!.";
					try {
						Tuple2<VertexKey, Message> next = messageIter.next();
						message = "Target vertex '" + next.f0 + "' does not exist!.";
					} catch (Throwable t) {}
					throw new Exception(message);
				} else {
					throw new Exception();
				}
			}
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
				this.vertexUpdateFunction.init(getIterationRuntimeContext());
			}
			this.vertexUpdateFunction.preSuperstep();
		}
		
		@Override
		public void close() throws Exception {
			this.vertexUpdateFunction.postSuperstep();
		}

		@Override
		public TypeInformation<Vertex<VertexKey, VertexValue>> getProducedType() {
			return this.resultType;
		}
	}

	/*
	 * UDF that encapsulates the message sending function for graphs where the edges have an associated value.
	 */
	private static final class MessagingUdfWithEdgeValues<VertexKey, VertexValue, Message, EdgeValue> 
		extends RichCoGroupFunction<Edge<VertexKey, EdgeValue>, Vertex<VertexKey, VertexValue>, Tuple2<VertexKey, Message>>
		implements ResultTypeQueryable<Tuple2<VertexKey, Message>>
	{
		private static final long serialVersionUID = 1L;
		
		private final MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> messagingFunction;
		
		private transient TypeInformation<Tuple2<VertexKey, Message>> resultType;
		
		
		private MessagingUdfWithEdgeValues(MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> messagingFunction,
				TypeInformation<Tuple2<VertexKey, Message>> resultType)
		{
			this.messagingFunction = messagingFunction;
			this.resultType = resultType;
		}

		@Override
		public void coGroup(Iterable<Edge<VertexKey, EdgeValue>> edges,
				Iterable<Vertex<VertexKey, VertexValue>> state, Collector<Tuple2<VertexKey, Message>> out)
			throws Exception
		{
			final Iterator<Vertex<VertexKey, VertexValue>> stateIter = state.iterator();
			
			if (stateIter.hasNext()) {
				Vertex<VertexKey, VertexValue> newVertexState = stateIter.next();
				messagingFunction.set((Iterator<?>) edges.iterator(), out);
				messagingFunction.sendMessages(newVertexState.f0, newVertexState.f1);
			}
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
				this.messagingFunction.init(getIterationRuntimeContext());
			}
			
			this.messagingFunction.preSuperstep();
		}
		
		@Override
		public void close() throws Exception {
			this.messagingFunction.postSuperstep();
		}
		
		@Override
		public TypeInformation<Tuple2<VertexKey, Message>> getProducedType() {
			return this.resultType;
		}
	}

	/**
	 * Configures this vertex-centric iteration with the provided parameters.
	 *
	 * @param parameters the configuration parameters
	 */
	public void configure(IterationConfiguration parameters) {
		this.configuration = parameters;
	}

	/**
	 * @return the configuration parameters of this vertex-centric iteration
	 */
	public IterationConfiguration getIterationConfiguration() {
		return this.configuration;
	}
}
