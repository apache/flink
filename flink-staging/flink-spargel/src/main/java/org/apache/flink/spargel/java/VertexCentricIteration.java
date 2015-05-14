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

package org.apache.flink.spargel.java;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.google.common.base.Preconditions;

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
@Deprecated
public class VertexCentricIteration<VertexKey extends Comparable<VertexKey>, VertexValue, Message, EdgeValue> 
	implements CustomUnaryOperation<Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, VertexValue>>
{
	private final VertexUpdateFunction<VertexKey, VertexValue, Message> updateFunction;
	
	private final MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> messagingFunction;
	
	private final DataSet<Tuple2<VertexKey, VertexKey>> edgesWithoutValue;
	
	private final DataSet<Tuple3<VertexKey, VertexKey, EdgeValue>> edgesWithValue;
	
	private final Map<String, Aggregator<?>> aggregators;
	
	private final int maximumNumberOfIterations;
	
	private final List<Tuple2<String, DataSet<?>>> bcVarsUpdate = new ArrayList<Tuple2<String,DataSet<?>>>(4);
	
	private final List<Tuple2<String, DataSet<?>>> bcVarsMessaging = new ArrayList<Tuple2<String,DataSet<?>>>(4);
	
	private final TypeInformation<Message> messageType;
	
	private DataSet<Tuple2<VertexKey, VertexValue>> initialVertices;
	
	private String name;
	
	private int parallelism = -1;
	
	private boolean unmanagedSolutionSet;
	
	// ----------------------------------------------------------------------------------
	
	private  VertexCentricIteration(VertexUpdateFunction<VertexKey, VertexValue, Message> uf,
			MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> mf,
			DataSet<Tuple2<VertexKey, VertexKey>> edgesWithoutValue,
			int maximumNumberOfIterations)
	{
		Preconditions.checkNotNull(uf);
		Preconditions.checkNotNull(mf);
		Preconditions.checkNotNull(edgesWithoutValue);
		Preconditions.checkArgument(maximumNumberOfIterations > 0, "The maximum number of iterations must be at least one.");
		
		// check that the edges are actually a valid tuple set of vertex key types
		TypeInformation<Tuple2<VertexKey, VertexKey>> edgesType = edgesWithoutValue.getType();
		Preconditions.checkArgument(edgesType.isTupleType() && edgesType.getArity() == 2, "The edges data set (for edges without edge values) must consist of 2-tuples.");
		
		TupleTypeInfo<?> tupleInfo = (TupleTypeInfo<?>) edgesType;
		Preconditions.checkArgument(tupleInfo.getTypeAt(0).equals(tupleInfo.getTypeAt(1))
			&& Comparable.class.isAssignableFrom(tupleInfo.getTypeAt(0).getTypeClass()),
			"Both tuple fields (source and target vertex id) must be of the data type that represents the vertex key and implement the java.lang.Comparable interface.");
		
		this.updateFunction = uf;
		this.messagingFunction = mf;
		this.edgesWithoutValue = edgesWithoutValue;
		this.edgesWithValue = null;
		this.maximumNumberOfIterations = maximumNumberOfIterations;
		this.aggregators = new HashMap<String, Aggregator<?>>();
		
		this.messageType = getMessageType(mf);
	}
	
	private VertexCentricIteration(VertexUpdateFunction<VertexKey, VertexValue, Message> uf,
			MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> mf,
			DataSet<Tuple3<VertexKey, VertexKey, EdgeValue>> edgesWithValue, 
			int maximumNumberOfIterations,
			boolean edgeHasValueMarker)
	{
		Preconditions.checkNotNull(uf);
		Preconditions.checkNotNull(mf);
		Preconditions.checkNotNull(edgesWithValue);
		Preconditions.checkArgument(maximumNumberOfIterations > 0, "The maximum number of iterations must be at least one.");
		
		// check that the edges are actually a valid tuple set of vertex key types
		TypeInformation<Tuple3<VertexKey, VertexKey, EdgeValue>> edgesType = edgesWithValue.getType();
		Preconditions.checkArgument(edgesType.isTupleType() && edgesType.getArity() == 3, "The edges data set (for edges with edge values) must consist of 3-tuples.");
		
		TupleTypeInfo<?> tupleInfo = (TupleTypeInfo<?>) edgesType;
		Preconditions.checkArgument(tupleInfo.getTypeAt(0).equals(tupleInfo.getTypeAt(1))
			&& Comparable.class.isAssignableFrom(tupleInfo.getTypeAt(0).getTypeClass()),
			"The first two tuple fields (source and target vertex id) must be of the data type that represents the vertex key and implement the java.lang.Comparable interface.");
		
		Preconditions.checkArgument(maximumNumberOfIterations > 0, "The maximum number of iterations must be at least one.");
		
		this.updateFunction = uf;
		this.messagingFunction = mf;
		this.edgesWithoutValue = null;
		this.edgesWithValue = edgesWithValue;
		this.maximumNumberOfIterations = maximumNumberOfIterations;
		this.aggregators = new HashMap<String, Aggregator<?>>();
		
		this.messageType = getMessageType(mf);
	}
	
	private TypeInformation<Message> getMessageType(MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> mf) {
		return TypeExtractor.createTypeInfo(MessagingFunction.class, mf.getClass(), 2, null, null);
	}
	
	/**
	 * Registers a new aggregator. Aggregators registered here are available during the execution of the vertex updates
	 * via {@link VertexUpdateFunction#getIterationAggregator(String)} and
	 * {@link VertexUpdateFunction#getPreviousIterationAggregate(String)}.
	 * 
	 * @param name The name of the aggregator, used to retrieve it and its aggregates during execution. 
	 * @param aggregator The aggregator.
	 */
	@Deprecated
	public void registerAggregator(String name, Aggregator<?> aggregator) {
		this.aggregators.put(name, aggregator);
	}
	
	/**
	 * Adds a data set as a broadcast set to the messaging function.
	 * 
	 * @param name The name under which the broadcast data is available in the messaging function.
	 * @param data The data set to be broadcasted.
	 */
	@Deprecated
	public void addBroadcastSetForMessagingFunction(String name, DataSet<?> data) {
		this.bcVarsMessaging.add(new Tuple2<String, DataSet<?>>(name, data));
	}

	/**
	 * Adds a data set as a broadcast set to the vertex update function.
	 * 
	 * @param name The name under which the broadcast data is available in the vertex update function.
	 * @param data The data set to be broadcasted.
	 */
	@Deprecated
	public void addBroadcastSetForUpdateFunction(String name, DataSet<?> data) {
		this.bcVarsUpdate.add(new Tuple2<String, DataSet<?>>(name, data));
	}
	
	/**
	 * Sets the name for the vertex-centric iteration. The name is displayed in logs and messages.
	 * 
	 * @param name The name for the iteration.
	 */
	@Deprecated
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * Gets the name from this vertex-centric iteration.
	 * 
	 * @return The name of the iteration.
	 */
	@Deprecated
	public String getName() {
		return name;
	}
	
	/**
	 * Sets the parallelism for the iteration.
	 * 
	 * @param parallelism The parallelism.
	 */
	@Deprecated
	public void setParallelism(int parallelism) {
		Preconditions.checkArgument(parallelism > 0 || parallelism == -1, "The parallelism must be positive, or -1 (use default).");
		this.parallelism = parallelism;
	}
	
	/**
	 * Gets the iteration's parallelism.
	 * 
	 * @return The iterations parallelism, or -1, if not set.
	 */
	@Deprecated
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
	@Deprecated
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
	@Deprecated
	public boolean isSolutionSetUnmanagedMemory() {
		return this.unmanagedSolutionSet;
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
	public void setInput(DataSet<Tuple2<VertexKey, VertexValue>> inputData) {
		// sanity check that we really have two tuples
		TypeInformation<Tuple2<VertexKey, VertexValue>> inputType = inputData.getType();
		Preconditions.checkArgument(inputType.isTupleType() && inputType.getArity() == 2, "The input data set (the initial vertices) must consist of 2-tuples.");

		// check that the key type here is the same as for the edges
		TypeInformation<VertexKey> keyType = ((TupleTypeInfo<?>) inputType).getTypeAt(0);
		TypeInformation<?> edgeType = edgesWithoutValue != null ? edgesWithoutValue.getType() : edgesWithValue.getType();
		TypeInformation<VertexKey> edgeKeyType = ((TupleTypeInfo<?>) edgeType).getTypeAt(0);
		
		Preconditions.checkArgument(keyType.equals(edgeKeyType), "The first tuple field (the vertex id) of the input data set (the initial vertices) " +
				"must be the same data type as the first fields of the edge data set (the source vertex id). " +
				"Here, the key type for the vertex ids is '%s' and the key type  for the edges is '%s'.", keyType, edgeKeyType);

		this.initialVertices = inputData;
	}
	
	/**
	 * Creates the operator that represents this vertex-centric graph computation.
	 * 
	 * @return The operator that represents this vertex-centric graph computation.
	 */
	@Override
	public DataSet<Tuple2<VertexKey, VertexValue>> createResult() {
		if (this.initialVertices == null) {
			throw new IllegalStateException("The input data set has not been set.");
		}
		
		// prepare some type information
		TypeInformation<Tuple2<VertexKey, VertexValue>> vertexTypes = initialVertices.getType();
		TypeInformation<VertexKey> keyType = ((TupleTypeInfo<?>) initialVertices.getType()).getTypeAt(0);
		TypeInformation<Tuple2<VertexKey, Message>> messageTypeInfo = new TupleTypeInfo<Tuple2<VertexKey,Message>>(keyType, messageType);		
		
		// set up the iteration operator
		final String name = (this.name != null) ? this.name :
			"Vertex-centric iteration (" + updateFunction + " | " + messagingFunction + ")";
		final int[] zeroKeyPos = new int[] {0};
	
		final DeltaIteration<Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, VertexValue>> iteration =
			this.initialVertices.iterateDelta(this.initialVertices, this.maximumNumberOfIterations, zeroKeyPos);
		iteration.name(name);
		iteration.parallelism(parallelism);
		iteration.setSolutionSetUnManaged(unmanagedSolutionSet);
		
		// register all aggregators
		for (Map.Entry<String, Aggregator<?>> entry : this.aggregators.entrySet()) {
			iteration.registerAggregator(entry.getKey(), entry.getValue());
		}
		
		// build the messaging function (co group)
		CoGroupOperator<?, ?, Tuple2<VertexKey, Message>> messages;
		if (edgesWithoutValue != null) {
			MessagingUdfNoEdgeValues<VertexKey, VertexValue, Message> messenger = new MessagingUdfNoEdgeValues<VertexKey, VertexValue, Message>(messagingFunction, messageTypeInfo);
			messages = this.edgesWithoutValue.coGroup(iteration.getWorkset()).where(0).equalTo(0).with(messenger);
		}
		else {
			MessagingUdfWithEdgeValues<VertexKey, VertexValue, Message, EdgeValue> messenger = new MessagingUdfWithEdgeValues<VertexKey, VertexValue, Message, EdgeValue>(messagingFunction, messageTypeInfo);
			messages = this.edgesWithValue.coGroup(iteration.getWorkset()).where(0).equalTo(0).with(messenger);
		}
		
		// configure coGroup message function with name and broadcast variables
		messages = messages.name("Messaging");
		for (Tuple2<String, DataSet<?>> e : this.bcVarsMessaging) {
			messages = messages.withBroadcastSet(e.f1, e.f0);
		}
		
		VertexUpdateUdf<VertexKey, VertexValue, Message> updateUdf = new VertexUpdateUdf<VertexKey, VertexValue, Message>(updateFunction, vertexTypes);
		
		// build the update function (co group)
		CoGroupOperator<?, ?, Tuple2<VertexKey, VertexValue>> updates =
				messages.coGroup(iteration.getSolutionSet()).where(0).equalTo(0).with(updateUdf);
		
		// configure coGroup update function with name and broadcast variables
		updates = updates.name("Vertex State Updates");
		for (Tuple2<String, DataSet<?>> e : this.bcVarsUpdate) {
			updates = updates.withBroadcastSet(e.f1, e.f0);
		}

		// let the operator know that we preserve the key field
		updates.withForwardedFieldsFirst("0").withForwardedFieldsSecond("0");
		
		return iteration.closeWith(updates, updates);
		
	}
	
	// --------------------------------------------------------------------------------------------
	// Constructor builders to avoid signature conflicts with generic type erasure
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new vertex-centric iteration operator for graphs where the edges are not associated with a value.
	 * 
	 * @param edgesWithoutValue The data set containing edges. Edges are represented as 2-tuples: (source-id, target-id)
	 * @param vertexUpdateFunction The function that updates the state of the vertices from the incoming messages.
	 * @param messagingFunction The function that turns changed vertex states into messages along the edges.
	 * 
	 * @param <VertexKey> The type of the vertex key (the vertex identifier).
	 * @param <VertexValue> The type of the vertex value (the state of the vertex).
	 * @param <Message> The type of the message sent between vertices along the edges.
	 * 
	 * @return An in stance of the vertex-centric graph computation operator.
	 */
	@Deprecated
	public static final <VertexKey extends Comparable<VertexKey>, VertexValue, Message>
			VertexCentricIteration<VertexKey, VertexValue, Message, ?> withPlainEdges(
					DataSet<Tuple2<VertexKey, VertexKey>> edgesWithoutValue,
						VertexUpdateFunction<VertexKey, VertexValue, Message> vertexUpdateFunction,
						MessagingFunction<VertexKey, VertexValue, Message, ?> messagingFunction,
						int maximumNumberOfIterations)
	{
		@SuppressWarnings("unchecked")
		MessagingFunction<VertexKey, VertexValue, Message, Object> tmf = 
								(MessagingFunction<VertexKey, VertexValue, Message, Object>) messagingFunction;
		
		return new VertexCentricIteration<VertexKey, VertexValue, Message, Object>(vertexUpdateFunction, tmf, edgesWithoutValue, maximumNumberOfIterations);
	}
	
	/**
	 * Creates a new vertex-centric iteration operator for graphs where the edges are associated with a value (such as
	 * a weight or distance).
	 * 
	 * @param edgesWithValue The data set containing edges. Edges are represented as 2-tuples: (source-id, target-id)
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
	@Deprecated
	public static final <VertexKey extends Comparable<VertexKey>, VertexValue, Message, EdgeValue>
			VertexCentricIteration<VertexKey, VertexValue, Message, EdgeValue> withValuedEdges(
					DataSet<Tuple3<VertexKey, VertexKey, EdgeValue>> edgesWithValue,
					VertexUpdateFunction<VertexKey, VertexValue, Message> uf,
					MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> mf,
					int maximumNumberOfIterations)
	{
		return new VertexCentricIteration<VertexKey, VertexValue, Message, EdgeValue>(uf, mf, edgesWithValue, maximumNumberOfIterations, true);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Wrapping UDFs
	// --------------------------------------------------------------------------------------------
	
	private static final class VertexUpdateUdf<VertexKey extends Comparable<VertexKey>, VertexValue, Message> 
		extends RichCoGroupFunction<Tuple2<VertexKey, Message>, Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, VertexValue>>
		implements ResultTypeQueryable<Tuple2<VertexKey, VertexValue>>
	{
		private static final long serialVersionUID = 1L;
		
		private final VertexUpdateFunction<VertexKey, VertexValue, Message> vertexUpdateFunction;

		private final MessageIterator<Message> messageIter = new MessageIterator<Message>();
		
		private transient TypeInformation<Tuple2<VertexKey, VertexValue>> resultType;
		
		
		private VertexUpdateUdf(VertexUpdateFunction<VertexKey, VertexValue, Message> vertexUpdateFunction,
				TypeInformation<Tuple2<VertexKey, VertexValue>> resultType)
		{
			this.vertexUpdateFunction = vertexUpdateFunction;
			this.resultType = resultType;
		}

		@Override
		public void coGroup(Iterable<Tuple2<VertexKey, Message>> messages, Iterable<Tuple2<VertexKey, VertexValue>> vertex,
				Collector<Tuple2<VertexKey, VertexValue>> out)
			throws Exception
		{
			final Iterator<Tuple2<VertexKey, VertexValue>> vertexIter = vertex.iterator();
			
			if (vertexIter.hasNext()) {
				Tuple2<VertexKey, VertexValue> vertexState = vertexIter.next();
				
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
		public TypeInformation<Tuple2<VertexKey, VertexValue>> getProducedType() {
			return this.resultType;
		}
	}
	
	/*
	 * UDF that encapsulates the message sending function for graphs where the edges have no associated values.
	 */
	private static final class MessagingUdfNoEdgeValues<VertexKey extends Comparable<VertexKey>, VertexValue, Message> 
		extends RichCoGroupFunction<Tuple2<VertexKey, VertexKey>, Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, Message>>
		implements ResultTypeQueryable<Tuple2<VertexKey, Message>>
	{
		private static final long serialVersionUID = 1L;
		
		private final MessagingFunction<VertexKey, VertexValue, Message, ?> messagingFunction;
		
		private transient TypeInformation<Tuple2<VertexKey, Message>> resultType;
		
		
		private MessagingUdfNoEdgeValues(MessagingFunction<VertexKey, VertexValue, Message, ?> messagingFunction,
				TypeInformation<Tuple2<VertexKey, Message>> resultType)
		{
			this.messagingFunction = messagingFunction;
			this.resultType = resultType;
		}
		
		@Override
		public void coGroup(Iterable<Tuple2<VertexKey, VertexKey>> edges,
				Iterable<Tuple2<VertexKey, VertexValue>> state, Collector<Tuple2<VertexKey, Message>> out)
			throws Exception
		{
			final Iterator<Tuple2<VertexKey, VertexValue>> stateIter = state.iterator();
			
			if (stateIter.hasNext()) {
				Tuple2<VertexKey, VertexValue> newVertexState = stateIter.next();
				messagingFunction.set((Iterator<?>) edges.iterator(), out);
				messagingFunction.sendMessages(newVertexState.f0, newVertexState.f1);
			}
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
				this.messagingFunction.init(getIterationRuntimeContext(), false);
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
	
	/*
	 * UDF that encapsulates the message sending function for graphs where the edges have an associated value.
	 */
	private static final class MessagingUdfWithEdgeValues<VertexKey extends Comparable<VertexKey>, VertexValue, Message, EdgeValue> 
		extends RichCoGroupFunction<Tuple3<VertexKey, VertexKey, EdgeValue>, Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, Message>>
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
		public void coGroup(Iterable<Tuple3<VertexKey, VertexKey, EdgeValue>> edges,
				Iterable<Tuple2<VertexKey, VertexValue>> state, Collector<Tuple2<VertexKey, Message>> out)
			throws Exception
		{
			final Iterator<Tuple2<VertexKey, VertexValue>> stateIter = state.iterator();
			
			if (stateIter.hasNext()) {
				Tuple2<VertexKey, VertexValue> newVertexState = stateIter.next();
				messagingFunction.set((Iterator<?>) edges.iterator(), out);
				messagingFunction.sendMessages(newVertexState.f0, newVertexState.f1);
			}
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
				this.messagingFunction.init(getIterationRuntimeContext(), true);
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
}
