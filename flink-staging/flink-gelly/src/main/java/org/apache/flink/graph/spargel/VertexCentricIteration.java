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

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
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
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
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
 *
 * Vertex-centric graph iterations are are run by calling
 * {@link Graph#runVertexCentricIteration(VertexUpdateFunction, MessagingFunction, int)}.
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

	private VertexCentricConfiguration configuration;

	private DataSet<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> verticesWithDegrees;
	// ----------------------------------------------------------------------------------
	
	private VertexCentricIteration(VertexUpdateFunction<VertexKey, VertexValue, Message> uf,
			MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> mf,
			DataSet<Edge<VertexKey, EdgeValue>> edgesWithValue, 
			int maximumNumberOfIterations)
	{
		Preconditions.checkNotNull(uf);
		Preconditions.checkNotNull(mf);
		Preconditions.checkNotNull(edgesWithValue);
		Preconditions.checkArgument(maximumNumberOfIterations > 0, "The maximum number of iterations must be at least one.");

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
		TypeInformation<VertexKey> keyType = ((TupleTypeInfo<?>) initialVertices.getType()).getTypeAt(0);
		TypeInformation<Tuple2<VertexKey, Message>> messageTypeInfo = new TupleTypeInfo<Tuple2<VertexKey,Message>>(keyType, messageType);

		// create a graph
		Graph<VertexKey, VertexValue, EdgeValue> graph =
				Graph.fromDataSet(initialVertices, edgesWithValue, ExecutionEnvironment.getExecutionEnvironment());

		// check whether the numVertices option is set and, if so, compute the total number of vertices
		// and set it within the messaging and update functions

		if (this.configuration != null && this.configuration.isOptNumVertices()) {
			try {
				long numberOfVertices = graph.numberOfVertices();
				messagingFunction.setNumberOfVertices(numberOfVertices);
				updateFunction.setNumberOfVertices(numberOfVertices);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		if(this.configuration != null) {
			messagingFunction.setDirection(this.configuration.getDirection());
		} else {
			messagingFunction.setDirection(EdgeDirection.OUT);
		}

		// retrieve the direction in which the updates are made and in which the messages are sent
		EdgeDirection messagingDirection = messagingFunction.getDirection();

		// check whether the degrees option is set and, if so, compute the in and the out degrees and
		// add them to the vertex value
		if(this.configuration != null && this.configuration.isOptDegrees()) {
			return createResultVerticesWithDegrees(graph, messagingDirection, messageTypeInfo);
		} else {
			return createResultSimpleVertex(messagingDirection, messageTypeInfo);
		}
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

	/**
	 * Configures this vertex-centric iteration with the provided parameters.
	 *
	 * @param parameters the configuration parameters
	 */
	public void configure(VertexCentricConfiguration parameters) {
		this.configuration = parameters;
	}

	/**
	 * @return the configuration parameters of this vertex-centric iteration
	 */
	public VertexCentricConfiguration getIterationConfiguration() {
		return this.configuration;
	}

	// --------------------------------------------------------------------------------------------
	//  Wrapping UDFs
	// --------------------------------------------------------------------------------------------

	private static final class VertexUpdateUdf<VertexKey, VertexValue, Message> 
		extends RichCoGroupFunction<Tuple2<VertexKey, Message>, Vertex<VertexKey, VertexValue>, Vertex<VertexKey, VertexValue>>
		implements ResultTypeQueryable<Vertex<VertexKey, VertexValue>>
	{
		private static final long serialVersionUID = 1L;
		
		final VertexUpdateFunction<VertexKey, VertexValue, Message> vertexUpdateFunction;

		final MessageIterator<Message> messageIter = new MessageIterator<Message>();
		
		private transient TypeInformation<Vertex<VertexKey, VV>> resultType;
		
		
		private VertexUpdateUdf(VertexUpdateFunction<VertexKey, VertexValue, Message> vertexUpdateFunction,
				TypeInformation<Vertex<VertexKey, VV>> resultType)
		{
			this.vertexUpdateFunction = vertexUpdateFunction;
			this.resultType = resultType;
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
		public TypeInformation<Vertex<VertexKey, VV>> getProducedType() {
			return this.resultType;
		}
	}

	private static final class VertexUpdateUdfSimpleVertexValue<VertexKey, VertexValue, Message>
		extends VertexUpdateUdf<VertexKey, VertexValue, Message> {


		private VertexUpdateUdfSimpleVertexValue(VertexUpdateFunction<VertexKey, VertexValue, Message> vertexUpdateFunction, TypeInformation<Vertex<VertexKey, VertexValue>> resultType) {
			super(vertexUpdateFunction, resultType);
		}

		@Override
		public void coGroup(Iterable<Tuple2<VertexKey, Message>> messages,
							Iterable<Vertex<VertexKey, VertexValue>> vertex,
							Collector<Vertex<VertexKey, VertexValue>> out) throws Exception {
			final Iterator<Vertex<VertexKey, VertexValue>> vertexIter = vertex.iterator();

			if (vertexIter.hasNext()) {
				Vertex<VertexKey, VertexValue> vertexState = vertexIter.next();

				@SuppressWarnings("unchecked")
				Iterator<Tuple2<?, Message>> downcastIter = (Iterator<Tuple2<?, Message>>) (Iterator<?>) messages.iterator();
				messageIter.setSource(downcastIter);

				vertexUpdateFunction.setOutput(vertexState, out);
				vertexUpdateFunction.updateVertex(vertexState, messageIter);
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
	}

	private static final class VertexUpdateUdfVertexValueWithDegrees<VertexKey,	VertexValue, Message> extends VertexUpdateUdf<VertexKey,
			Tuple3<VertexValue, Long, Long>, VertexValue, Message> {


		private VertexUpdateUdfVertexValueWithDegrees(VertexUpdateFunction<VertexKey, VertexValue, Message> vertexUpdateFunction, TypeInformation<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> resultType) {
			super(vertexUpdateFunction, resultType);
		}

		@Override
		public void coGroup(Iterable<Tuple2<VertexKey, Message>> messages,
							Iterable<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> vertex,
							Collector<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> out) throws Exception {
			final Iterator<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> vertexIter = vertex.iterator();

			if (vertexIter.hasNext()) {
				Vertex<VertexKey, Tuple3<VertexValue, Long, Long>> vertexState = vertexIter.next();

				@SuppressWarnings("unchecked")
				Iterator<Tuple2<?, Message>> downcastIter = (Iterator<Tuple2<?, Message>>) (Iterator<?>) messages.iterator();
				messageIter.setSource(downcastIter);

				vertexUpdateFunction.setOutputWithDegrees(vertexState, out);
				vertexUpdateFunction.updateVertexFromVertexCentricIteration(vertexState, messageIter);
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
	}

	/*
	 * UDF that encapsulates the message sending function for graphs where the edges have an associated value.
	 */
	private static final class MessagingUdfWithEdgeValues<VertexKey, VertexValue, Message, EdgeValue> 
		extends RichCoGroupFunction<Edge<VertexKey, EdgeValue>, Vertex<VertexKey, VertexValue>, Tuple2<VertexKey, Message>>
		implements ResultTypeQueryable<Tuple2<VertexKey, Message>>
	{
		private static final long serialVersionUID = 1L;
		
		final MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> messagingFunction;
		
		private transient TypeInformation<Tuple2<VertexKey, Message>> resultType;


		private MessagingUdfWithEdgeValues(MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> messagingFunction,
				TypeInformation<Tuple2<VertexKey, Message>> resultType)
		{
			this.messagingFunction = messagingFunction;
			this.resultType = resultType;
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

	private static final class MessagingUdfWithEdgeValuesSimpleVertexValue<VertexKey, VertexValue, Message, EdgeValue>
			extends MessagingUdfWithEdgeValues<VertexKey, VertexValue, VertexValue, Message, EdgeValue> {

		private MessagingUdfWithEdgeValuesSimpleVertexValue(MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> messagingFunction,
															TypeInformation<Tuple2<VertexKey, Message>> resultType) {
			super(messagingFunction, resultType);
		}

		@Override
		public void coGroup(Iterable<Edge<VertexKey, EdgeValue>> edges,
							Iterable<Vertex<VertexKey, VertexValue>> state,
							Collector<Tuple2<VertexKey, Message>> out) throws Exception {
			final Iterator<Vertex<VertexKey, VertexValue>> stateIter = state.iterator();

			if (stateIter.hasNext()) {
				Vertex<VertexKey, VertexValue> newVertexState = stateIter.next();
				messagingFunction.set((Iterator<?>) edges.iterator(), out);
				messagingFunction.sendMessages(newVertexState);
			}
		}
	}

	private static final class MessagingUdfWithEdgeValuesVertexValueWithDegrees<VertexKey, VertexValue, Message, EdgeValue>
			extends MessagingUdfWithEdgeValues<VertexKey, Tuple3<VertexValue, Long, Long>, VertexValue, Message, EdgeValue> {


		private MessagingUdfWithEdgeValuesVertexValueWithDegrees
				(MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> messagingFunction,
				TypeInformation<Tuple2<VertexKey, Message>> resultType) {
			super(messagingFunction, resultType);
		}

		@Override
		public void coGroup(Iterable<Edge<VertexKey, EdgeValue>> edges,
							Iterable<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> state,
							Collector<Tuple2<VertexKey, Message>> out) throws Exception {

			final Iterator<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> stateIter = state.iterator();

			if (stateIter.hasNext()) {
				Vertex<VertexKey, Tuple3<VertexValue, Long, Long>> newVertexState = stateIter.next();
				messagingFunction.set((Iterator<?>) edges.iterator(), out);
				messagingFunction.sendMessagesFromVertexCentricIteration(newVertexState);
			}
		}
	}


	// --------------------------------------------------------------------------------------------
	//  UTIL methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Method that builds the messaging function using a coGroup operator for a simple vertex(without
	 * degrees).
	 * It afterwards configures the function with a custom name and broadcast variables.
	 *
	 * @param iteration
	 * @param messageTypeInfo
	 * @param whereArg the argument for the where within the coGroup
	 * @param equalToArg the argument for the equalTo within the coGroup
	 * @return the messaging function
	 */
	private CoGroupOperator<?, ?, Tuple2<VertexKey, Message>> buildMessagingFunction(
			DeltaIteration<Vertex<VertexKey, VertexValue>, Vertex<VertexKey, VertexValue>> iteration,
			TypeInformation<Tuple2<VertexKey, Message>> messageTypeInfo, int whereArg, int equalToArg) {

		// build the messaging function (co group)
		CoGroupOperator<?, ?, Tuple2<VertexKey, Message>> messages;
		MessagingUdfWithEdgeValues<VertexKey, VertexValue, VertexValue, Message, EdgeValue> messenger =
				new MessagingUdfWithEdgeValuesSimpleVertexValue<VertexKey, VertexValue, Message, EdgeValue>(
						messagingFunction, messageTypeInfo);

		messages = this.edgesWithValue.coGroup(iteration.getWorkset()).where(whereArg)
				.equalTo(equalToArg).with(messenger);

		// configure coGroup message function with name and broadcast variables
		messages = messages.name("Messaging");
		if(this.configuration != null) {
			for (Tuple2<String, DataSet<?>> e : this.configuration.getMessagingBcastVars()) {
				messages = messages.withBroadcastSet(e.f1, e.f0);
			}
		}

		return messages;
	}

	/**
	 * Method that builds the messaging function using a coGroup operator for a vertex
	 * containing degree information.
	 * It afterwards configures the function with a custom name and broadcast variables.
	 *
	 * @param iteration
	 * @param messageTypeInfo
	 * @param whereArg the argument for the where within the coGroup
	 * @param equalToArg the argument for the equalTo within the coGroup
	 * @return the messaging function
	 */
	private CoGroupOperator<?, ?, Tuple2<VertexKey, Message>> buildMessagingFunctionVerticesWithDegrees(
			DeltaIteration<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>,
					Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> iteration,
			TypeInformation<Tuple2<VertexKey, Message>> messageTypeInfo, int whereArg, int equalToArg) {

		// build the messaging function (co group)
		CoGroupOperator<?, ?, Tuple2<VertexKey, Message>> messages;
		MessagingUdfWithEdgeValues<VertexKey, Tuple3<VertexValue, Long, Long>, VertexValue, Message, EdgeValue> messenger =
				new MessagingUdfWithEdgeValuesVertexValueWithDegrees<VertexKey, VertexValue, Message, EdgeValue>(
						messagingFunction, messageTypeInfo);

		messages = this.edgesWithValue.coGroup(iteration.getWorkset()).where(whereArg)
				.equalTo(equalToArg).with(messenger);

		// configure coGroup message function with name and broadcast variables
		messages = messages.name("Messaging");

		if (this.configuration != null) {
			for (Tuple2<String, DataSet<?>> e : this.configuration.getMessagingBcastVars()) {
				messages = messages.withBroadcastSet(e.f1, e.f0);
			}
		}

		return messages;
	}

	/**
	 * Helper method which sets up an iteration with the given vertex value(either simple or with degrees)
	 *
	 * @param vertices
	 * @param <VV>
	 */

	private <VV> DeltaIteration<Vertex<VertexKey, VV>, Vertex<VertexKey, VV>> setUpIteration(
			DataSet<Vertex<VertexKey, VV>> vertices) {

		final int[] zeroKeyPos = new int[] {0};

		final DeltaIteration<Vertex<VertexKey, VV>, Vertex<VertexKey, VV>> iteration =
				vertices.iterateDelta(vertices, this.maximumNumberOfIterations, zeroKeyPos);

		// set up the iteration operator
		if (this.configuration != null) {

			iteration.name(this.configuration.getName("Vertex-centric iteration (" + updateFunction + " | " + messagingFunction + ")"));
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

		return iteration;
	}

	/**
	 * Creates the operator that represents this vertex centric graph computation for a simple vertex.
	 *
	 * @param messagingDirection
	 * @param messageTypeInfo
	 * @return the operator
	 */
	private DataSet<Vertex<VertexKey, VertexValue>> createResultSimpleVertex(EdgeDirection messagingDirection,
																			TypeInformation<Tuple2<VertexKey, Message>> messageTypeInfo) {
		DataSet<Tuple2<VertexKey, Message>> messages;

		TypeInformation<Vertex<VertexKey, VertexValue>> vertexTypes = initialVertices.getType();

		final DeltaIteration<Vertex<VertexKey, VertexValue>, Vertex<VertexKey, VertexValue>> iteration =
				setUpIteration(this.initialVertices);

		switch (messagingDirection) {
			case IN:
				messages = buildMessagingFunction(iteration, messageTypeInfo, 1, 0);
				break;
			case OUT:
				messages = buildMessagingFunction(iteration, messageTypeInfo, 0, 0);
				break;
			case ALL:
				messages = buildMessagingFunction(iteration, messageTypeInfo, 1, 0)
						.union(buildMessagingFunction(iteration, messageTypeInfo, 0, 0)) ;
				break;
			default:
				throw new IllegalArgumentException("Illegal edge direction");
		}

		VertexUpdateUdf<VertexKey, VertexValue, VertexValue, Message> updateUdf =
				new VertexUpdateUdfSimpleVertexValue<VertexKey, VertexValue, Message>(updateFunction, vertexTypes);

		// build the update function (co group)
		CoGroupOperator<?, ?, Vertex<VertexKey, VertexValue>> updates =
				messages.coGroup(iteration.getSolutionSet()).where(0).equalTo(0).with(updateUdf);

		configureUpdateFunction(updates);

		return iteration.closeWith(updates, updates);
	}

	/**
	 * Creates the operator that represents this vertex centric graph computation for a vertex with in
	 * and out degrees added to the vertex value.
	 *
	 * @param graph
	 * @param messagingDirection
	 * @param messageTypeInfo
	 * @return the operator
	 */
	private DataSet<Vertex<VertexKey, VertexValue>> createResultVerticesWithDegrees(
			Graph<VertexKey, VertexValue, EdgeValue> graph,
			EdgeDirection messagingDirection,
			TypeInformation<Tuple2<VertexKey, Message>> messageTypeInfo) {

		DataSet<Tuple2<VertexKey, Message>> messages;

		this.updateFunction.setOptDegrees(this.configuration.isOptDegrees());

		DataSet<Tuple2<VertexKey, Long>> inDegrees = graph.inDegrees();
		DataSet<Tuple2<VertexKey, Long>> outDegrees = graph.outDegrees();

		DataSet<Tuple3<VertexKey, Long, Long>> degrees = inDegrees.join(outDegrees).where(0).equalTo(0)
				.with(new FlatJoinFunction<Tuple2<VertexKey, Long>, Tuple2<VertexKey, Long>, Tuple3<VertexKey, Long, Long>>() {

					@Override
					public void join(Tuple2<VertexKey, Long> first, Tuple2<VertexKey, Long> second, Collector<Tuple3<VertexKey, Long, Long>> out) throws Exception {
						out.collect(new Tuple3<VertexKey, Long, Long>(first.f0, first.f1, second.f1));
					}
				});

		DataSet<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> verticesWithDegrees= initialVertices
				.join(degrees).where(0).equalTo(0)
				.with(new FlatJoinFunction<Vertex<VertexKey,VertexValue>, Tuple3<VertexKey,Long,Long>, Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>>() {
					@Override
					public void join(Vertex<VertexKey, VertexValue> vertex,
									Tuple3<VertexKey, Long, Long> degrees,
									Collector<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> out) throws Exception {

						out.collect(new Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>(vertex.getId(),
								new Tuple3<VertexValue, Long, Long>(vertex.getValue(), degrees.f1, degrees.f2)));
					}
				});

		// add type info
		TypeInformation<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> vertexTypes = verticesWithDegrees.getType();

		final DeltaIteration<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>,
				Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> iteration =
				setUpIteration(verticesWithDegrees);

		switch (messagingDirection) {
			case IN:
				messages = buildMessagingFunctionVerticesWithDegrees(iteration, messageTypeInfo, 1, 0);
				break;
			case OUT:
				messages = buildMessagingFunctionVerticesWithDegrees(iteration, messageTypeInfo, 0, 0);
				break;
			case ALL:
				messages = buildMessagingFunctionVerticesWithDegrees(iteration, messageTypeInfo, 1, 0)
						.union(buildMessagingFunctionVerticesWithDegrees(iteration, messageTypeInfo, 0, 0)) ;
				break;
			default:
				throw new IllegalArgumentException("Illegal edge direction");
		}

		VertexUpdateUdf<VertexKey, Tuple3<VertexValue, Long, Long>, VertexValue, Message> updateUdf =
				new VertexUpdateUdfVertexValueWithDegrees<VertexKey, VertexValue, Message>(updateFunction, vertexTypes);

		// build the update function (co group)
		CoGroupOperator<?, ?, Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>> updates =
				messages.coGroup(iteration.getSolutionSet()).where(0).equalTo(0).with(updateUdf);

		configureUpdateFunction(updates);

		return iteration.closeWith(updates, updates).map(new MapFunction<Vertex<VertexKey, Tuple3<VertexValue, Long, Long>>, Vertex<VertexKey, VertexValue>>() {
			@Override
			public Vertex<VertexKey, VertexValue> map(Vertex<VertexKey, Tuple3<VertexValue, Long, Long>> vertex) throws Exception {
				return new Vertex<VertexKey, VertexValue>(vertex.getId(), vertex.getValue().f0);
			}
		});
	}

	private <VV> void configureUpdateFunction(CoGroupOperator<?, ?, Vertex<VertexKey, VV>> updates) {

		// configure coGroup update function with name and broadcast variables
		updates = updates.name("Vertex State Updates");
		if (this.configuration != null) {
			for (Tuple2<String, DataSet<?>> e : this.configuration.getUpdateBcastVars()) {
				updates = updates.withBroadcastSet(e.f1, e.f0);
			}
		}

		// let the operator know that we preserve the key field
		updates.withForwardedFieldsFirst("0").withForwardedFieldsSecond("0");
	}
}
