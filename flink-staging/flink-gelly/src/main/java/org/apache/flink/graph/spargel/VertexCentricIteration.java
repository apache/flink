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
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
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
 * @param <K> The type of the vertex key (the vertex identifier).
 * @param <VV> The type of the vertex value (the state of the vertex).
 * @param <Message> The type of the message sent between vertices along the edges.
 * @param <EV> The type of the values that are associated with the edges.
 */
public class VertexCentricIteration<K, VV, Message, EV> 
	implements CustomUnaryOperation<Vertex<K, VV>, Vertex<K, VV>>
{
	private final VertexUpdateFunction<K, VV, Message> updateFunction;

	private final MessagingFunction<K, VV, Message, EV> messagingFunction;
	
	private final DataSet<Edge<K, EV>> edgesWithValue;
	
	private final int maximumNumberOfIterations;
	
	private final TypeInformation<Message> messageType;
	
	private DataSet<Vertex<K, VV>> initialVertices;

	private VertexCentricConfiguration configuration;

	// ----------------------------------------------------------------------------------
	
	private VertexCentricIteration(VertexUpdateFunction<K, VV, Message> uf,
			MessagingFunction<K, VV, Message, EV> mf,
			DataSet<Edge<K, EV>> edgesWithValue, 
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
	
	private TypeInformation<Message> getMessageType(MessagingFunction<K, VV, Message, EV> mf) {
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
	public void setInput(DataSet<Vertex<K, VV>> inputData) {
		this.initialVertices = inputData;
	}
	
	/**
	 * Creates the operator that represents this vertex-centric graph computation.
	 * 
	 * @return The operator that represents this vertex-centric graph computation.
	 */
	@Override
	public DataSet<Vertex<K, VV>> createResult() {
		if (this.initialVertices == null) {
			throw new IllegalStateException("The input data set has not been set.");
		}

		// prepare some type information
		TypeInformation<K> keyType = ((TupleTypeInfo<?>) initialVertices.getType()).getTypeAt(0);
		TypeInformation<Tuple2<K, Message>> messageTypeInfo = new TupleTypeInfo<Tuple2<K,Message>>(keyType, messageType);

		// create a graph
		Graph<K, VV, EV> graph =
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
	 * @param <K> The type of the vertex key (the vertex identifier).
	 * @param <VV> The type of the vertex value (the state of the vertex).
	 * @param <Message> The type of the message sent between vertices along the edges.
	 * @param <EV> The type of the values that are associated with the edges.
	 * 
	 * @return An in stance of the vertex-centric graph computation operator.
	 */
	public static final <K, VV, Message, EV>
			VertexCentricIteration<K, VV, Message, EV> withEdges(
					DataSet<Edge<K, EV>> edgesWithValue,
					VertexUpdateFunction<K, VV, Message> uf,
					MessagingFunction<K, VV, Message, EV> mf,
					int maximumNumberOfIterations)
	{
		return new VertexCentricIteration<K, VV, Message, EV>(uf, mf, edgesWithValue, maximumNumberOfIterations);
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

	private static abstract class VertexUpdateUdf<K, VVWithDegrees, Message> extends RichCoGroupFunction<
		Tuple2<K, Message>, Vertex<K, VVWithDegrees>, Vertex<K, VVWithDegrees>>
		implements ResultTypeQueryable<Vertex<K, VVWithDegrees>>
	{
		private static final long serialVersionUID = 1L;
		
		final VertexUpdateFunction<K, VVWithDegrees, Message> vertexUpdateFunction;

		final MessageIterator<Message> messageIter = new MessageIterator<Message>();
		
		private transient TypeInformation<Vertex<K, VVWithDegrees>> resultType;
		
		
		private VertexUpdateUdf(VertexUpdateFunction<K, VVWithDegrees, Message> vertexUpdateFunction,
				TypeInformation<Vertex<K, VVWithDegrees>> resultType)
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
		public TypeInformation<Vertex<K, VVWithDegrees>> getProducedType() {
			return this.resultType;
		}
	}

	@SuppressWarnings("serial")
	private static final class VertexUpdateUdfSimpleVV<K, VV, Message> extends VertexUpdateUdf<K, VV, Message> {

		private VertexUpdateUdfSimpleVV(VertexUpdateFunction<K, VV, Message> vertexUpdateFunction, TypeInformation<Vertex<K, VV>> resultType) {
			super(vertexUpdateFunction, resultType);
		}

		@Override
		public void coGroup(Iterable<Tuple2<K, Message>> messages,
							Iterable<Vertex<K, VV>> vertex,
							Collector<Vertex<K, VV>> out) throws Exception {
			final Iterator<Vertex<K, VV>> vertexIter = vertex.iterator();

			if (vertexIter.hasNext()) {
				Vertex<K, VV> vertexState = vertexIter.next();

				@SuppressWarnings("unchecked")
				Iterator<Tuple2<?, Message>> downcastIter = (Iterator<Tuple2<?, Message>>) (Iterator<?>) messages.iterator();
				messageIter.setSource(downcastIter);

				vertexUpdateFunction.setOutput(vertexState, out);
				vertexUpdateFunction.updateVertex(vertexState, messageIter);
			}
			else {
				final Iterator<Tuple2<K, Message>> messageIter = messages.iterator();
				if (messageIter.hasNext()) {
					String message = "Target vertex does not exist!.";
					try {
						Tuple2<K, Message> next = messageIter.next();
						message = "Target vertex '" + next.f0 + "' does not exist!.";
					} catch (Throwable t) {}
					throw new Exception(message);
				} else {
					throw new Exception();
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class VertexUpdateUdfVVWithDegrees<K, VV, Message> extends VertexUpdateUdf<K, Tuple3<VV, Long, Long>, Message> {

		private VertexUpdateUdfVVWithDegrees(VertexUpdateFunction<K, Tuple3<VV, Long, Long>, Message> vertexUpdateFunction,
				TypeInformation<Vertex<K, Tuple3<VV, Long, Long>>> resultType) {
			super(vertexUpdateFunction, resultType);
		}
		
		@Override
		public void coGroup(Iterable<Tuple2<K, Message>> messages, Iterable<Vertex<K, Tuple3<VV, Long, Long>>> vertex,
							Collector<Vertex<K, Tuple3<VV, Long, Long>>> out) throws Exception {

			final Iterator<Vertex<K, Tuple3<VV, Long, Long>>> vertexIter = vertex.iterator();
		
			if (vertexIter.hasNext()) {
				Vertex<K, Tuple3<VV, Long, Long>> vertexWithDegrees = vertexIter.next();
		
				@SuppressWarnings("unchecked")
				Iterator<Tuple2<?, Message>> downcastIter = (Iterator<Tuple2<?, Message>>) (Iterator<?>) messages.iterator();
				messageIter.setSource(downcastIter);

				vertexUpdateFunction.setInDegree(vertexWithDegrees.f1.f1);
				vertexUpdateFunction.setOutDegree(vertexWithDegrees.f1.f2);

				vertexUpdateFunction.setOutputWithDegrees(vertexWithDegrees, out);
				vertexUpdateFunction.updateVertexFromVertexCentricIteration(vertexWithDegrees, messageIter);
			}
			else {
				final Iterator<Tuple2<K, Message>> messageIter = messages.iterator();
				if (messageIter.hasNext()) {
					String message = "Target vertex does not exist!.";
					try {
						Tuple2<K, Message> next = messageIter.next();
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
	private static abstract class MessagingUdfWithEdgeValues<K, VVWithDegrees, VV, Message, EV>
		extends RichCoGroupFunction<Edge<K, EV>, Vertex<K, VVWithDegrees>, Tuple2<K, Message>>
		implements ResultTypeQueryable<Tuple2<K, Message>>
	{
		private static final long serialVersionUID = 1L;
		
		final MessagingFunction<K, VV, Message, EV> messagingFunction;
		
		private transient TypeInformation<Tuple2<K, Message>> resultType;
	
	
		private MessagingUdfWithEdgeValues(MessagingFunction<K, VV, Message, EV> messagingFunction,
				TypeInformation<Tuple2<K, Message>> resultType)
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
		public TypeInformation<Tuple2<K, Message>> getProducedType() {
			return this.resultType;
		}
	}

	@SuppressWarnings("serial")
	private static final class MessagingUdfWithEVsSimpleVV<K, VV, Message, EV>
		extends MessagingUdfWithEdgeValues<K, VV, VV, Message, EV> {

		private MessagingUdfWithEVsSimpleVV(MessagingFunction<K, VV, Message, EV> messagingFunction,
			TypeInformation<Tuple2<K, Message>> resultType) {
			super(messagingFunction, resultType);
		}

		@Override
		public void coGroup(Iterable<Edge<K, EV>> edges,
							Iterable<Vertex<K, VV>> state,
							Collector<Tuple2<K, Message>> out) throws Exception {
			final Iterator<Vertex<K, VV>> stateIter = state.iterator();
		
			if (stateIter.hasNext()) {
				Vertex<K, VV> newVertexState = stateIter.next();
				messagingFunction.set((Iterator<?>) edges.iterator(), out);
				messagingFunction.sendMessages(newVertexState);
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class MessagingUdfWithEVsVVWithDegrees<K, VV, Message, EV>
		extends MessagingUdfWithEdgeValues<K, Tuple3<VV, Long, Long>, VV, Message, EV> {

		private Vertex<K, VV> nextVertex = new Vertex<K, VV>();

		private MessagingUdfWithEVsVVWithDegrees(MessagingFunction<K, VV, Message, EV> messagingFunction,
				TypeInformation<Tuple2<K, Message>> resultType) {
			super(messagingFunction, resultType);
		}

		@Override
		public void coGroup(Iterable<Edge<K, EV>> edges, Iterable<Vertex<K, Tuple3<VV, Long, Long>>> state,
				Collector<Tuple2<K, Message>> out) throws Exception {

			final Iterator<Vertex<K, Tuple3<VV, Long, Long>>> stateIter = state.iterator();
		
			if (stateIter.hasNext()) {
				Vertex<K, Tuple3<VV, Long, Long>> vertexWithDegrees = stateIter.next();

				nextVertex.setField(vertexWithDegrees.f0, 0);
				nextVertex.setField(vertexWithDegrees.f1.f0, 1);

				messagingFunction.setInDegree(vertexWithDegrees.f1.f1);
				messagingFunction.setOutDegree(vertexWithDegrees.f1.f2);

				messagingFunction.set((Iterator<?>) edges.iterator(), out);
				messagingFunction.sendMessages(nextVertex);
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
	private CoGroupOperator<?, ?, Tuple2<K, Message>> buildMessagingFunction(
			DeltaIteration<Vertex<K, VV>, Vertex<K, VV>> iteration,
			TypeInformation<Tuple2<K, Message>> messageTypeInfo, int whereArg, int equalToArg) {

		// build the messaging function (co group)
		CoGroupOperator<?, ?, Tuple2<K, Message>> messages;
		MessagingUdfWithEdgeValues<K, VV, VV, Message, EV> messenger =
				new MessagingUdfWithEVsSimpleVV<K, VV, Message, EV>(messagingFunction, messageTypeInfo);

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
	private CoGroupOperator<?, ?, Tuple2<K, Message>> buildMessagingFunctionVerticesWithDegrees(
			DeltaIteration<Vertex<K, Tuple3<VV, Long, Long>>, Vertex<K, Tuple3<VV, Long, Long>>> iteration,
			TypeInformation<Tuple2<K, Message>> messageTypeInfo, int whereArg, int equalToArg) {

		// build the messaging function (co group)
		CoGroupOperator<?, ?, Tuple2<K, Message>> messages;
		MessagingUdfWithEdgeValues<K, Tuple3<VV, Long, Long>, VV, Message, EV> messenger =
				new MessagingUdfWithEVsVVWithDegrees<K, VV, Message, EV>(messagingFunction, messageTypeInfo);

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
	 * @param iteration
	 */

	private void setUpIteration(DeltaIteration<?, ?> iteration) {

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
	}

	/**
	 * Creates the operator that represents this vertex centric graph computation for a simple vertex.
	 *
	 * @param messagingDirection
	 * @param messageTypeInfo
	 * @return the operator
	 */
	private DataSet<Vertex<K, VV>> createResultSimpleVertex(EdgeDirection messagingDirection,
		TypeInformation<Tuple2<K, Message>> messageTypeInfo) {

		DataSet<Tuple2<K, Message>> messages;

		TypeInformation<Vertex<K, VV>> vertexTypes = initialVertices.getType();

		final DeltaIteration<Vertex<K, VV>,	Vertex<K, VV>> iteration =
				initialVertices.iterateDelta(initialVertices, this.maximumNumberOfIterations, 0);
				setUpIteration(iteration);

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

		VertexUpdateUdf<K, VV, Message> updateUdf =
				new VertexUpdateUdfSimpleVV<K, VV, Message>(updateFunction, vertexTypes);

		// build the update function (co group)
		CoGroupOperator<?, ?, Vertex<K, VV>> updates =
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
	@SuppressWarnings("serial")
	private DataSet<Vertex<K, VV>> createResultVerticesWithDegrees(Graph<K, VV, EV> graph, EdgeDirection messagingDirection,
			TypeInformation<Tuple2<K, Message>> messageTypeInfo) {

		DataSet<Tuple2<K, Message>> messages;

		this.updateFunction.setOptDegrees(this.configuration.isOptDegrees());

		DataSet<Tuple2<K, Long>> inDegrees = graph.inDegrees();
		DataSet<Tuple2<K, Long>> outDegrees = graph.outDegrees();

		DataSet<Tuple3<K, Long, Long>> degrees = inDegrees.join(outDegrees).where(0).equalTo(0)
				.with(new FlatJoinFunction<Tuple2<K, Long>, Tuple2<K, Long>, Tuple3<K, Long, Long>>() {

					@Override
					public void join(Tuple2<K, Long> first, Tuple2<K, Long> second,	Collector<Tuple3<K, Long, Long>> out) {
						out.collect(new Tuple3<K, Long, Long>(first.f0, first.f1, second.f1));
					}
				}).withForwardedFieldsFirst("f0;f1").withForwardedFieldsSecond("f1");

		DataSet<Vertex<K, Tuple3<VV, Long, Long>>> verticesWithDegrees = initialVertices
				.join(degrees).where(0).equalTo(0)
				.with(new FlatJoinFunction<Vertex<K,VV>, Tuple3<K,Long,Long>, Vertex<K, Tuple3<VV, Long, Long>>>() {
					@Override
					public void join(Vertex<K, VV> vertex, Tuple3<K, Long, Long> degrees,
									Collector<Vertex<K, Tuple3<VV, Long, Long>>> out) throws Exception {

						out.collect(new Vertex<K, Tuple3<VV, Long, Long>>(vertex.getId(),
								new Tuple3<VV, Long, Long>(vertex.getValue(), degrees.f1, degrees.f2)));
					}
				}).withForwardedFieldsFirst("f0");

		// add type info
		TypeInformation<Vertex<K, Tuple3<VV, Long, Long>>> vertexTypes = verticesWithDegrees.getType();

		final DeltaIteration<Vertex<K, Tuple3<VV, Long, Long>>,	Vertex<K, Tuple3<VV, Long, Long>>> iteration =
				verticesWithDegrees.iterateDelta(verticesWithDegrees, this.maximumNumberOfIterations, 0);
				setUpIteration(iteration);

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

		@SuppressWarnings({ "unchecked", "rawtypes" })
		VertexUpdateUdf<K, Tuple3<VV, Long, Long>, Message> updateUdf =
				new VertexUpdateUdfVVWithDegrees(updateFunction, vertexTypes);

		// build the update function (co group)
		CoGroupOperator<?, ?, Vertex<K, Tuple3<VV, Long, Long>>> updates =
				messages.coGroup(iteration.getSolutionSet()).where(0).equalTo(0).with(updateUdf);

		configureUpdateFunction(updates);

		return iteration.closeWith(updates, updates).map(
				new MapFunction<Vertex<K, Tuple3<VV, Long, Long>>, Vertex<K, VV>>() {

					public Vertex<K, VV> map(Vertex<K, Tuple3<VV, Long, Long>> vertex) {
						return new Vertex<K, VV>(vertex.getId(), vertex.getValue().f0);
					}
				});
	}

	private <VVWithDegree> void configureUpdateFunction(CoGroupOperator<?, ?, Vertex<K, VVWithDegree>> updates) {

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