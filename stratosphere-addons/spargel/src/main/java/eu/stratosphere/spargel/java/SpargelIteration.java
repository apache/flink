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
package eu.stratosphere.spargel.java;

import java.lang.reflect.Type;
import java.util.Iterator;

import org.apache.commons.lang3.Validate;

import eu.stratosphere.api.common.operators.DeltaIteration;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.operators.CustomUnaryOperation;
import eu.stratosphere.api.java.operators.TwoInputOperator;
import eu.stratosphere.api.java.operators.translation.BinaryNodeTranslation;
import eu.stratosphere.api.java.operators.translation.PlanCogroupOperator;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;

/**
 *
 * @param <VertexKey> The type of the vertex key (the vertex identifier).
 * @param <VertexValue> The type of the vertex value (the state of the vertex).
 * @param <Message> The type of the message sent between vertices along the edges.
 * @param <EdgeValue> The type of the values that are associated with the edges.
 */
public class SpargelIteration<VertexKey extends Comparable<VertexKey>, VertexValue, Message, EdgeValue> 
	implements CustomUnaryOperation<Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, VertexValue>>
{
	private final VertexUpdateFunction<VertexKey, VertexValue, Message> updateFunction;
	
	private final MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> messagingFunction;
	
	private final DataSet<Tuple2<VertexKey, VertexKey>> edgesWithoutValue;
	
	private final DataSet<Tuple3<VertexKey, VertexKey, EdgeValue>> edgesWithValue;
	
	private final TypeInformation<Message> messageType;
	
	private final int maximumNumberOfIterations;
	
	private DataSet<Tuple2<VertexKey, VertexValue>> initialVertices;
		
	// ----------------------------------------------------------------------------------
	
	private  SpargelIteration(VertexUpdateFunction<VertexKey, VertexValue, Message> uf,
			MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> mf,
			DataSet<Tuple2<VertexKey, VertexKey>> edgesWithoutValue,
			int maximumNumberOfIterations)
	{
		// check that the edges are actually a valid tuple set of vertex key types
		TypeInformation<Tuple2<VertexKey, VertexKey>> edgesType = edgesWithoutValue.getType();
		Validate.isTrue(edgesType.isTupleType() && edgesType.getArity() == 2, "The edges data set (for edges without edge values) must consist of 2-tuples.");
		
		TupleTypeInfo<?> tupleInfo = (TupleTypeInfo<?>) edgesType;
		Validate.isTrue(tupleInfo.getTypeAt(0).equals(tupleInfo.getTypeAt(1))
			&& Comparable.class.isAssignableFrom(tupleInfo.getTypeAt(0).getTypeClass()),
			"Both tuple fields (source and target vertex id) must be of the data type that represents the vertex key and implement the java.lang.Comparable interface.");
		
		this.updateFunction = uf;
		this.messagingFunction = mf;
		this.edgesWithoutValue = edgesWithoutValue;
		this.edgesWithValue = null;
		this.maximumNumberOfIterations = maximumNumberOfIterations;
		
		this.messageType = getMessageType(mf);
	}
	
	private SpargelIteration(VertexUpdateFunction<VertexKey, VertexValue, Message> uf,
			MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> mf,
			DataSet<Tuple3<VertexKey, VertexKey, EdgeValue>> edgesWithValue, 
			int maximumNumberOfIterations,
			boolean edgeHasValueMarker)
	{
		// check that the edges are actually a valid tuple set of vertex key types
		TypeInformation<Tuple3<VertexKey, VertexKey, EdgeValue>> edgesType = edgesWithValue.getType();
		Validate.isTrue(edgesType.isTupleType() && edgesType.getArity() == 3, "The edges data set (for edges with edge values) must consist of 3-tuples.");
		
		TupleTypeInfo<?> tupleInfo = (TupleTypeInfo<?>) edgesType;
		Validate.isTrue(tupleInfo.getTypeAt(0).equals(tupleInfo.getTypeAt(1))
			&& Comparable.class.isAssignableFrom(tupleInfo.getTypeAt(0).getTypeClass()),
			"The first two tuple fields (source and target vertex id) must be of the data type that represents the vertex key and implement the java.lang.Comparable interface.");
		
		this.updateFunction = uf;
		this.messagingFunction = mf;
		this.edgesWithoutValue = null;
		this.edgesWithValue = edgesWithValue;
		this.maximumNumberOfIterations = maximumNumberOfIterations;
		
		this.messageType = getMessageType(mf);
	}
	
	private TypeInformation<Message> getMessageType(MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> mf) {
		Type returnType = TypeExtractor.getTemplateTypes (MessagingFunction.class, mf.getClass(), 2);
		return TypeExtractor.createTypeInfo(returnType);
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
	 * @see eu.stratosphere.api.java.operators.CustomUnaryOperation#setInput(eu.stratosphere.api.java.DataSet)
	 */
	@Override
	public void setInput(DataSet<Tuple2<VertexKey, VertexValue>> inputData) {
		// sanity check that we really have two tuples
		TypeInformation<Tuple2<VertexKey, VertexValue>> inputType = inputData.getType();
		Validate.isTrue(inputType.isTupleType() && inputType.getArity() == 2, "The input data set (the initial vertices) must consist of 2-tuples.");

		// check that the key type here is the same as for the edges
		TypeInformation<VertexKey> keyType = ((TupleTypeInfo<?>) inputType).getTypeAt(0);
		TypeInformation<?> edgeType = edgesWithoutValue != null ? edgesWithoutValue.getType() : edgesWithValue.getType();
		TypeInformation<VertexKey> edgeKeyType = ((TupleTypeInfo<?>) edgeType).getTypeAt(0);
		
		Validate.isTrue(keyType.equals(edgeKeyType), "The first tuple field (the vertex id) of the input data set (the initial vertices) " +
				"must be the same data type as the first fields of the edge data set (the source vertex id). " +
				"Here, the key type for the vertex ids is '%s' and the key type  for the edges is '%s'.", keyType, edgeKeyType);

		this.initialVertices = inputData;
	}
	
	@Override
	public GraphIterationOperator<VertexKey, VertexValue, Message, ?> createOperator() {
		
		VertexUpdateUdf<VertexKey, VertexValue, Message> updateUdf = new VertexUpdateUdf<VertexKey, VertexValue, Message>(updateFunction);
		
		if (edgesWithoutValue != null) {
			// edges have no values
			MessagingUdfNoEdgeValues<VertexKey, VertexValue, Message> messenger = new MessagingUdfNoEdgeValues<VertexKey, VertexValue, Message>(messagingFunction);
			return new GraphIterationOperator<VertexKey, VertexValue, Message, Tuple2<VertexKey, VertexKey>>(
					initialVertices, edgesWithoutValue, updateUdf, messenger, messageType, maximumNumberOfIterations);
		}
		else {
			// edges have values
			// edges have no values
			MessagingUdfWithEdgeValues<VertexKey, VertexValue, Message, EdgeValue> messenger = new MessagingUdfWithEdgeValues<VertexKey, VertexValue, Message, EdgeValue>(messagingFunction);
			return new GraphIterationOperator<VertexKey, VertexValue, Message, Tuple3<VertexKey, VertexKey, EdgeValue>>(
					initialVertices, edgesWithValue, updateUdf, messenger, messageType, maximumNumberOfIterations);
		}
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
	public static final <VertexKey extends Comparable<VertexKey>, VertexValue, Message>
			SpargelIteration<VertexKey, VertexValue, Message, ?> withPlainEdges(
					DataSet<Tuple2<VertexKey, VertexKey>> edgesWithoutValue,
						VertexUpdateFunction<VertexKey, VertexValue, Message> vertexUpdateFunction,
						MessagingFunction<VertexKey, VertexValue, Message, ?> messagingFunction,
						int maximumNumberOfIterations)
	{
		@SuppressWarnings("unchecked")
		MessagingFunction<VertexKey, VertexValue, Message, Object> tmf = 
								(MessagingFunction<VertexKey, VertexValue, Message, Object>) messagingFunction;
		
		return new SpargelIteration<VertexKey, VertexValue, Message, Object>(vertexUpdateFunction, tmf, edgesWithoutValue, maximumNumberOfIterations);
	}
	
	/**
	 * Creates a new vertex-centric iteration operator for graphs where the edges are associated with a value (such as
	 * a weight or distance).
	 * 
	 * @param edgesWithoutValue The data set containing edges. Edges are represented as 2-tuples: (source-id, target-id)
	 * @param vertexUpdateFunction The function that updates the state of the vertices from the incoming messages.
	 * @param messagingFunction The function that turns changed vertex states into messages along the edges.
	 * 
	 * @param <VertexKey> The type of the vertex key (the vertex identifier).
	 * @param <VertexValue> The type of the vertex value (the state of the vertex).
	 * @param <Message> The type of the message sent between vertices along the edges.
	 * @param <EdgeValue> The type of the values that are associated with the edges.
	 * 
	 * @return An in stance of the vertex-centric graph computation operator.
	 */
	public static final <VertexKey extends Comparable<VertexKey>, VertexValue, Message, EdgeValue>
			SpargelIteration<VertexKey, VertexValue, Message, EdgeValue> withValuedEdges(
					DataSet<Tuple3<VertexKey, VertexKey, EdgeValue>> edgesWithValue,
					VertexUpdateFunction<VertexKey, VertexValue, Message> uf,
					MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> mf,
					int maximumNumberOfIterations)
	{
		return new SpargelIteration<VertexKey, VertexValue, Message, EdgeValue>(uf, mf, edgesWithValue, maximumNumberOfIterations, true);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Wrapping UDFs
	// --------------------------------------------------------------------------------------------
	
	private static final class VertexUpdateUdf<VertexKey extends Comparable<VertexKey>, VertexValue, Message> 
		extends CoGroupFunction<Tuple2<VertexKey, Message>, Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, VertexValue>>
	{
		private static final long serialVersionUID = 1L;
		
		private final VertexUpdateFunction<VertexKey, VertexValue, Message> vertexUpdateFunction;

		private final MessageIterator<Message> messageIter = new MessageIterator<Message>();
		
		
		private VertexUpdateUdf(VertexUpdateFunction<VertexKey, VertexValue, Message> vertexUpdateFunction) {
			this.vertexUpdateFunction = vertexUpdateFunction;
		}

		@Override
		public void coGroup(Iterator<Tuple2<VertexKey, Message>> messages, Iterator<Tuple2<VertexKey, VertexValue>> vertex,
				Collector<Tuple2<VertexKey, VertexValue>> out)
			throws Exception
		{
			if (vertex.hasNext()) {
				Tuple2<VertexKey, VertexValue> vertexState = vertex.next();
				
				@SuppressWarnings("unchecked")
				Iterator<Tuple2<?, Message>> downcastIter = (Iterator<Tuple2<?, Message>>) (Iterator<?>) messages;
				messageIter.setSource(downcastIter);
				
				vertexUpdateFunction.setOutput(vertexState, out);
				vertexUpdateFunction.updateVertex(vertexState.f0, vertexState.f1, messageIter);
			} else {
				if (messages.hasNext()) {
					String message = "Target vertex does not exist!.";
					try {
						Tuple2<VertexKey, Message> next = messages.next();
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
				this.vertexUpdateFunction.setup(parameters);
			}
			this.vertexUpdateFunction.preSuperstep();
		}
		
		@Override
		public void close() throws Exception {
			this.vertexUpdateFunction.postSuperstep();
		}

		@Override
		public void combineFirst(Iterator<Tuple2<VertexKey, Message>> records, Collector<Tuple2<VertexKey, Message>> out) {}

		@Override
		public void combineSecond(Iterator<Tuple2<VertexKey, VertexValue>> records, Collector<Tuple2<VertexKey, VertexValue>> out) {}
	}
	
	/*
	 * UDF that encapsulates the message sending function for graphs where the edges have no associated values.
	 */
	private static final class MessagingUdfNoEdgeValues<VertexKey extends Comparable<VertexKey>, VertexValue, Message> 
		extends CoGroupFunction<Tuple2<VertexKey, VertexKey>, Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, Message>>
	{
		private static final long serialVersionUID = 1L;
		
		private final MessagingFunction<VertexKey, VertexValue, Message, ?> messagingFunction;
		
		
		private MessagingUdfNoEdgeValues(MessagingFunction<VertexKey, VertexValue, Message, ?> messagingFunction) {
			this.messagingFunction = messagingFunction;
		}
		
		@Override
		public void coGroup(Iterator<Tuple2<VertexKey, VertexKey>> edges,
				Iterator<Tuple2<VertexKey, VertexValue>> state, Collector<Tuple2<VertexKey, Message>> out)
			throws Exception
		{
			if (state.hasNext()) {
				Tuple2<VertexKey, VertexValue> newVertexState = state.next();
				messagingFunction.set((Iterator<?>) edges, out);
				messagingFunction.sendMessages(newVertexState.f0, newVertexState.f1);
			}
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
				this.messagingFunction.init(getIterationRuntimeContext(), false);
				this.messagingFunction.setup(parameters);
			}
			
			this.messagingFunction.preSuperstep();
		}
		
		@Override
		public void close() throws Exception {
			this.messagingFunction.postSuperstep();
		}

		@Override
		public void combineFirst(Iterator<Tuple2<VertexKey, VertexKey>> records, Collector<Tuple2<VertexKey, VertexKey>> out) {}
		
		@Override
		public void combineSecond(Iterator<Tuple2<VertexKey, VertexValue>> records, Collector<Tuple2<VertexKey, VertexValue>> out) {}
	}
	
	/*
	 * UDF that encapsulates the message sending function for graphs where the edges have an associated value.
	 */
	private static final class MessagingUdfWithEdgeValues<VertexKey extends Comparable<VertexKey>, VertexValue, Message, EdgeValue> 
		extends CoGroupFunction<Tuple3<VertexKey, VertexKey, EdgeValue>, Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, Message>>
	{
		private static final long serialVersionUID = 1L;
		
		private final MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> messagingFunction;
		
		
		private MessagingUdfWithEdgeValues(MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> messagingFunction) {
			this.messagingFunction = messagingFunction;
		}

		@Override
		public void coGroup(Iterator<Tuple3<VertexKey, VertexKey, EdgeValue>> edges,
				Iterator<Tuple2<VertexKey, VertexValue>> state, Collector<Tuple2<VertexKey, Message>> out)
			throws Exception
		{
			if (state.hasNext()) {
				Tuple2<VertexKey, VertexValue> newVertexState = state.next();
				messagingFunction.set((Iterator<?>) edges, out);
				messagingFunction.sendMessages(newVertexState.f0, newVertexState.f1);
			}
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
				this.messagingFunction.init(getIterationRuntimeContext(), true);
				this.messagingFunction.setup(parameters);
			}
			
			this.messagingFunction.preSuperstep();
		}
		
		@Override
		public void close() throws Exception {
			this.messagingFunction.postSuperstep();
		}

		@Override
		public void combineFirst(Iterator<Tuple3<VertexKey, VertexKey, EdgeValue>> records, Collector<Tuple3<VertexKey, VertexKey, EdgeValue>> out) {}
		
		@Override
		public void combineSecond(Iterator<Tuple2<VertexKey, VertexValue>> records, Collector<Tuple2<VertexKey, VertexValue>> out) {}
	}
	
	// --------------------------------------------------------------------------------------------
	//  The data flow operator
	// --------------------------------------------------------------------------------------------
	
	/*
	 * The data flow operator.
	 */
	private static final class GraphIterationOperator<VertexKey extends Comparable<VertexKey>, VertexValue, Message, EdgeType extends Tuple> extends 
		TwoInputOperator<Tuple2<VertexKey, VertexValue>, EdgeType, Tuple2<VertexKey, VertexValue>, GraphIterationOperator<VertexKey, VertexValue, Message, EdgeType>>
	{
	
		private final DataSet<EdgeType> edges;
		
		private final CoGroupFunction<Tuple2<VertexKey, Message>, Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, VertexValue>> updateFunction;
		
		private final CoGroupFunction<EdgeType, Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, Message>> messagingFunction;
		
		private final TypeInformation<Tuple2<VertexKey, Message>> messageType;
		
		private final int maximumNumberOfIterations;
		
		private GraphIterationOperator(DataSet<Tuple2<VertexKey, VertexValue>> initialVertices,
		                                            DataSet<EdgeType> edges,
		                                            VertexUpdateUdf<VertexKey, VertexValue, Message> updateFunction,
		                                            CoGroupFunction<EdgeType, Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, Message>> messagingFunction,
		                                            TypeInformation<Message> messageType,
		                                            int maximumNumberOfIterations)
		{
			super(initialVertices, edges, initialVertices.getType());
			
			this.edges = edges;
			this.updateFunction = updateFunction;
			this.messagingFunction = messagingFunction;
			this.maximumNumberOfIterations = maximumNumberOfIterations;
			
			// construct the type for the messages between the messaging function and the vertex update function
			TypeInformation<VertexKey> keyType = ((TupleTypeInfo<?>) initialVertices.getType()).getTypeAt(0);
			this.messageType = new TupleTypeInfo<Tuple2<VertexKey,Message>>(keyType, messageType);
		}

		@Override
		protected BinaryNodeTranslation translateToDataFlow() {
			
			final String name = (getName() != null) ? getName() :
					"Vertex-centric iteration (" + updateFunction + " | " + messagingFunction + ")";
			
			final int[] zeroKeyPos = new int[] {0};
			
			final DeltaIteration iteration = new DeltaIteration(0, name);
			iteration.setMaximumNumberOfIterations(maximumNumberOfIterations);
			
			final PlanCogroupOperator<EdgeType, Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, Message>> messenger =
					new PlanCogroupOperator<EdgeType, Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, Message>>(
							messagingFunction, zeroKeyPos, zeroKeyPos, "Messaging", edges.getType(), getInput1Type(), messageType);
			
			messenger.setSecondInput(iteration.getWorkset());
			
			final PlanCogroupOperator<Tuple2<VertexKey, Message>, Tuple2<VertexKey, VertexValue>, Tuple2<VertexKey, VertexValue>> updater =
					new PlanCogroupOperator<Tuple2<VertexKey,Message>, Tuple2<VertexKey,VertexValue>, Tuple2<VertexKey,VertexValue>>(
							updateFunction, zeroKeyPos, zeroKeyPos, "Vertex State Updates", messageType, getInput1Type(), getInput1Type());
			
			updater.setFirstInput(messenger);
			updater.setSecondInput(iteration.getSolutionSet());
			
			iteration.setSolutionSetDelta(updater);
			iteration.setNextWorkset(updater);
			
			// return a translation node that will assign the first input to the iteration (both initial solution set and workset)
			// and that assigns the second input to the messenger function (as the edge input)
			return new BinaryNodeTranslation(iteration) {
				
				@Override
				public void setInput1(Operator op) {
					iteration.setFirstInput(op);
					iteration.setSecondInput(op);
				}
				
				@Override
				public void setInput2(Operator op) {
					messenger.setFirstInput(op);
				}
			};
		}
	}
}
