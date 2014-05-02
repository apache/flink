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

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.DeltaIteration;
import eu.stratosphere.api.java.record.functions.CoGroupFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.java.record.operators.CoGroupOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.InstantiationUtil;
import eu.stratosphere.util.ReflectionUtil;

public class SpargelIteration {
	
	private static final String DEFAULT_NAME = "<unnamed vertex-centric iteration>";
	
	private final DeltaIteration iteration;
	
	private final Class<? extends Key> vertexKey;
	private final Class<? extends Value> vertexValue;
	private final Class<? extends Value> messageType;
	private final Class<? extends Value> edgeValue;
	
	private final CoGroupOperator vertexUpdater;
	private final CoGroupOperator messager;
	
	
	// ----------------------------------------------------------------------------------
	
	public <VertexKey extends Key, VertexValue extends Value, Message extends Value, EdgeValue extends Value>
	SpargelIteration(MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> mf,
					VertexUpdateFunction<VertexKey, VertexValue, Message> uf)
	{
		this(mf, uf, DEFAULT_NAME);
	}
	
	public <VertexKey extends Key, VertexValue extends Value, Message extends Value, EdgeValue extends Value>
	SpargelIteration(MessagingFunction<VertexKey, VertexValue, Message, EdgeValue> mf,
					VertexUpdateFunction<VertexKey, VertexValue, Message> uf,
					String name)
	{
		// get the types
		this.vertexKey = ReflectionUtil.getTemplateType1(mf.getClass());
		this.vertexValue = ReflectionUtil.getTemplateType2(mf.getClass());
		this.messageType = ReflectionUtil.getTemplateType3(mf.getClass());
		this.edgeValue = ReflectionUtil.getTemplateType4(mf.getClass());
		
		if (vertexKey == null || vertexValue == null || messageType == null || edgeValue == null) {
			throw new RuntimeException();
		}
	
		// instantiate the data flow
		this.iteration = new DeltaIteration(0, name);
		
		this.messager = CoGroupOperator.builder(MessagingDriver.class, vertexKey, 0, 0)
			.input2(iteration.getWorkset())
			.name("Message Sender")
			.build();
		this.vertexUpdater = CoGroupOperator.builder(VertexUpdateDriver.class, vertexKey, 0, 0)
			.input1(messager)
			.input2(iteration.getSolutionSet())
			.name("Vertex Updater")
			.build();
		
		iteration.setNextWorkset(vertexUpdater);
		iteration.setSolutionSetDelta(vertexUpdater);
		
		// parameterize the data flow
		try {
			Configuration vertexUdfParams = vertexUpdater.getParameters();
			InstantiationUtil.writeObjectToConfig(uf, vertexUdfParams, VertexUpdateDriver.UDF_PARAM);
			vertexUdfParams.setClass(VertexUpdateDriver.KEY_PARAM, vertexKey);
			vertexUdfParams.setClass(VertexUpdateDriver.VALUE_PARAM, vertexValue);
			vertexUdfParams.setClass(VertexUpdateDriver.MESSAGE_PARAM, messageType);
			
			Configuration messageUdfParams = messager.getParameters();
			InstantiationUtil.writeObjectToConfig(mf, messageUdfParams, MessagingDriver.UDF_PARAM);
			messageUdfParams.setClass(MessagingDriver.KEY_PARAM, vertexKey);
			messageUdfParams.setClass(MessagingDriver.VALUE_PARAM, vertexValue);
			messageUdfParams.setClass(MessagingDriver.MESSAGE_PARAM, messageType);
			messageUdfParams.setClass(MessagingDriver.EDGE_PARAM, edgeValue);
		}
		catch (IOException e) {
			throw new RuntimeException("Could not serialize the UDFs for distribution" + 
					(e.getMessage() == null ? '.' : ": " + e.getMessage()), e);
		}
	}
	
	// ----------------------------------------------------------------------------------
	//  inputs and outputs
	// ----------------------------------------------------------------------------------
	
	public void setVertexInput(Operator c) {
		this.iteration.setInitialSolutionSet(c);
		this.iteration.setInitialWorkset(c);
	}
	
	public void setEdgesInput(Operator c) {
		this.messager.setFirstInput(c);
	}
	
	public Operator getOutput() {
		return this.iteration;
	}
	
	public void setDegreeOfParallelism(int dop) {
		this.iteration.setDegreeOfParallelism(dop);
	}
	
	public void setNumberOfIterations(int iterations) {
		this.iteration.setMaximumNumberOfIterations(iterations);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Wrapping UDFs
	// --------------------------------------------------------------------------------------------
	
	@ConstantFieldsFirst(0)
	public static final class VertexUpdateDriver<K extends Key, V extends Value, M extends Value> extends CoGroupFunction {
		
		private static final long serialVersionUID = 1L;
		
		private static final String UDF_PARAM = "spargel.udf";
		private static final String KEY_PARAM = "spargel.key-type";
		private static final String VALUE_PARAM = "spargel.value-type";
		private static final String MESSAGE_PARAM = "spargel.message-type";
		
		private VertexUpdateFunction<K, V, M> vertexUpdateFunction;
		
		private K vertexKey;
		private V vertexValue;
		private MessageIterator<M> messageIter;

		@Override
		public void coGroup(Iterator<Record> messages, Iterator<Record> vertex, Collector<Record> out) throws Exception {
			if (vertex.hasNext()) {
				Record first = vertex.next();
				first.getFieldInto(0, vertexKey);
				first.getFieldInto(1, vertexValue);
				messageIter.setSource(messages);
				vertexUpdateFunction.setOutput(first, out);
				vertexUpdateFunction.updateVertex(vertexKey, vertexValue, messageIter);
			} else {
				if (messages.hasNext()) {
					String message = "Target vertex does not exist!.";
					try {
						Record next = messages.next();
						next.getFieldInto(0, vertexKey);
						message = "Target vertex '" + vertexKey + "' does not exist!.";
					} catch (Throwable t) {}
					throw new Exception(message);
				} else {
					throw new Exception();
				}
			}
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public void open(Configuration parameters) throws Exception {
			// instantiate only the first time
			if (vertexUpdateFunction == null) {
				Class<K> vertexKeyClass = parameters.getClass(KEY_PARAM, null, Key.class);
				Class<V> vertexValueClass = parameters.getClass(VALUE_PARAM, null, Value.class);
				Class<M> messageClass = parameters.getClass(MESSAGE_PARAM, null, Value.class);
				
				vertexKey = InstantiationUtil.instantiate(vertexKeyClass, Key.class);
				vertexValue = InstantiationUtil.instantiate(vertexValueClass, Value.class);
				messageIter = new MessageIterator<M>(InstantiationUtil.instantiate(messageClass, Value.class));
				
				try {
					this.vertexUpdateFunction = (VertexUpdateFunction<K, V, M>) InstantiationUtil.readObjectFromConfig(parameters, UDF_PARAM, parameters.getClassLoader());
				} catch (Exception e) {
					String message = e.getMessage() == null ? "." : ": " + e.getMessage();
					throw new Exception("Could not instantiate VertexUpdateFunction" + message, e);
				}
				
				this.vertexUpdateFunction.init(getIterationRuntimeContext());
				this.vertexUpdateFunction.setup(parameters);
			}
			this.vertexUpdateFunction.preSuperstep();
		}
		
		@Override
		public void close() throws Exception {
			this.vertexUpdateFunction.postSuperstep();
		}
	}
	
	public static final class MessagingDriver<K extends Key, V extends Value, M extends Value, E extends Value> extends CoGroupFunction {

		private static final long serialVersionUID = 1L;
		
		private static final String UDF_PARAM = "spargel.udf";
		private static final String KEY_PARAM = "spargel.key-type";
		private static final String VALUE_PARAM = "spargel.value-type";
		private static final String MESSAGE_PARAM = "spargel.message-type";
		private static final String EDGE_PARAM = "spargel.edge-value";
		
		
		private MessagingFunction<K, V, M, E> messagingFunction;
		
		private K vertexKey;
		private V vertexValue;
		
		@Override
		public void coGroup(Iterator<Record> edges, Iterator<Record> state, Collector<Record> out) throws Exception {
			if (state.hasNext()) {
				Record first = state.next();
				first.getFieldInto(0, vertexKey);
				first.getFieldInto(1, vertexValue);
				messagingFunction.set(edges, out);
				messagingFunction.sendMessages(vertexKey, vertexValue);
			}
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public void open(Configuration parameters) throws Exception {
			// instantiate only the first time
			if (messagingFunction == null) {
				Class<K> vertexKeyClass = parameters.getClass(KEY_PARAM, null, Key.class);
				Class<V> vertexValueClass = parameters.getClass(VALUE_PARAM, null, Value.class);
//				Class<M> messageClass = parameters.getClass(MESSAGE_PARAM, null, Value.class);
				Class<E> edgeClass = parameters.getClass(EDGE_PARAM, null, Value.class);
				
				vertexKey = InstantiationUtil.instantiate(vertexKeyClass, Key.class);
				vertexValue = InstantiationUtil.instantiate(vertexValueClass, Value.class);
				
				K edgeKeyHolder = InstantiationUtil.instantiate(vertexKeyClass, Key.class);
				E edgeValueHolder = InstantiationUtil.instantiate(edgeClass, Value.class);
				
				try {
					this.messagingFunction = (MessagingFunction<K, V, M, E>) InstantiationUtil.readObjectFromConfig(parameters, UDF_PARAM, parameters.getClassLoader());
				} catch (Exception e) {
					String message = e.getMessage() == null ? "." : ": " + e.getMessage();
					throw new Exception("Could not instantiate MessagingFunction" + message, e);
				}
				
				this.messagingFunction.init(getIterationRuntimeContext(), edgeKeyHolder, edgeValueHolder);
				this.messagingFunction.setup(parameters);
			}
			this.messagingFunction.preSuperstep();
		}
		
		@Override
		public void close() throws Exception {
			this.messagingFunction.postSuperstep();
		}
	}
}
