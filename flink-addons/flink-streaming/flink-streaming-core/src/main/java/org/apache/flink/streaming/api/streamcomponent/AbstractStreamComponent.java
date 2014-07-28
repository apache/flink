/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.api.streamcomponent;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.FilterFunction;
import org.apache.flink.api.java.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.collector.DirectedStreamCollector;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.collector.StreamCollector;
import org.apache.flink.streaming.api.function.co.CoMapFunction;
import org.apache.flink.streaming.api.invokable.StreamComponentInvokable;
import org.apache.flink.streaming.api.invokable.UserSourceInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public abstract class AbstractStreamComponent<OUT extends Tuple> extends AbstractInvokable {

	private static final Log LOG = LogFactory.getLog(AbstractStreamComponent.class);

	protected TupleTypeInfo<OUT> outTupleTypeInfo = null;
	protected StreamRecordSerializer<OUT> outTupleSerializer = null;
	protected SerializationDelegate<StreamRecord<OUT>> outSerializationDelegate = null;

	protected StreamConfig configuration;
	protected StreamCollector<OUT> collector;
	protected int instanceID;
	protected String name;
	private static int numComponents = 0;
	protected boolean isMutable;
	protected Object function;
	protected String functionName;
	
	protected static int newComponent() {
		numComponents++;
		return numComponents;
	}

	protected void initialize() {
		this.configuration = new StreamConfig(getTaskConfiguration());
		this.name = configuration.getComponentName();
		this.isMutable = configuration.getMutability();
		this.functionName = configuration.getFunctionName();
		this.function = configuration.getFunction();

	}

	protected Collector<OUT> setCollector() {
		if (configuration.getDirectedEmit()) {
			OutputSelector<OUT> outputSelector = configuration.getOutputSelector();

			collector = new DirectedStreamCollector<OUT>(instanceID, outSerializationDelegate,
					outputSelector);
		} else {
			collector = new StreamCollector<OUT>(instanceID, outSerializationDelegate);
		}
		return collector;
	}

	protected void setSerializers() {
		try {
			if (functionName.equals("flatMap")) {
				setSerializer(function, FlatMapFunction.class, 1);
			} else if (functionName.equals("map")) {
				setSerializer(function, MapFunction.class, 1);
			} else if (functionName.equals("batchReduce")) {
				setSerializer(function, GroupReduceFunction.class, 1);
			} else if (functionName.equals("filter")) {
				setSerializer(function, FilterFunction.class, 0);
			} else if (functionName.equals("source")) {
				setSerializer(function, UserSourceInvokable.class, 0);
			} else if (functionName.equals("coMap")) {
				setSerializer(function, CoMapFunction.class, 2);
			} else if (functionName.equals("elements")) {
				outTupleTypeInfo = new TupleTypeInfo<OUT>(TypeExtractor.getForObject(function));

				outTupleSerializer = new StreamRecordSerializer<OUT>(outTupleTypeInfo.createSerializer());
				outSerializationDelegate = new SerializationDelegate<StreamRecord<OUT>>(
						outTupleSerializer);
			} else {
				throw new Exception("Wrong operator name: " + functionName);
			}
		} catch (Exception e) {
			throw new StreamComponentException(e);
		}
	
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected void setSerializer(Object function, Class<?> clazz, int typeParameter) {
		outTupleTypeInfo = (TupleTypeInfo) TypeExtractor.createTypeInfo(clazz, function.getClass(),
				typeParameter, null, null);

		outTupleSerializer = new StreamRecordSerializer(outTupleTypeInfo.createSerializer());
		outSerializationDelegate = new SerializationDelegate<StreamRecord<OUT>>(outTupleSerializer);
	}

	protected void setConfigOutputs(
			List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputs) {
		setSerializers();
		setCollector();
		
		int numberOfOutputs = configuration.getNumberOfOutputs();

		for (int i = 0; i < numberOfOutputs; i++) {
			setPartitioner(i, outputs);
		}
	}

	private void setPartitioner(int outputNumber,
			List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputs) {
		StreamPartitioner<OUT> outputPartitioner = null;
		
		try {
			outputPartitioner = configuration.getPartitioner(outputNumber);

			RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output;

			long bufferTimeout = configuration.getBufferTimeout();

			if (bufferTimeout > 0) {
				output = new StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>(this,
						outputPartitioner, bufferTimeout);
			} else {
				output = new RecordWriter<SerializationDelegate<StreamRecord<OUT>>>(this,
						outputPartitioner);
			}

			outputs.add(output);
			String outputName = configuration.getOutputName(outputNumber);

			if (collector != null) {
				collector.addOutput(output, outputName);
			}

			if (LOG.isTraceEnabled()) {
				LOG.trace("Partitioner set: " + outputPartitioner.getClass().getSimpleName()
						+ " with " + outputNumber + " outputs");
			}
		} catch (Exception e) {
			throw new StreamComponentException("Cannot deserialize partitioner "
					+ outputPartitioner.getClass().getSimpleName() + " of " + name + " with "
					+ outputNumber + " outputs", e);
		}
	}

	/**
	 * Reads and creates a StreamComponent from the config.
	 * 
	 * @param userFunctionClass
	 *            Class of the invokable function
	 * @return The StreamComponent object
	 */
	protected <T extends StreamComponentInvokable> T getInvokable() {
		return configuration.getUserInvokableObject();
	}

	protected <IN extends Tuple> MutableObjectIterator<StreamRecord<IN>> createInputIterator(
			MutableReader<?> inputReader, TypeSerializer<?> serializer) {

		// generic data type serialization
		@SuppressWarnings("unchecked")
		MutableReader<DeserializationDelegate<?>> reader = (MutableReader<DeserializationDelegate<?>>) inputReader;
		@SuppressWarnings({ "unchecked", "rawtypes" })
		final MutableObjectIterator<StreamRecord<IN>> iter = new ReaderIterator(reader, serializer);
		return iter;
	}

	@SuppressWarnings("unchecked")
	protected static <T> T deserializeObject(byte[] serializedObject) throws IOException,
			ClassNotFoundException {
		return (T) SerializationUtils.deserialize(serializedObject);
	}


	@Override
	public void registerInputOutput() {
		initialize();
		setInputsOutputs();		
		setInvokable();
	}
	
	protected abstract void setInputsOutputs();
	
	protected abstract void setInvokable();

}
