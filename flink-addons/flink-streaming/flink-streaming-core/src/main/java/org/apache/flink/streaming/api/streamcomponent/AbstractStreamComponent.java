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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.AbstractFunction;
import org.apache.flink.api.java.functions.FilterFunction;
import org.apache.flink.api.java.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.api.AbstractRecordReader;
import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.io.network.api.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.collector.DirectedStreamCollector;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.collector.StreamCollector;
import org.apache.flink.streaming.api.function.SinkFunction;
import org.apache.flink.streaming.api.invokable.StreamComponentInvokable;
import org.apache.flink.streaming.api.invokable.StreamRecordInvokable;
import org.apache.flink.streaming.api.invokable.UserSourceInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.partitioner.DefaultPartitioner;
import org.apache.flink.streaming.partitioner.FieldsPartitioner;
import org.apache.flink.util.Collector;

public abstract class AbstractStreamComponent<IN extends Tuple, OUT extends Tuple> extends AbstractInvokable {
	private final Log log = LogFactory.getLog(AbstractStreamComponent.class);

	protected TupleTypeInfo<IN> inTupleTypeInfo = null;
	protected TupleSerializer<IN> inTupleSerializer = null;
	protected DeserializationDelegate<IN> inDeserializationDelegate = null;

	protected TupleTypeInfo<OUT> outTupleTypeInfo = null;
	protected TupleSerializer<OUT> outTupleSerializer = null;
	protected SerializationDelegate<OUT> outSerializationDelegate = null;

	protected Configuration configuration;
	protected StreamCollector<OUT> collectorManager;
	protected int instanceID;
	protected String name;
	private static int numComponents = 0;

	protected static int newComponent() {
		numComponents++;
		return numComponents;
	}

	protected void initialize() {
		configuration = getTaskConfiguration();
		name = configuration.getString("componentName", "MISSING_COMPONENT_NAME");
	}

	@SuppressWarnings("unchecked")
	protected Collector<OUT> setCollector() {
		if (configuration.getBoolean("directedEmit", false)) {
			OutputSelector<OUT> outputSelector = null;
			try {
				outputSelector = (OutputSelector<OUT>) deserializeObject(configuration.getBytes(
						"outputSelector", null));
			} catch (Exception e) {
				if (log.isErrorEnabled()) {
					log.error("Cannot instantiate OutputSelector");
				}
			}

			collectorManager = new DirectedStreamCollector<OUT>(instanceID, outSerializationDelegate,
					outputSelector);
		} else {
			collectorManager = new StreamCollector<OUT>(instanceID, outSerializationDelegate);
		}
		return collectorManager;
	}

	protected void setSerializers() {
		byte[] operatorBytes = configuration.getBytes("operator", null);
		String operatorName = configuration.getString("operatorName", "");

		Object function = null;
		try {
			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(operatorBytes));
			function = in.readObject();

			if (operatorName.equals("flatMap")) {
				setSerializerDeserializer(function, FlatMapFunction.class);
			} else if (operatorName.equals("map")) {
				setSerializerDeserializer(function, MapFunction.class);
			} else if (operatorName.equals("batchReduce")) {
				setSerializerDeserializer(function, GroupReduceFunction.class);
			} else if (operatorName.equals("filter")) {
				setSerializerDeserializer(function, FilterFunction.class);
			} else if (operatorName.equals("sink")) {
				setDeserializer(function, SinkFunction.class);
			} else if (operatorName.equals("source")) {
				setSerializer(function, UserSourceInvokable.class, 0);
			} else if (operatorName.equals("elements")) {
				outTupleTypeInfo = new TupleTypeInfo<OUT>(TypeExtractor.getForObject(function));

				outTupleSerializer = outTupleTypeInfo.createSerializer();
				outSerializationDelegate = new SerializationDelegate<OUT>(outTupleSerializer);
			} else {
				throw new Exception("Wrong operator name!");
			}

		} catch (Exception e) {
			throw new StreamComponentException("Nonsupported object (named " + operatorName
					+ ") passed as operator");
		}
	}

	private void setSerializerDeserializer(Object function, Class<? extends AbstractFunction> clazz) {
		setDeserializer(function, clazz);
		setSerializer(function, clazz, 1);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void setDeserializer(Object function, Class<? extends AbstractFunction> clazz) {
		inTupleTypeInfo = (TupleTypeInfo) TypeExtractor.createTypeInfo(clazz, function.getClass(),
				0, null, null);

		inTupleSerializer = inTupleTypeInfo.createSerializer();
		inDeserializationDelegate = new DeserializationDelegate<IN>(inTupleSerializer);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void setSerializer(Object function, Class<?> clazz, int typeParameter) {
		outTupleTypeInfo = (TupleTypeInfo) TypeExtractor.createTypeInfo(clazz, function.getClass(),
				typeParameter, null, null);

		outTupleSerializer = outTupleTypeInfo.createSerializer();
		outSerializationDelegate = new SerializationDelegate<OUT>(outTupleSerializer);
	}

	@SuppressWarnings("unchecked")
	protected void setSinkSerializer() {
		if (outSerializationDelegate != null) {
			inTupleTypeInfo = (TupleTypeInfo<IN>) outTupleTypeInfo;

			inTupleSerializer = inTupleTypeInfo.createSerializer();
			inDeserializationDelegate = new DeserializationDelegate<IN>(inTupleSerializer);
		}
	}

	@SuppressWarnings("unchecked")
	protected AbstractRecordReader getConfigInputs() throws StreamComponentException {
		int numberOfInputs = configuration.getInteger("numberOfInputs", 0);

		if (numberOfInputs < 2) {

			return new StreamRecordReader<IN>(this, (Class<? extends StreamRecord<IN>>) StreamRecord.class, inDeserializationDelegate,
					inTupleSerializer);

		} else {
			MutableRecordReader<StreamRecord<IN>>[] recordReaders = (MutableRecordReader<StreamRecord<IN>>[]) new MutableRecordReader<?>[numberOfInputs];

			for (int i = 0; i < numberOfInputs; i++) {
				recordReaders[i] = new MutableRecordReader<StreamRecord<IN>>(this);
			}
			return new UnionStreamRecordReader<IN>(recordReaders, (Class<? extends StreamRecord<IN>>) StreamRecord.class,
					inDeserializationDelegate, inTupleSerializer);
		}
	}

	protected void setConfigOutputs(List<RecordWriter<StreamRecord<OUT>>> outputs) throws StreamComponentException {

		int numberOfOutputs = configuration.getInteger("numberOfOutputs", 0);

		for (int i = 0; i < numberOfOutputs; i++) {
			setPartitioner(i, outputs);
		}
	}

	@SuppressWarnings("unchecked")
	private void setPartitioner(int numberOfOutputs,
			List<RecordWriter<StreamRecord<OUT>>> outputs) {

		Class<? extends ChannelSelector<StreamRecord<OUT>>> partitioner = (Class<? extends ChannelSelector<StreamRecord<OUT>>>) configuration.getClass(
				"partitionerClass_" + numberOfOutputs, DefaultPartitioner.class,
				ChannelSelector.class);

		try {
			if (partitioner.equals(FieldsPartitioner.class)) {

				int keyPosition = configuration.getInteger(
						"partitionerIntParam_" + numberOfOutputs, 1);
				ChannelSelector<StreamRecord<OUT>> outputPartitioner = partitioner.getConstructor(
						int.class).newInstance(keyPosition);
				RecordWriter<StreamRecord<OUT>> output = new RecordWriter<StreamRecord<OUT>>(this,
						outputPartitioner);
				outputs.add(output);
				String outputName = configuration.getString("outputName_" + numberOfOutputs, null);
				if (collectorManager != null) {
					collectorManager.addOutput(output, outputName);
				}

			} else {
				ChannelSelector<StreamRecord<OUT>> outputPartitioner = partitioner.newInstance();
				RecordWriter<StreamRecord<OUT>> output = new RecordWriter<StreamRecord<OUT>>(this,
						outputPartitioner);
				outputs.add(output);
				String outputName = configuration.getString("outputName_" + numberOfOutputs, null);
				if (collectorManager != null) {
					collectorManager.addOutput(output, outputName);
				}
			}
			if (log.isTraceEnabled()) {
				log.trace("Partitioner set: " + partitioner.getSimpleName() + " with "
						+ numberOfOutputs + " outputs");
			}
		} catch (Exception e) {
			if (log.isErrorEnabled()) {
				log.error("Error while setting partitioner: " + partitioner.getSimpleName()
						+ " with " + numberOfOutputs + " outputs", e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected void invokeRecords(StreamRecordInvokable<IN, OUT> userFunction,
			AbstractRecordReader inputs) throws Exception {
		if (inputs instanceof UnionStreamRecordReader) {
			UnionStreamRecordReader<IN> recordReader = (UnionStreamRecordReader<IN>) inputs;
			while (recordReader.hasNext()) {
				StreamRecord<IN> record = recordReader.next();
				userFunction.invoke(record, collectorManager);
			}

		} else if (inputs instanceof StreamRecordReader) {
			StreamRecordReader<IN> recordReader = (StreamRecordReader<IN>) inputs;

			while (recordReader.hasNext()) {
				StreamRecord<IN> record = recordReader.next();
				userFunction.invoke(record, collectorManager);
			}
		}
	}

	/**
	 * Reads and creates a StreamComponent from the config.
	 * 
	 * @param userFunctionClass
	 *            Class of the invokable function
	 * @return The StreamComponent object
	 */
	protected StreamComponentInvokable getInvokable(
			Class<? extends StreamComponentInvokable> userFunctionClass) {
		StreamComponentInvokable userFunction = null;

		byte[] userFunctionSerialized = configuration.getBytes("serializedudf", null);

		try {
			userFunction = (StreamComponentInvokable) deserializeObject(userFunctionSerialized);
		} catch (Exception e) {
			if (log.isErrorEnabled()) {
				log.error("Cannot instantiate user function: " + userFunctionClass.getSimpleName());
			}
		}

		return userFunction;
	}

	private static Object deserializeObject(byte[] serializedObject) throws IOException,
			ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(serializedObject));
		return ois.readObject();
	}

	protected abstract void setInvokable();

	// protected void threadSafePublish(AbstractTaskEvent event,
	// AbstractRecordReader inputs)
	// throws InterruptedException, IOException {
	//
	// boolean concurrentModificationOccured = false;
	// while (!concurrentModificationOccured) {
	// try {
	// inputs.publishEvent(event);
	// concurrentModificationOccured = true;
	// } catch (ConcurrentModificationException exeption) {
	// if (log.isTraceEnabled()) {
	// log.trace("Waiting to publish " + event.getClass());
	// }
	// }
	// }
	// }
	//
	// protected void setAckListener(FaultToleranceUtil recordBuffer, int
	// sourceInstanceID,
	// List<RecordWriter<StreamRecord>> outputs) {
	//
	// EventListener[] ackListeners = new EventListener[outputs.size()];
	//
	// for (int i = 0; i < outputs.size(); i++) {
	// ackListeners[i] = new AckEventListener(sourceInstanceID, recordBuffer,
	// i);
	// outputs.get(i).subscribeToEvent(ackListeners[i], AckEvent.class);
	// }
	//
	// }
	//
	// protected void setFailListener(FaultToleranceUtil recordBuffer, int
	// sourceInstanceID,
	// List<RecordWriter<StreamRecord>> outputs) {
	//
	// EventListener[] failListeners = new EventListener[outputs.size()];
	//
	// for (int i = 0; i < outputs.size(); i++) {
	// failListeners[i] = new FailEventListener(sourceInstanceID, recordBuffer,
	// i);
	// outputs.get(i).subscribeToEvent(failListeners[i], FailEvent.class);
	// }
	// }
}
