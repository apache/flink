/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api.streamcomponent;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.runtime.io.api.AbstractRecordReader;
import eu.stratosphere.runtime.io.api.ChannelSelector;
import eu.stratosphere.runtime.io.api.MutableRecordReader;
import eu.stratosphere.runtime.io.api.RecordWriter;
import eu.stratosphere.streaming.api.collector.DirectedStreamCollectorManager;
import eu.stratosphere.streaming.api.collector.OutputSelector;
import eu.stratosphere.streaming.api.collector.StreamCollectorManager;
import eu.stratosphere.streaming.api.function.SinkFunction;
import eu.stratosphere.streaming.api.invokable.StreamComponentInvokable;
import eu.stratosphere.streaming.api.invokable.StreamRecordInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.ArrayStreamRecord;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.partitioner.DefaultPartitioner;
import eu.stratosphere.streaming.partitioner.FieldsPartitioner;
import eu.stratosphere.util.Collector;

public abstract class AbstractStreamComponent extends AbstractInvokable {
	private final Log log = LogFactory.getLog(AbstractStreamComponent.class);

	protected TupleTypeInfo<Tuple> inTupleTypeInfo = null;
	protected TupleSerializer<Tuple> inTupleSerializer = null;
	protected DeserializationDelegate<Tuple> inDeserializationDelegate = null;

	protected TupleTypeInfo<Tuple> outTupleTypeInfo = null;
	protected TupleSerializer<Tuple> outTupleSerializer = null;
	protected SerializationDelegate<Tuple> outSerializationDelegate = null;

	protected Configuration configuration;
	protected StreamCollectorManager<Tuple> collectorManager;
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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Collector<Tuple> setCollector() {
		long batchTimeout = configuration.getLong("batchTimeout", 1000);

		if (configuration.getBoolean("directedEmit", false)) {
			OutputSelector outputSelector = null;
			try {
				outputSelector = (OutputSelector) deserializeObject(configuration.getBytes(
						"outputSelector", null));
			} catch (Exception e) {
				if (log.isErrorEnabled()) {
					log.error("Cannot instantiate OutputSelector");
				}
			}

			collectorManager = new DirectedStreamCollectorManager<Tuple>(outSerializationDelegate,
					instanceID, batchTimeout, outputSelector);
		} else {
			collectorManager = new StreamCollectorManager<Tuple>(outSerializationDelegate,
					instanceID, batchTimeout);
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
				outTupleTypeInfo = new TupleTypeInfo<Tuple>(TypeExtractor.getForObject(function));

				outTupleSerializer = outTupleTypeInfo.createSerializer();
				outSerializationDelegate = new SerializationDelegate<Tuple>(outTupleSerializer);
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
		inDeserializationDelegate = new DeserializationDelegate<Tuple>(inTupleSerializer);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void setSerializer(Object function, Class<?> clazz, int typeParameter) {
		outTupleTypeInfo = (TupleTypeInfo) TypeExtractor.createTypeInfo(clazz, function.getClass(),
				typeParameter, null, null);

		outTupleSerializer = outTupleTypeInfo.createSerializer();
		outSerializationDelegate = new SerializationDelegate<Tuple>(outTupleSerializer);
	}

	protected void setSinkSerializer() {
		if (outSerializationDelegate != null) {
			inTupleTypeInfo = outTupleTypeInfo;

			inTupleSerializer = inTupleTypeInfo.createSerializer();
			inDeserializationDelegate = new DeserializationDelegate<Tuple>(inTupleSerializer);
		}
	}

	protected AbstractRecordReader getConfigInputs() throws StreamComponentException {
		int numberOfInputs = configuration.getInteger("numberOfInputs", 0);

		if (numberOfInputs < 2) {

			return new StreamRecordReader(this, ArrayStreamRecord.class, inDeserializationDelegate,
					inTupleSerializer);

		} else {
			@SuppressWarnings("unchecked")
			MutableRecordReader<StreamRecord>[] recordReaders = (MutableRecordReader<StreamRecord>[]) new MutableRecordReader<?>[numberOfInputs];

			for (int i = 0; i < numberOfInputs; i++) {
				recordReaders[i] = new MutableRecordReader<StreamRecord>(this);
			}
			return new UnionStreamRecordReader(recordReaders, ArrayStreamRecord.class,
					inDeserializationDelegate, inTupleSerializer);
		}
	}

	protected void setConfigOutputs(List<RecordWriter<StreamRecord>> outputs,
			List<ChannelSelector<StreamRecord>> partitioners) throws StreamComponentException {

		int numberOfOutputs = configuration.getInteger("numberOfOutputs", 0);

		for (int i = 0; i < numberOfOutputs; i++) {
			setPartitioner(i, partitioners, outputs);
		}
	}

	private void setPartitioner(int numberOfOutputs,
			List<ChannelSelector<StreamRecord>> partitioners,
			List<RecordWriter<StreamRecord>> outputs) {

		Class<? extends ChannelSelector<StreamRecord>> partitioner = configuration.getClass(
				"partitionerClass_" + numberOfOutputs, DefaultPartitioner.class,
				ChannelSelector.class);

		Integer batchSize = configuration.getInteger("batchSize_" + numberOfOutputs, 1);

		try {
			if (partitioner.equals(FieldsPartitioner.class)) {

				int parallelism = configuration.getInteger("numOfOutputs_" + numberOfOutputs, -1);
				// TODO:force one partitioning field
				int keyPosition = configuration.getInteger(
						"partitionerIntParam_" + numberOfOutputs, 1);
				ChannelSelector<StreamRecord> outputPartitioner = partitioner.getConstructor(
						int.class).newInstance(keyPosition);
				RecordWriter<StreamRecord> output = new RecordWriter<StreamRecord>(this,
						outputPartitioner);
				outputs.add(output);
				partitioners.add(outputPartitioner);
				String outputName = configuration.getString("outputName_" + numberOfOutputs, null);
				if (collectorManager != null) {
					collectorManager.addPartitionedCollector(output, parallelism, keyPosition,
							batchSize, outputName);
				}

			} else {
				ChannelSelector<StreamRecord> outputPartitioner = partitioner.newInstance();
				partitioners.add(outputPartitioner);
				RecordWriter<StreamRecord> output = new RecordWriter<StreamRecord>(this,
						outputPartitioner);
				outputs.add(output);
				String outputName = configuration.getString("outputName_" + numberOfOutputs, null);
				if (collectorManager != null) {
					collectorManager.addNotPartitionedCollector(output, batchSize, outputName);
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

	protected void invokeRecords(StreamRecordInvokable<Tuple, Tuple> userFunction,
			AbstractRecordReader inputs) throws Exception {
		if (inputs instanceof UnionStreamRecordReader) {
			UnionStreamRecordReader recordReader = (UnionStreamRecordReader) inputs;
			while (recordReader.hasNext()) {
				StreamRecord record = recordReader.next();
				userFunction.invoke(record, collectorManager);
			}

		} else if (inputs instanceof StreamRecordReader) {
			StreamRecordReader recordReader = (StreamRecordReader) inputs;

			while (recordReader.hasNext()) {
				StreamRecord record = recordReader.next();
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
