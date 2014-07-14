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
import java.io.ObjectInputStream;
import java.util.ArrayList;
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
import eu.stratosphere.streaming.api.function.SinkFunction;
import eu.stratosphere.streaming.api.invokable.StreamComponentInvokable;
import eu.stratosphere.streaming.api.invokable.StreamRecordInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.ArrayStreamRecord;
import eu.stratosphere.streaming.api.streamrecord.StreamCollectorManager;
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

	private List<Integer> batchSizesNotPartitioned = new ArrayList<Integer>();
	private List<Integer> batchSizesPartitioned = new ArrayList<Integer>();
	private List<Integer> numOfOutputsPartitioned = new ArrayList<Integer>();
	private int keyPosition = 0;

	private List<RecordWriter<StreamRecord>> outputsNotPartitioned = new ArrayList<RecordWriter<StreamRecord>>();
	private List<RecordWriter<StreamRecord>> outputsPartitioned = new ArrayList<RecordWriter<StreamRecord>>();

	protected Configuration configuration;
	protected Collector<Tuple> collector;
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
	
	protected Collector<Tuple> setCollector(List<RecordWriter<StreamRecord>> outputs) {
		long batchTimeout = configuration.getLong("batchTimeout", 1000);

		collector = new StreamCollectorManager<Tuple>(batchSizesNotPartitioned,
				batchSizesPartitioned, numOfOutputsPartitioned, keyPosition, batchTimeout,
				instanceID, outSerializationDelegate, outputsPartitioned, outputsNotPartitioned);
		return collector;
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

	protected AbstractRecordReader getConfigInputs()
			throws StreamComponentException {
		int numberOfInputs = configuration.getInteger("numberOfInputs", 0);

		if (numberOfInputs < 2) {

			return new StreamRecordReader(this, ArrayStreamRecord.class,
					inDeserializationDelegate, inTupleSerializer);

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
			setPartitioner(i, partitioners);
			ChannelSelector<StreamRecord> outputPartitioner = partitioners.get(i);

			outputs.add(new RecordWriter<StreamRecord>(this, outputPartitioner));

			if (outputsPartitioned.size() < batchSizesPartitioned.size()) {
				outputsPartitioned.add(outputs.get(i));
			} else {
				outputsNotPartitioned.add(outputs.get(i));
			}
		}
	}

	private void setPartitioner(int numberOfOutputs,
			List<ChannelSelector<StreamRecord>> partitioners) {

		Class<? extends ChannelSelector<StreamRecord>> partitioner = configuration.getClass(
				"partitionerClass_" + numberOfOutputs, DefaultPartitioner.class,
				ChannelSelector.class);

		Integer batchSize = configuration.getInteger("batchSize_" + numberOfOutputs, 1);

		try {
			if (partitioner.equals(FieldsPartitioner.class)) {
				batchSizesPartitioned.add(batchSize);
				numOfOutputsPartitioned.add(configuration
						.getInteger("numOfOutputs_" + numberOfOutputs, -1));
				// TODO:force one partitioning field
				keyPosition = configuration.getInteger("partitionerIntParam_" + numberOfOutputs, 1);

				partitioners.add(partitioner.getConstructor(int.class).newInstance(keyPosition));

			} else {
				batchSizesNotPartitioned.add(batchSize);
				partitioners.add(partitioner.newInstance());
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
				userFunction.invoke(record, collector);
			}

		} else if (inputs instanceof StreamRecordReader) {
			StreamRecordReader recordReader = (StreamRecordReader) inputs;

			while (recordReader.hasNext()) {
				StreamRecord record = recordReader.next();
				userFunction.invoke(record, collector);
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
	protected StreamComponentInvokable getInvokable(Class<? extends StreamComponentInvokable> userFunctionClass) {
		StreamComponentInvokable userFunction = null;

		byte[] userFunctionSerialized = configuration.getBytes("serializedudf", null);

		try {
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(
					userFunctionSerialized));
			userFunction = (StreamComponentInvokable) ois.readObject();
		} catch (Exception e) {
			if (log.isErrorEnabled()) {
				log.error("Cannot instanciate user function: " + userFunctionClass.getSimpleName());
			}
		}

		return userFunction;
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
