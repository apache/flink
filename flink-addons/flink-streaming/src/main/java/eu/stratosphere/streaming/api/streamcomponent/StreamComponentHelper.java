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
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
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
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.io.AbstractRecordReader;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.streaming.api.SinkFunction;
import eu.stratosphere.streaming.api.StreamCollectorManager;
import eu.stratosphere.streaming.api.invokable.DefaultSinkInvokable;
import eu.stratosphere.streaming.api.invokable.DefaultSourceInvokable;
import eu.stratosphere.streaming.api.invokable.DefaultTaskInvokable;
import eu.stratosphere.streaming.api.invokable.StreamComponent;
import eu.stratosphere.streaming.api.invokable.StreamRecordInvokable;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.ArrayStreamRecord;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.faulttolerance.AckEvent;
import eu.stratosphere.streaming.faulttolerance.AckEventListener;
import eu.stratosphere.streaming.faulttolerance.FailEvent;
import eu.stratosphere.streaming.faulttolerance.FailEventListener;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceUtil;
import eu.stratosphere.streaming.partitioner.DefaultPartitioner;
import eu.stratosphere.streaming.partitioner.FieldsPartitioner;
import eu.stratosphere.util.Collector;

public final class StreamComponentHelper<T extends AbstractInvokable> {
	private static final Log log = LogFactory.getLog(StreamComponentHelper.class);
	private static int numComponents = 0;

	private TupleTypeInfo<Tuple> inTupleTypeInfo = null;
	private TupleSerializer<Tuple> inTupleSerializer = null;
	private DeserializationDelegate<Tuple> inDeserializationDelegate = null;

	private TupleTypeInfo<Tuple> outTupleTypeInfo = null;
	private TupleSerializer<Tuple> outTupleSerializer = null;
	private SerializationDelegate<Tuple> outSerializationDelegate = null;

	public Collector<Tuple> collector;
	private List<Integer> batchSizesNotPartitioned = new ArrayList<Integer>();
	private List<Integer> batchSizesPartitioned = new ArrayList<Integer>();
	private List<Integer> numOfOutputsPartitioned = new ArrayList<Integer>();
	private int keyPosition = 0;

	private List<RecordWriter<StreamRecord>> outputsNotPartitioned = new ArrayList<RecordWriter<StreamRecord>>();
	private List<RecordWriter<StreamRecord>> outputsPartitioned = new ArrayList<RecordWriter<StreamRecord>>();

	public static int newComponent() {
		numComponents++;
		return numComponents;
	}

	public void setAckListener(FaultToleranceUtil recordBuffer, int sourceInstanceID,
			List<RecordWriter<StreamRecord>> outputs) {

		EventListener[] ackListeners = new EventListener[outputs.size()];

		for (int i = 0; i < outputs.size(); i++) {
			ackListeners[i] = new AckEventListener(sourceInstanceID, recordBuffer, i);
			outputs.get(i).subscribeToEvent(ackListeners[i], AckEvent.class);
		}

	}

	public void setFailListener(FaultToleranceUtil recordBuffer, int sourceInstanceID,
			List<RecordWriter<StreamRecord>> outputs) {

		EventListener[] failListeners = new EventListener[outputs.size()];

		for (int i = 0; i < outputs.size(); i++) {
			failListeners[i] = new FailEventListener(sourceInstanceID, recordBuffer, i);
			outputs.get(i).subscribeToEvent(failListeners[i], FailEvent.class);
		}

	}

	public Collector<Tuple> setCollector(Configuration taskConfiguration, int id,
			List<RecordWriter<StreamRecord>> outputs) {

		long batchTimeout = taskConfiguration.getLong("batchTimeout", 1000);

		collector = new StreamCollectorManager<Tuple>(batchSizesNotPartitioned,
				batchSizesPartitioned, numOfOutputsPartitioned, keyPosition, batchTimeout, id,
				outSerializationDelegate, outputsPartitioned, outputsNotPartitioned);
		return collector;
	}

	public void setSerializers(Configuration taskConfiguration) {
		byte[] operatorBytes = taskConfiguration.getBytes("operator", null);
		String operatorName = taskConfiguration.getString("operatorName", "");

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

	public void setSinkSerializer() {
		if (outSerializationDelegate != null) {
			inTupleTypeInfo = outTupleTypeInfo;

			inTupleSerializer = inTupleTypeInfo.createSerializer();
			inDeserializationDelegate = new DeserializationDelegate<Tuple>(inTupleSerializer);
		}
	}

	public AbstractRecordReader getConfigInputs(T taskBase, Configuration taskConfiguration)
			throws StreamComponentException {
		int numberOfInputs = taskConfiguration.getInteger("numberOfInputs", 0);

		if (numberOfInputs < 2) {
			if (taskBase instanceof StreamTask) {
				return new StreamRecordReader((StreamTask) taskBase, ArrayStreamRecord.class,
						inDeserializationDelegate, inTupleSerializer);
			} else if (taskBase instanceof StreamSink) {
				return new StreamRecordReader((StreamSink) taskBase, ArrayStreamRecord.class,
						inDeserializationDelegate, inTupleSerializer);
			} else {
				throw new StreamComponentException("Nonsupported object passed to setConfigInputs");
			}
		} else {
			@SuppressWarnings("unchecked")
			MutableRecordReader<StreamRecord>[] recordReaders = (MutableRecordReader<StreamRecord>[]) new MutableRecordReader<?>[numberOfInputs];

			for (int i = 0; i < numberOfInputs; i++) {

				if (taskBase instanceof StreamTask) {
					recordReaders[i] = new MutableRecordReader<StreamRecord>((StreamTask) taskBase);
				} else if (taskBase instanceof StreamSink) {
					recordReaders[i] = new MutableRecordReader<StreamRecord>((StreamSink) taskBase);
				} else {
					throw new StreamComponentException(
							"Nonsupported object passed to setConfigInputs");
				}
			}
			return new UnionStreamRecordReader(recordReaders, ArrayStreamRecord.class,
					inDeserializationDelegate, inTupleSerializer);
		}
	}

	public void setConfigOutputs(T taskBase, Configuration taskConfiguration,
			List<RecordWriter<StreamRecord>> outputs,
			List<ChannelSelector<StreamRecord>> partitioners) throws StreamComponentException {

		int numberOfOutputs = taskConfiguration.getInteger("numberOfOutputs", 0);

		for (int i = 0; i < numberOfOutputs; i++) {
			setPartitioner(taskConfiguration, i, partitioners);
			ChannelSelector<StreamRecord> outputPartitioner = partitioners.get(i);

			if (taskBase instanceof StreamTask) {
				outputs.add(new RecordWriter<StreamRecord>((StreamTask) taskBase,
						StreamRecord.class, outputPartitioner));
			} else if (taskBase instanceof StreamSource) {
				outputs.add(new RecordWriter<StreamRecord>((StreamSource) taskBase,
						StreamRecord.class, outputPartitioner));
			} else {
				throw new StreamComponentException("Nonsupported object passed to setConfigOutputs");
			}
			if (outputsPartitioned.size() < batchSizesPartitioned.size()) {
				outputsPartitioned.add(outputs.get(i));
			} else {
				outputsNotPartitioned.add(outputs.get(i));
			}
		}
	}

	/**
	 * Reads and creates a StreamComponent from the config.
	 * 
	 * @param userFunctionClass
	 *            Class of the invokable function
	 * @param config
	 *            Configuration object
	 * @return The StreamComponent object
	 */
	private StreamComponent getInvokable(Class<? extends StreamComponent> userFunctionClass,
			Configuration config) {
		StreamComponent userFunction = null;

		byte[] userFunctionSerialized = config.getBytes("serializedudf", null);

		try {
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(
					userFunctionSerialized));
			userFunction = (StreamComponent) ois.readObject();
		} catch (Exception e) {
			if (log.isErrorEnabled()) {
				log.error("Cannot instanciate user function: " + userFunctionClass.getSimpleName());
			}
		}

		return userFunction;
	}

	@SuppressWarnings("rawtypes")
	public UserSinkInvokable getSinkInvokable(Configuration config) {
		Class<? extends UserSinkInvokable> userFunctionClass = config.getClass("userfunction",
				DefaultSinkInvokable.class, UserSinkInvokable.class);
		return (UserSinkInvokable) getInvokable(userFunctionClass, config);
	}

	// TODO consider logging stack trace!
	@SuppressWarnings("rawtypes")
	public UserTaskInvokable getTaskInvokable(Configuration config) {

		// Default value is a TaskInvokable even if it was called from a source
		Class<? extends UserTaskInvokable> userFunctionClass = config.getClass("userfunction",
				DefaultTaskInvokable.class, UserTaskInvokable.class);
		return (UserTaskInvokable) getInvokable(userFunctionClass, config);
	}

	@SuppressWarnings("rawtypes")
	public UserSourceInvokable getSourceInvokable(Configuration config) {
		
		// Default value is a TaskInvokable even if it was called from a source
		Class<? extends UserSourceInvokable> userFunctionClass = config.getClass("userfunction",
				DefaultSourceInvokable.class, UserSourceInvokable.class);
		return (UserSourceInvokable) getInvokable(userFunctionClass, config);
	}

	// TODO find a better solution for this
	public void threadSafePublish(AbstractTaskEvent event, AbstractRecordReader inputs)
			throws InterruptedException, IOException {

		boolean concurrentModificationOccured = false;
		while (!concurrentModificationOccured) {
			try {
				inputs.publishEvent(event);
				concurrentModificationOccured = true;
			} catch (ConcurrentModificationException exeption) {
				if (log.isTraceEnabled()) {
					log.trace("Waiting to publish " + event.getClass());
				}
			}
		}
	}

	private void setPartitioner(Configuration config, int numberOfOutputs,
			List<ChannelSelector<StreamRecord>> partitioners) {
		Class<? extends ChannelSelector<StreamRecord>> partitioner = config.getClass(
				"partitionerClass_" + numberOfOutputs, DefaultPartitioner.class,
				ChannelSelector.class);

		Integer batchSize = config.getInteger("batchSize_" + numberOfOutputs, 1);

		try {
			if (partitioner.equals(FieldsPartitioner.class)) {
				batchSizesPartitioned.add(batchSize);
				numOfOutputsPartitioned.add(config
						.getInteger("numOfOutputs_" + numberOfOutputs, -1));
				// TODO:force one partitioning field
				keyPosition = config.getInteger("partitionerIntParam_" + numberOfOutputs, 1);

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

	public void invokeRecords(StreamRecordInvokable userFunction, AbstractRecordReader inputs)
			throws Exception {
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

}