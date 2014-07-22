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
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.AbstractFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.runtime.io.network.api.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.MutableUnionRecordReader;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.function.co.CoMapFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoMapInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.util.MutableObjectIterator;

public class CoStreamTask<IN1 extends Tuple, IN2 extends Tuple, OUT extends Tuple> extends
		AbstractStreamComponent<OUT> {
	private static final Log LOG = LogFactory.getLog(CoStreamTask.class);

	protected StreamRecordSerializer<IN1> inTupleSerializer1 = null;
	protected StreamRecordSerializer<IN2> inTupleSerializer2 = null;

	private MutableReader<IOReadableWritable> inputs1;
	private MutableReader<IOReadableWritable> inputs2;
	MutableObjectIterator<StreamRecord<IN1>> inputIter1;
	MutableObjectIterator<StreamRecord<IN2>> inputIter2;

	private List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputs;
	private CoInvokable<IN1, IN2, OUT> userFunction;
	private int[] numberOfOutputChannels;
	private static int numTasks;

	public CoStreamTask() {

		outputs = new LinkedList<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>();
		userFunction = null;
		numTasks = newComponent();
		instanceID = numTasks;
	}

	protected void setSerializers() {
		byte[] operatorBytes = configuration.getBytes("operator", null);
		String operatorName = configuration.getString("operatorName", "");

		Object function = null;
		try {
			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(operatorBytes));
			function = in.readObject();

			if (operatorName.equals("coMap")) {
				setSerializer(function, CoMapFunction.class, 2);
				setDeserializers(function, CoMapFunction.class);
			} else {
				throw new Exception("Wrong operator name!");
			}

		} catch (Exception e) {
			throw new StreamComponentException(e);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void setDeserializers(Object function, Class<? extends AbstractFunction> clazz) {

		TupleTypeInfo<IN1> inTupleTypeInfo = (TupleTypeInfo) TypeExtractor.createTypeInfo(clazz,
				function.getClass(), 0, null, null);
		inTupleSerializer1 = new StreamRecordSerializer(inTupleTypeInfo.createSerializer());

		inTupleTypeInfo = (TupleTypeInfo) TypeExtractor.createTypeInfo(clazz, function.getClass(),
				1, null, null);
		inTupleSerializer2 = new StreamRecordSerializer(inTupleTypeInfo.createSerializer());
	}

	@Override
	public void registerInputOutput() {
		initialize();

		setSerializers();
		setCollector();

		// inputs1 = setConfigInputs();
		setConfigInputs();

		inputIter1 = createInputIterator(inputs1, inTupleSerializer1);

		// inputs2 = setConfigInputs();
		inputIter2 = createInputIterator(inputs2, inTupleSerializer2);

		setConfigOutputs(outputs);

		numberOfOutputChannels = new int[outputs.size()];
		for (int i = 0; i < numberOfOutputChannels.length; i++) {
			numberOfOutputChannels[i] = configuration.getInteger("channels_" + i, 0);
		}

		setInvokable();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void setInvokable() {
		// Default value is a CoMapInvokable
		Class<? extends CoInvokable> userFunctionClass = configuration.getClass("userfunction",
				CoMapInvokable.class, CoInvokable.class);
		userFunction = (CoInvokable<IN1, IN2, OUT>) getInvokable(userFunctionClass);
		userFunction.initialize(collector, inputIter1, inTupleSerializer1, inputIter2,
				inTupleSerializer2);
	}

	protected void setConfigInputs() throws StreamComponentException {
		int numberOfInputs = configuration.getInteger("numberOfInputs", 0);

		ArrayList<MutableRecordReader<IOReadableWritable>> inputList1 = new ArrayList<MutableRecordReader<IOReadableWritable>>();
		ArrayList<MutableRecordReader<IOReadableWritable>> inputList2 = new ArrayList<MutableRecordReader<IOReadableWritable>>();

		for (int i = 0; i < numberOfInputs; i++) {
			int inputType = configuration.getInteger("inputType_" + i, 0);
			switch (inputType) {
			case 1:
				inputList1.add(new MutableRecordReader<IOReadableWritable>(this));
				break;
			case 2:
				inputList2.add(new MutableRecordReader<IOReadableWritable>(this));
				break;
			default:
				throw new RuntimeException("Invalid input type number: " + inputType);
			}
		}

		inputs1 = getInputs(inputList1);
		inputs2 = getInputs(inputList2);
	}

	@SuppressWarnings("unchecked")
	private MutableReader<IOReadableWritable> getInputs(
			ArrayList<MutableRecordReader<IOReadableWritable>> inputList) {
		if (inputList.size() == 1) {
			return inputList.get(0);
		} else if (inputList.size() > 1) {
			return new MutableUnionRecordReader<IOReadableWritable>(
					(MutableRecordReader<IOReadableWritable>[]) inputList.toArray());
		}
		return null;
	}

	@Override
	public void invoke() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("TASK " + name + " invoked with instance id " + instanceID);
		}

		for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputs) {
			output.initializeSerializers();
		}

		userFunction.invoke();

		if (LOG.isDebugEnabled()) {
			LOG.debug("TASK " + name + " invoke finished with instance id " + instanceID);
		}

		for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputs) {
			output.flush();
		}
	}

}
