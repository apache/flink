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

import java.util.ArrayList;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.runtime.io.network.api.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.MutableUnionRecordReader;
import org.apache.flink.streaming.api.function.co.CoMapFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.types.TypeInformation;
import org.apache.flink.util.MutableObjectIterator;

public class CoStreamTask<IN1 extends Tuple, IN2 extends Tuple, OUT extends Tuple> extends
		AbstractStreamComponent<OUT> {

	protected StreamRecordSerializer<IN1> inputDeserializer1 = null;
	protected StreamRecordSerializer<IN2> inputDeserializer2 = null;

	private MutableReader<IOReadableWritable> inputs1;
	private MutableReader<IOReadableWritable> inputs2;
	MutableObjectIterator<StreamRecord<IN1>> inputIter1;
	MutableObjectIterator<StreamRecord<IN2>> inputIter2;

	private CoInvokable<IN1, IN2, OUT> userInvokable;
	private static int numTasks;

	public CoStreamTask() {
		outputHandler = new OutputHandler();
		userInvokable = null;
		numTasks = newComponent();
		instanceID = numTasks;
	}

	@Override
	protected void setSerializers() {
		String operatorName = configuration.getFunctionName();

		Object function = configuration.getFunction();
		try {
			if (operatorName.equals("coMap")) {
				setSerializer();
				setDeserializers(function, CoMapFunction.class);
			} else {
				throw new Exception("Wrong operator name!");
			}
		} catch (Exception e) {
			throw new StreamComponentException(e);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void setDeserializers(Object function, Class<? extends Function> clazz) {
		TypeInformation<IN1> inputTypeInfo1 = (TypeInformation<IN1>) typeWrapper
				.getInputTypeInfo1();
		inputDeserializer1 = new StreamRecordSerializer<IN1>(inputTypeInfo1);

		TypeInformation<IN2> inputTypeInfo2 = (TypeInformation<IN2>) typeWrapper
				.getInputTypeInfo2();
		inputDeserializer2 = new StreamRecordSerializer(inputTypeInfo2);
	}

	@Override
	public void setInputsOutputs() {
		outputHandler.setConfigOutputs();
		setConfigInputs();

		inputIter1 = createInputIterator(inputs1, inputDeserializer1);
		inputIter2 = createInputIterator(inputs2, inputDeserializer2);
	}

	@Override
	protected void setInvokable() {
		userInvokable = getInvokable();
		userInvokable.initialize(collector, inputIter1, inputDeserializer1, inputIter2,
				inputDeserializer2, isMutable);
	}

	protected void setConfigInputs() throws StreamComponentException {
		int numberOfInputs = configuration.getNumberOfInputs();

		ArrayList<MutableRecordReader<IOReadableWritable>> inputList1 = new ArrayList<MutableRecordReader<IOReadableWritable>>();
		ArrayList<MutableRecordReader<IOReadableWritable>> inputList2 = new ArrayList<MutableRecordReader<IOReadableWritable>>();

		for (int i = 0; i < numberOfInputs; i++) {
			int inputType = configuration.getInputType(i);
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
			MutableRecordReader<IOReadableWritable>[] inputArray = inputList
					.toArray(new MutableRecordReader[inputList.size()]);

			return new MutableUnionRecordReader<IOReadableWritable>(inputArray);
		}
		return null;
	}

	@Override
	public void invoke() throws Exception {
		outputHandler.invokeUserFunction("CO-TASK", userInvokable);
	}

}
