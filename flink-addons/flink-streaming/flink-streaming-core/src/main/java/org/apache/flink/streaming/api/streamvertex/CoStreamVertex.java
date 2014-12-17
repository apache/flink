/*
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
 */

package org.apache.flink.streaming.api.streamvertex;

import java.util.ArrayList;

import org.apache.flink.runtime.io.network.api.MutableRecordReader;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.CoReaderIterator;
import org.apache.flink.streaming.io.CoRecordReader;
import org.apache.flink.util.MutableObjectIterator;

public class CoStreamVertex<IN1, IN2, OUT> extends StreamVertex<IN1, OUT> {

	protected StreamRecordSerializer<IN1> inputDeserializer1 = null;
	protected StreamRecordSerializer<IN2> inputDeserializer2 = null;

	MutableObjectIterator<StreamRecord<IN1>> inputIter1;
	MutableObjectIterator<StreamRecord<IN2>> inputIter2;

	CoRecordReader<DeserializationDelegate<StreamRecord<IN1>>, DeserializationDelegate<StreamRecord<IN2>>> coReader;
	CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>> coIter;

	private CoInvokable<IN1, IN2, OUT> userInvokable;
	private static int numTasks;

	public CoStreamVertex() {
		userInvokable = null;
		numTasks = newVertex();
		instanceID = numTasks;
	}

	private void setDeserializers() {
		inputDeserializer1 = configuration.getTypeSerializerIn1(userClassLoader);
		inputDeserializer2 = configuration.getTypeSerializerIn2(userClassLoader);
	}

	@Override
	public void setInputsOutputs() {
		outputHandler = new OutputHandler<OUT>(this);

		setConfigInputs();

		coIter = new CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>>(coReader,
				inputDeserializer1, inputDeserializer2);
	}

	@Override
	protected void setInvokable() {
		userInvokable = configuration.getUserInvokable(userClassLoader);
		userInvokable.setup(this);
	}

	protected void setConfigInputs() throws StreamVertexException {
		setDeserializers();

		int numberOfInputs = configuration.getNumberOfInputs();

		ArrayList<MutableRecordReader<DeserializationDelegate<StreamRecord<IN1>>>> inputList1 = new ArrayList<MutableRecordReader<DeserializationDelegate<StreamRecord<IN1>>>>();
		ArrayList<MutableRecordReader<DeserializationDelegate<StreamRecord<IN2>>>> inputList2 = new ArrayList<MutableRecordReader<DeserializationDelegate<StreamRecord<IN2>>>>();

		for (int i = 0; i < numberOfInputs; i++) {
			int inputType = configuration.getInputType(i);
			switch (inputType) {
			case 1:
				inputList1.add(new MutableRecordReader<DeserializationDelegate<StreamRecord<IN1>>>(
						this));
				break;
			case 2:
				inputList2.add(new MutableRecordReader<DeserializationDelegate<StreamRecord<IN2>>>(
						this));
				break;
			default:
				throw new RuntimeException("Invalid input type number: " + inputType);
			}
		}

		coReader = new CoRecordReader<DeserializationDelegate<StreamRecord<IN1>>, DeserializationDelegate<StreamRecord<IN2>>>(
				inputList1, inputList2);
	}

	@Override
	public void invoke() throws Exception {
		outputHandler.invokeUserFunction("CO-TASK", userInvokable);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> MutableObjectIterator<X> getInput(int index) {
		switch (index) {
		case 0:
			return (MutableObjectIterator<X>) inputIter1;
		case 1:
			return (MutableObjectIterator<X>) inputIter2;
		default:
			throw new IllegalArgumentException("CoStreamVertex has only 2 inputs");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> StreamRecordSerializer<X> getInputSerializer(int index) {
		switch (index) {
		case 0:
			return (StreamRecordSerializer<X>) inputDeserializer1;
		case 1:
			return (StreamRecordSerializer<X>) inputDeserializer2;
		default:
			throw new IllegalArgumentException("CoStreamVertex has only 2 inputs");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X, Y> CoReaderIterator<X, Y> getCoReader() {
		return (CoReaderIterator<X, Y>) coIter;
	}

}
