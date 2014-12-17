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

package org.apache.flink.streaming.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.api.streamvertex.StreamTaskContext;
import org.apache.flink.streaming.io.CoReaderIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public class MockCoContext<IN1, IN2, OUT> implements StreamTaskContext<OUT> {
	// private Collection<IN1> input1;
	// private Collection<IN2> input2;
	private Iterator<IN1> inputIterator1;
	private Iterator<IN2> inputIterator2;
	private List<OUT> outputs;

	private Collector<OUT> collector;
	private StreamRecordSerializer<IN1> inDeserializer1;
	private CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>> mockIterator;
	private StreamRecordSerializer<IN2> inDeserializer2;

	public MockCoContext(Collection<IN1> input1, Collection<IN2> input2) {

		if (input1.isEmpty() || input2.isEmpty()) {
			throw new RuntimeException("Inputs must not be empty");
		}

		this.inputIterator1 = input1.iterator();
		this.inputIterator2 = input2.iterator();

		TypeInformation<IN1> inTypeInfo1 = TypeExtractor.getForObject(input1.iterator().next());
		inDeserializer1 = new StreamRecordSerializer<IN1>(inTypeInfo1);
		TypeInformation<IN2> inTypeInfo2 = TypeExtractor.getForObject(input2.iterator().next());
		inDeserializer2 = new StreamRecordSerializer<IN2>(inTypeInfo2);

		mockIterator = new MockCoReaderIterator(inDeserializer1, inDeserializer2);

		outputs = new ArrayList<OUT>();
		collector = new MockCollector<OUT>(outputs);
	}

	private int currentInput = 1;
	private StreamRecord<IN1> reuse1;
	private StreamRecord<IN2> reuse2;

	private class MockCoReaderIterator extends
			CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>> {

		public MockCoReaderIterator(TypeSerializer<StreamRecord<IN1>> serializer1,
				TypeSerializer<StreamRecord<IN2>> serializer2) {
			super(null, serializer1, serializer2);
			reuse1 = inDeserializer1.createInstance();
			reuse2 = inDeserializer2.createInstance();
		}

		@Override
		public int next(StreamRecord<IN1> target1, StreamRecord<IN2> target2) throws IOException {
			this.delegate1.setInstance(target1);
			this.delegate2.setInstance(target2);

			int inputNumber = nextRecord();
			target1.setObject(reuse1.getObject());
			target2.setObject(reuse2.getObject());

			return inputNumber;
		}
	}

	private Integer nextRecord() {
		if (inputIterator1.hasNext() && inputIterator2.hasNext()) {
			switch (currentInput) {
			case 1:
				return next1();
			case 2:
				return next2();
			default:
				return 0;
			}
		}

		if (inputIterator1.hasNext()) {
			return next1();
		}

		if (inputIterator2.hasNext()) {
			return next2();
		}

		return 0;
	}

	private int next1() {
		reuse1 = inDeserializer1.createInstance();
		reuse1.setObject(inputIterator1.next());
		currentInput = 2;
		return 1;
	}

	private int next2() {
		reuse2 = inDeserializer2.createInstance();
		reuse2.setObject(inputIterator2.next());
		currentInput = 1;
		return 2;
	}

	public List<OUT> getOutputs() {
		return outputs;
	}

	public Collector<OUT> getCollector() {
		return collector;
	}

	public StreamRecordSerializer<IN1> getInDeserializer1() {
		return inDeserializer1;
	}

	public StreamRecordSerializer<IN2> getInDeserializer2() {
		return inDeserializer2;
	}

	public CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>> getIterator() {
		return mockIterator;
	}

	public static <IN1, IN2, OUT> List<OUT> createAndExecute(CoInvokable<IN1, IN2, OUT> invokable,
			List<IN1> input1, List<IN2> input2) {
		MockCoContext<IN1, IN2, OUT> mockContext = new MockCoContext<IN1, IN2, OUT>(input1, input2);
		invokable.setup(mockContext);

		try {
			invokable.open(null);
			invokable.invoke();
			invokable.close();
		} catch (Exception e) {
			throw new RuntimeException("Cannot invoke invokable.", e);
		}

		return mockContext.getOutputs();
	}

	@Override
	public StreamConfig getConfig() {
		return null;
	}

	@Override
	public ClassLoader getUserCodeClassLoader() {
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> MutableObjectIterator<X> getInput(int index) {
		switch (index) {
		case 0:
			return (MutableObjectIterator<X>) inputIterator1;
		case 1:
			return (MutableObjectIterator<X>) inputIterator2;
		default:
			throw new IllegalArgumentException("CoStreamVertex has only 2 inputs");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> StreamRecordSerializer<X> getInputSerializer(int index) {
		switch (index) {
		case 0:
			return (StreamRecordSerializer<X>) inDeserializer1;
		case 1:
			return (StreamRecordSerializer<X>) inDeserializer2;
		default:
			throw new IllegalArgumentException("CoStreamVertex has only 2 inputs");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X, Y> CoReaderIterator<X, Y> getCoReader() {
		return (CoReaderIterator<X, Y>) mockIterator;
	}

	@Override
	public Collector<OUT> getOutputCollector() {
		return collector;
	}

}
