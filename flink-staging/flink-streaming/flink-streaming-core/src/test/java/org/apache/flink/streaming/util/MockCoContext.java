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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.io.CoReaderIterator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

public class MockCoContext<IN1, IN2, OUT> {
	// private Collection<IN1> input1;
	// private Collection<IN2> input2;
	private Iterator<IN1> inputIterator1;
	private Iterator<IN2> inputIterator2;
	private List<OUT> outputs;

	private Output<OUT> collector;
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
		inDeserializer1 = new StreamRecordSerializer<IN1>(inTypeInfo1, new ExecutionConfig());
		TypeInformation<IN2> inTypeInfo2 = TypeExtractor.getForObject(input2.iterator().next());
		inDeserializer2 = new StreamRecordSerializer<IN2>(inTypeInfo2, new ExecutionConfig());

		mockIterator = new MockCoReaderIterator(inDeserializer1, inDeserializer2);

		outputs = new ArrayList<OUT>();
		collector = new MockOutput<OUT>(outputs);
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

	public Output<OUT> getCollector() {
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

	public static <IN1, IN2, OUT> List<OUT> createAndExecute(TwoInputStreamOperator<IN1, IN2, OUT> operator,
			List<IN1> input1, List<IN2> input2) {
		MockCoContext<IN1, IN2, OUT> mockContext = new MockCoContext<IN1, IN2, OUT>(input1, input2);
		RuntimeContext runtimeContext =  new StreamingRuntimeContext("CoMockTask", new MockEnvironment(3 * 1024 * 1024, new MockInputSplitProvider(), 1024), null,
				new ExecutionConfig());

		operator.setup(mockContext.collector, runtimeContext);

		try {
			operator.open(null);

			StreamRecordSerializer<IN1> inputDeserializer1 = mockContext.getInDeserializer1();
			StreamRecordSerializer<IN2> inputDeserializer2 = mockContext.getInDeserializer2();
			CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>> coIter = mockContext.mockIterator;

			boolean isRunning = true;

			int next;
			StreamRecord<IN1> reuse1 = inputDeserializer1.createInstance();
			StreamRecord<IN2> reuse2 = inputDeserializer2.createInstance();

			while (isRunning) {
				try {
					next = coIter.next(reuse1, reuse2);
				} catch (IOException e) {
					if (isRunning) {
						throw new RuntimeException("Could not read next record.", e);
					} else {
						// Task already cancelled do nothing
						next = 0;
					}
				} catch (IllegalStateException e) {
					if (isRunning) {
						throw new RuntimeException("Could not read next record.", e);
					} else {
						// Task already cancelled do nothing
						next = 0;
					}
				}

				if (next == 0) {
					break;
				} else if (next == 1) {
					operator.processElement1(reuse1.getObject());
					reuse1 = inputDeserializer1.createInstance();
				} else {
					operator.processElement2(reuse2.getObject());
					reuse2 = inputDeserializer2.createInstance();
				}
			}

			operator.close();
		} catch (Exception e) {
			throw new RuntimeException("Cannot invoke operator.", e);
		}

		return mockContext.getOutputs();
	}
}
