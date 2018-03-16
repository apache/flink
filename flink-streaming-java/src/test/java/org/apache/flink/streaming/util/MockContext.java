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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Simple test context for stream operators.
 */
public class MockContext<IN, OUT> {

	private List<OUT> outputs;

	private MockOutput<OUT> output;

	public MockContext(Collection<IN> inputs) {
		if (inputs.isEmpty()) {
			throw new RuntimeException("Inputs must not be empty");
		}

		outputs = new ArrayList<OUT>();
		output = new MockOutput<OUT>(outputs);
	}

	public List<OUT> getOutputs() {
		return outputs;
	}

	public Output<StreamRecord<OUT>> getOutput() {
		return output;
	}

	public static <IN, OUT> List<OUT> createAndExecute(OneInputStreamOperator<IN, OUT> operator, List<IN> inputs) throws Exception {
		return createAndExecuteForKeyedStream(operator, inputs, null, null);
	}

	public static <IN, OUT, KEY> List<OUT> createAndExecuteForKeyedStream(
				OneInputStreamOperator<IN, OUT> operator, List<IN> inputs,
				KeySelector<IN, KEY> keySelector, TypeInformation<KEY> keyType) throws Exception {

		OneInputStreamOperatorTestHarness<IN, OUT> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keyType);

		testHarness.setup();
		testHarness.open();

		operator.open();

		for (IN in: inputs) {
			testHarness.processElement(new StreamRecord<>(in));
		}

		testHarness.close();

		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

		List<OUT> result = new ArrayList<>();

		for (Object o : output) {
			if (o instanceof StreamRecord) {
				result.add((OUT) ((StreamRecord) o).getValue());
			}
		}

		return result;
	}
}
