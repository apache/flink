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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

public class MockContext<IN, OUT> {
	private Collection<IN> inputs;
	private List<OUT> outputs;

	private MockOutput<OUT> output;

	public MockContext(Collection<IN> inputs) {
		this.inputs = inputs;
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

	public static <IN, OUT> List<OUT> createAndExecute(OneInputStreamOperator<IN, OUT> operator, List<IN> inputs) {
		MockContext<IN, OUT> mockContext = new MockContext<IN, OUT>(inputs);
		StreamingRuntimeContext runtimeContext = new StreamingRuntimeContext(
				new MockEnvironment("MockTask", 3 * 1024 * 1024, new MockInputSplitProvider(), 1024),
				new ExecutionConfig(), 
				null, null, 
				new HashMap<String, Accumulator<?, ?>>(),
				null);

		operator.setup(mockContext.output, runtimeContext);
		try {
			operator.open(null);

			StreamRecord<IN> nextRecord;
			for (IN in: inputs) {
				operator.processElement(new StreamRecord<IN>(in));
			}

			operator.close();
		} catch (Exception e) {
			throw new RuntimeException("Cannot invoke operator.", e);
		}

		return mockContext.getOutputs();
	}
}
