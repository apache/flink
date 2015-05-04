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
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.io.IndexedReaderIterator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public class MockContext<IN, OUT> {
	private Collection<IN> inputs;
	private List<OUT> outputs;

	private MockOutput<OUT> output;
	private StreamRecordSerializer<IN> inDeserializer;
	private IndexedReaderIterator<StreamRecord<IN>> iterator;

	public MockContext(Collection<IN> inputs) {
		this.inputs = inputs;
		if (inputs.isEmpty()) {
			throw new RuntimeException("Inputs must not be empty");
		}

		TypeInformation<IN> inTypeInfo = TypeExtractor.getForObject(inputs.iterator().next());
		inDeserializer = new StreamRecordSerializer<IN>(inTypeInfo, new ExecutionConfig());

		iterator = new IndexedInputIterator();
		outputs = new ArrayList<OUT>();
		output = new MockOutput<OUT>(outputs);
	}

	private class IndexedInputIterator extends IndexedReaderIterator<StreamRecord<IN>> {
		Iterator<IN> listIterator;

		public IndexedInputIterator() {
			super(null, null);
			listIterator = inputs.iterator();
		}

		@Override
		public StreamRecord<IN> next(StreamRecord<IN> reuse) throws IOException {
			if (listIterator.hasNext()) {
				reuse.setObject(listIterator.next());
			} else {
				reuse = null;
			}
			return reuse;
		}

		@Override
		public StreamRecord<IN> next() throws IOException {
			if (listIterator.hasNext()) {
				StreamRecord<IN> result = inDeserializer.createInstance();
				result.setObject(listIterator.next());
				return result;
			} else {
				return null;
			}
		}
	}

	public List<OUT> getOutputs() {
		return outputs;
	}

	public Collector<OUT> getOutput() {
		return output;
	}

	public MutableObjectIterator<StreamRecord<IN>> getIterator() {
		return iterator;
	}

	public static <IN, OUT> List<OUT> createAndExecute(OneInputStreamOperator<IN, OUT> operator,
			List<IN> inputs) {
		MockContext<IN, OUT> mockContext = new MockContext<IN, OUT>(inputs);
		RuntimeContext runtimeContext =  new StreamingRuntimeContext("MockTask", new MockEnvironment(3 * 1024 * 1024, new MockInputSplitProvider(), 1024), null,
				new ExecutionConfig());

		operator.setup(mockContext.output, runtimeContext);
		try {
			operator.open(null);

			StreamRecord<IN> nextRecord;
			while ((nextRecord = mockContext.getIterator().next()) != null) {
				operator.processElement(nextRecord.getObject());
			}

			operator.close();
		} catch (Exception e) {
			throw new RuntimeException("Cannot invoke operator.", e);
		}

		return mockContext.getOutputs();
	}
}
