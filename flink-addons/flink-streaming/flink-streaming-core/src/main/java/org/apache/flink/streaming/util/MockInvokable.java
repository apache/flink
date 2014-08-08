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

package org.apache.flink.streaming.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.invokable.UserTaskInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.types.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public class MockInvokable<IN, OUT> {
	private Collection<IN> inputs;
	private List<OUT> outputs;

	private Collector<OUT> collector;
	private StreamRecordSerializer<IN> inDeserializer;
	private MutableObjectIterator<StreamRecord<IN>> iterator;

	public MockInvokable(Collection<IN> inputs) {
		this.inputs = inputs;
		if (inputs.isEmpty()) {
			throw new RuntimeException("Inputs must not be empty");
		}

		TypeInformation<IN> inTypeInfo = TypeExtractor.getForObject(inputs.iterator().next());
		inDeserializer = new StreamRecordSerializer<IN>(inTypeInfo);
		
		iterator = new MockInputIterator();
		outputs = new ArrayList<OUT>();
		collector = new MockCollector<OUT>(outputs);
	}


	private class MockInputIterator implements MutableObjectIterator<StreamRecord<IN>> {
		Iterator<IN> listIterator;
		
		public MockInputIterator() {
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
	}

	public List<OUT> getOutputs() {
		return outputs;
	}

	public Collector<OUT> getCollector() {
		return collector;
	}

	public StreamRecordSerializer<IN> getInDeserializer() {
		return inDeserializer;
	}

	public MutableObjectIterator<StreamRecord<IN>> getIterator() {
		return iterator;
	}

	public static <IN, OUT> List<OUT> createAndExecute(UserTaskInvokable<IN, OUT> invokable, List<IN> inputs) {
		MockInvokable<IN, OUT> mock = new MockInvokable<IN, OUT>(inputs);
		invokable.initialize(mock.getCollector(), mock.getIterator(), mock.getInDeserializer(), false);
		try {
			invokable.open(null);
			invokable.invoke();
		} catch (Exception e) {
			throw new RuntimeException("Cannot invoke invokable.", e);
		}
		
		return mock.getOutputs();
	}
	
}
