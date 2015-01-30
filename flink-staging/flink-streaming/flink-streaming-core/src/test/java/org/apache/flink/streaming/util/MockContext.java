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
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.api.streamvertex.StreamTaskContext;
import org.apache.flink.streaming.io.CoReaderIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public class MockContext<IN, OUT> implements StreamTaskContext<OUT> {
	private Collection<IN> inputs;
	private List<OUT> outputs;

	private Collector<OUT> collector;
	private StreamRecordSerializer<IN> inDeserializer;
	private MutableObjectIterator<StreamRecord<IN>> iterator;

	public MockContext(Collection<IN> inputs) {
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

		@Override
		public StreamRecord<IN> next() throws IOException {
			if (listIterator.hasNext()) {
				StreamRecord<IN> result = new StreamRecord<IN>();
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

	public Collector<OUT> getCollector() {
		return collector;
	}

	public StreamRecordSerializer<IN> getInDeserializer() {
		return inDeserializer;
	}

	public MutableObjectIterator<StreamRecord<IN>> getIterator() {
		return iterator;
	}

	public static <IN, OUT> List<OUT> createAndExecute(StreamInvokable<IN, OUT> invokable,
			List<IN> inputs) {
		MockContext<IN, OUT> mockContext = new MockContext<IN, OUT>(inputs);
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
		if (index == 0) {
			return (MutableObjectIterator<X>) iterator;
		} else {
			throw new IllegalArgumentException("There is only 1 input");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> StreamRecordSerializer<X> getInputSerializer(int index) {
		if (index == 0) {
			return (StreamRecordSerializer<X>) inDeserializer;
		} else {
			throw new IllegalArgumentException("There is only 1 input");
		}
	}

	@Override
	public Collector<OUT> getOutputCollector() {
		return collector;
	}

	@Override
	public <X, Y> CoReaderIterator<X, Y> getCoReader() {
		throw new IllegalArgumentException("CoReader not available");
	}

}
