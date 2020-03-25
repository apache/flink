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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.MultipleConnectedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests for {@link MultipleInputStreamOperator}.
 */
@SuppressWarnings("serial")
public class MultipleInputITCase extends AbstractTestBase {
	@Test
	public void test() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		TestListResultSink<Long> resultSink = new TestListResultSink<>();

		DataStream<Integer> source1 = env.fromElements(1, 10);
		DataStream<Long> source2 = env.fromElements(2L, 11L);
		DataStream<String> source3 = env.fromElements("42", "44");

		MultipleInputTransformation<Long> transform = new MultipleInputTransformation<>(
			"My Operator",
			new SumAllInputOperatorFactory(),
			BasicTypeInfo.LONG_TYPE_INFO,
			1);

		transform.addInput(source1.getTransformation());
		transform.addInput(source2.getTransformation());
		transform.addInput(source3.getTransformation());

		env.addOperator(transform);

		new MultipleConnectedStreams(env)
			.transform(transform)
			.addSink(resultSink);

		env.execute();

		List<Long> result = resultSink.getResult();
		Collections.sort(result);
		long actualSum = result.get(result.size() - 1);
		assertEquals(1 + 10 + 2 + 11 + 42 + 44, actualSum);
	}

	/**
	 * 3 input operator that sums all of it inputs.
	 */
	public static class SumAllInputOperator extends AbstractStreamOperatorV2<Long> implements MultipleInputStreamOperator<Long> {
		private long sum;

		public SumAllInputOperator(StreamOperatorParameters<Long> parameters) {
			super(parameters, 3);
		}

		@Override
		public List<Input> getInputs() {
			return Arrays.asList(
				new SumInput<Integer>(this, 1),
				new SumInput<Long>(this, 2),
				new SumInput<String>(this, 3));
		}

		/**
		 * Summing input for {@link SumAllInputOperator}.
		 */
		public class SumInput<T> extends AbstractInput<T, Long> {
			public SumInput(AbstractStreamOperatorV2<Long> owner, int inputId) {
				super(owner, inputId);
			}

			@Override
			public void processElement(StreamRecord<T> element) throws Exception {
				sum += Long.valueOf(element.getValue().toString());
				output.collect(new StreamRecord<>(sum));
			}
		}
	}

	/**
	 * Factory for {@link SumAllInputOperator}.
	 */
	public static class SumAllInputOperatorFactory extends AbstractStreamOperatorFactory<Long> {
		@Override
		public <T extends StreamOperator<Long>> T createStreamOperator(StreamOperatorParameters<Long> parameters) {
			return (T) new SumAllInputOperator(parameters);
		}

		@Override
		public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
			return SumAllInputOperator.class;
		}
	}
}
