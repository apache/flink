/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.operators;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.AbstractOneInputSubstituteStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractTwoInputSubstituteStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@code AbstractOneInputSubstituteStreamOperator} and {@code AbstractTwoInputSubstituteStreamOperator}.
 */
public class SubstituteStreamOperatorITCase extends AbstractTestBase {

	@Test
	public void testOneInputSubstituteStreamOperator() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<String> source = env.addSource(new TestSource());
		TestListResultSink<String> resultSink = new TestListResultSink<>();

		source.transform(
				"Custom Operator",
				BasicTypeInfo.STRING_TYPE_INFO,
				new TestOneInputSubstituteStreamOperator<>(new TestOneInputStreamOperator("testSubstitute")))
			.addSink(resultSink);

		env.execute("test substitute");

		List<String> result = resultSink.getResult();
		assertEquals(3, result.size());

		String[] expected = new String[]{
			"Hello-1-[testSubstitute]",
			"Hello-2-[testSubstitute]",
			"Hello-3-[testSubstitute]"
		};

		assertArrayEquals(expected, result.toArray(new String[0]));
	}

	@Test
	public void testTwoInputSubstituteStreamOperator() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<String> source0 = env.addSource(new TestSource());
		DataStream<String> source1 = env.addSource(new TestSource());
		TestListResultSink<String> resultSink = new TestListResultSink<>();

		source0.connect(source1)
			.transform(
				"Custom Operator",
				BasicTypeInfo.STRING_TYPE_INFO,
				new TestTwoInputSubstituteStreamOperator<>(new TestTwoInputStreamOperator("testSubstitute0", "testSubstitute1"))
			).addSink(resultSink);

		env.execute("test substitute");

		List<String> result = resultSink.getResult();
		assertEquals(6, result.size());

		String[] expected = new String[]{
			"Hello-1-[testSubstitute0]",
			"Hello-1-[testSubstitute1]",
			"Hello-2-[testSubstitute0]",
			"Hello-2-[testSubstitute1]",
			"Hello-3-[testSubstitute0]",
			"Hello-3-[testSubstitute1]"
		};

		Collections.sort(result);
		assertArrayEquals(expected, result.toArray(new String[0]));
	}

	// ------------------------------------------------------------------------
	//  Test Utilities
	// ------------------------------------------------------------------------

	private static class TestOneInputSubstituteStreamOperator<IN, OUT> implements AbstractOneInputSubstituteStreamOperator<IN, OUT> {

		private ChainingStrategy chainingStrategy = ChainingStrategy.ALWAYS;
		private final OneInputStreamOperator<IN, OUT> actualStreamOperator;

		TestOneInputSubstituteStreamOperator(OneInputStreamOperator<IN, OUT> actualStreamOperator) {
			this.actualStreamOperator = actualStreamOperator;
		}

		@Override
		public StreamOperator<OUT> getActualStreamOperator(ClassLoader cl) {
			return actualStreamOperator;
		}

		@Override
		public void setChainingStrategy(ChainingStrategy chainingStrategy) {
			this.chainingStrategy = chainingStrategy;
			this.actualStreamOperator.setChainingStrategy(chainingStrategy);
		}

		@Override
		public boolean requireState() {
			return false;
		}

		@Override
		public ChainingStrategy getChainingStrategy() {
			return chainingStrategy;
		}

		@Override
		public void endInput() throws Exception {

		}
	}

	private static class TestOneInputStreamOperator	extends AbstractStreamOperator<String>
		implements OneInputStreamOperator<String, String> {

		private static final long serialVersionUID = 1L;

		private final String suffix;

		TestOneInputStreamOperator(String suffix) {
			this.suffix = suffix;
		}

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			output.collect((element.replace(element.getValue() + "-[" + suffix + "]")));
		}

		@Override
		public void endInput() throws Exception {

		}
	}

	private static class TestTwoInputSubstituteStreamOperator<IN1, IN2, OUT> implements AbstractTwoInputSubstituteStreamOperator<IN1, IN2, OUT> {

		private ChainingStrategy chainingStrategy = ChainingStrategy.ALWAYS;
		private final TwoInputStreamOperator<IN1, IN2, OUT> actualStreamOperator;

		TestTwoInputSubstituteStreamOperator(TwoInputStreamOperator<IN1, IN2, OUT> actualStreamOperator) {
			this.actualStreamOperator = actualStreamOperator;
		}

		@Override
		public StreamOperator<OUT> getActualStreamOperator(ClassLoader cl) {
			return actualStreamOperator;
		}

		@Override
		public void setChainingStrategy(ChainingStrategy chainingStrategy) {
			this.chainingStrategy = chainingStrategy;
			this.actualStreamOperator.setChainingStrategy(chainingStrategy);
		}

		@Override
		public boolean requireState() {
			return false;
		}

		@Override
		public ChainingStrategy getChainingStrategy() {
			return chainingStrategy;
		}

		@Override
		public TwoInputSelection processElement1(StreamRecord<IN1> element) throws Exception {
			return TwoInputSelection.ANY;
		}

		@Override
		public TwoInputSelection processElement2(StreamRecord<IN2> element) throws Exception {
			return TwoInputSelection.ANY;
		}

		@Override
		public void endInput1() throws Exception {

		}

		@Override
		public void endInput2() throws Exception {

		}
	}

	private static class TestTwoInputStreamOperator	extends AbstractStreamOperator<String>
		implements TwoInputStreamOperator<String, String, String> {

		private static final long serialVersionUID = 1L;

		private final String suffix1;
		private final String suffix2;

		TestTwoInputStreamOperator(String suffix1, String suffix2) {
			this.suffix1 = suffix1;
			this.suffix2 = suffix2;
		}

		@Override
		public TwoInputSelection processElement1(StreamRecord<String> element) throws Exception {
			output.collect((element.replace(element.getValue() + "-[" + suffix1 + "]")));
			return TwoInputSelection.ANY;
		}

		@Override
		public TwoInputSelection processElement2(StreamRecord<String> element) throws Exception {
			output.collect((element.replace(element.getValue() + "-[" + suffix2 + "]")));
			return TwoInputSelection.ANY;
		}

		@Override
		public TwoInputSelection firstInputSelection() {
			return TwoInputSelection.ANY;
		}

		@Override
		public void endInput1() throws Exception {

		}

		@Override
		public void endInput2() throws Exception {

		}
	}

	private static class TestSource extends RichParallelSourceFunction<String> {
		private static final long serialVersionUID = 1L;

		private volatile boolean running = true;

		private static String[] elements = new String[] {
			"Hello-1", "Hello-2", "Hello-3"
		};

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			int elementIndex = 0;
			while (running) {
				if (elementIndex < elements.length) {
					synchronized (ctx.getCheckpointLock()) {
						ctx.collect(elements[elementIndex]);
						elementIndex++;
					}
				} else {
					break;
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
