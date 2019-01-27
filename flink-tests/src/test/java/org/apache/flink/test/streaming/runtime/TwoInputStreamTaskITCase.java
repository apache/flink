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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@code TwoInputStreamTask}.
 */
public class TwoInputStreamTaskITCase extends AbstractTestBase {

	@Test
	public void testSelectedReading() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<String> source0 = env.addSource(new TestSource(0));
		DataStream<String> source1 = env.addSource(new TestSource(1)).setParallelism(2);
		TestListResultSink<String> resultSink = new TestListResultSink<String>();

		source0
			.connect(source1)
			.transform(
				"Custom Operator",
				BasicTypeInfo.STRING_TYPE_INFO,
				new SelectedReadingOperator(2, 2, ChainingStrategy.NEVER)
			).addSink(resultSink);

		env.execute("Selected-Reading test");

		List<String> result = resultSink.getResult();
		assertEquals(11, result.size());

		List<String> expected1 = Arrays.asList(
			"Hello-1-[source0-0]-[operator2-1]",
			"Hello-2-[source0-0]-[operator2-1]",
			"Hello-3-[source0-0]-[operator2-1]",
			"Ciao-[operator2-1]"
			);

		List<String> expected2 = Arrays.asList(
			"Hello-1-[source1-0]-[operator2-2]",
			"Hello-2-[source1-0]-[operator2-2]",
			"Hello-3-[source1-0]-[operator2-2]",
			"Hello-1-[source1-1]-[operator2-2]",
			"Hello-2-[source1-1]-[operator2-2]",
			"Hello-3-[source1-1]-[operator2-2]"
		);
		Collections.sort(expected2);

		assertEquals(
			expected1,
			Arrays.asList(
				result.get(0),
				result.get(1),
				result.get(4),
				result.get(5)));

		List<String> result2 = Arrays.asList(
			result.get(2),
			result.get(3),
			result.get(6),
			result.get(7),
			result.get(8),
			result.get(9));
		Collections.sort(result2);
		assertEquals(expected2, result2);

		assertEquals("Ciao-[operator2-2]", result.get(10));
	}

	private static class SelectedReadingOperator
		extends AbstractStreamOperator<String>
		implements TwoInputStreamOperator<String, String, String> {

		private static final long serialVersionUID = 1L;

		private final int index;
		private final String name;
		private final int maxContinuousReadingNum;

		private transient boolean isInputEnd1;
		private transient boolean isInputEnd2;

		private transient int currentInputReadingCount;

		public SelectedReadingOperator(int index, int maxContinuousReadingNum, ChainingStrategy chainingStrategy) {
			this.index = index;
			this.name = "operator" + index;
			this.maxContinuousReadingNum = maxContinuousReadingNum;

			this.setChainingStrategy(chainingStrategy);

			this.currentInputReadingCount = 0;
		}

		@Override
		public TwoInputSelection firstInputSelection() {
			return TwoInputSelection.FIRST;
		}

		@Override
		public TwoInputSelection processElement1(StreamRecord<String> element) throws Exception {
			output.collect(element.replace(element.getValue() + "-[" + name + "-1]"));

			currentInputReadingCount++;
			if (currentInputReadingCount == maxContinuousReadingNum) {
				currentInputReadingCount = 0;
				return !isInputEnd2 ? TwoInputSelection.SECOND : TwoInputSelection.FIRST;
			} else {
				return TwoInputSelection.FIRST;
			}
		}

		@Override
		public TwoInputSelection processElement2(StreamRecord<String> element) throws Exception {
			output.collect(element.replace(element.getValue() + "-[" + name + "-2]"));

			currentInputReadingCount++;
			if (currentInputReadingCount == maxContinuousReadingNum) {
				currentInputReadingCount = 0;
				return !isInputEnd1 ? TwoInputSelection.FIRST : TwoInputSelection.SECOND;
			} else {
				return TwoInputSelection.SECOND;
			}
		}

		@Override
		public void endInput1() throws Exception {
			output.collect(new StreamRecord<>("Ciao-[" + name + "-1]"));
			isInputEnd1 = true;
		}

		@Override
		public void endInput2() throws Exception {
			output.collect(new StreamRecord<>("Ciao-[" + name + "-2]"));
			isInputEnd2 = true;
		}
	}

	private static class TestSource extends RichParallelSourceFunction<String> {
		private static final long serialVersionUID = 1L;

		private final int index;
		private final String name;

		private volatile boolean running = true;
		private transient RuntimeContext context;

		private static String[] elements = new String[] {
			"Hello-1", "Hello-2", "Hello-3"
		};

		public TestSource(int index) {
			this.index = index;
			this.name = "source" + index;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.context = getRuntimeContext();
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			int elementIndex = 0;
			while (running) {
				if (elementIndex < elements.length) {
					synchronized (ctx.getCheckpointLock()) {
						String value = elements[elementIndex] + "-[" + name + "-" + context.getIndexOfThisSubtask() + "]";

						ctx.collect(value);
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
