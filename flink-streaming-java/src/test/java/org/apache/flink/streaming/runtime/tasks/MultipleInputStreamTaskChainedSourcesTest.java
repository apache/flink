/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.streaming.api.graph.StreamConfig.InputConfig;
import org.apache.flink.streaming.api.graph.StreamConfig.SourceInputConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTaskTest.MapToStringMultipleInputOperator;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTaskTest.MapToStringMultipleInputOperatorFactory;
import org.apache.flink.util.SerializedValue;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Tests for {@link MultipleInputStreamTask} combined with {@link org.apache.flink.streaming.api.operators.SourceOperator} chaining.
 */
public class MultipleInputStreamTaskChainedSourcesTest {
	private static final List<String> LIFE_CYCLE_EVENTS = new ArrayList<>();

	@Before
	public void setUp() {
		LIFE_CYCLE_EVENTS.clear();
	}

	@Test
	public void testBasicProcessing() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness =
			new StreamTaskMailboxTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
				.modifyExecutionConfig(config -> config.enableObjectReuse())
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.addSourceInput(
					new SourceOperatorFactory<>(
						new MockSource(Boundedness.BOUNDED, 1),
						WatermarkStrategy.noWatermarks()))
				.addInput(BasicTypeInfo.DOUBLE_TYPE_INFO)
				.setupOutputForSingletonOperatorChain(new MapToStringMultipleInputOperatorFactory())
				.build()) {

			long initialTime = 0L;
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

			addSourceRecords(testHarness, 1, 42, 43);
			expectedOutput.add(new StreamRecord<>("42", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("43", TimestampAssigner.NO_TIMESTAMP));
			testHarness.processElement(new StreamRecord<>("Hello", initialTime + 1), 0);
			expectedOutput.add(new StreamRecord<>("Hello", initialTime + 1));
			testHarness.processElement(new StreamRecord<>(42.44d, initialTime + 3), 1);
			expectedOutput.add(new StreamRecord<>("42.44", initialTime + 3));

			testHarness.endInput();
			testHarness.waitForTaskCompletion();

			assertThat(testHarness.getOutput(), containsInAnyOrder(expectedOutput.toArray()));
		}
	}

	@Test
	public void testLifeCycleOrder() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness =
				new StreamTaskMailboxTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
					.modifyExecutionConfig(config -> config.enableObjectReuse())
					.addInput(BasicTypeInfo.STRING_TYPE_INFO)
					.addSourceInput(
						new SourceOperatorFactory<>(
							new LifeCycleTrackingMockSource(Boundedness.BOUNDED, 1),
							WatermarkStrategy.noWatermarks()))
					.addInput(BasicTypeInfo.DOUBLE_TYPE_INFO)
					.setupOperatorChain(new LifeCycleTrackingMapToStringMultipleInputOperatorFactory())
					.chain(new LifeCycleTrackingMap<>(), BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
					.finish()
					.build()) {

			testHarness.waitForTaskCompletion();
		}
		assertThat(
			LIFE_CYCLE_EVENTS,
			contains(
				LifeCycleTrackingMap.OPEN,
				LifeCycleTrackingMapToStringMultipleInputOperator.OPEN,
				LifeCycleTrackingMockSourceReader.START,
				LifeCycleTrackingMockSourceReader.CLOSE,
				LifeCycleTrackingMapToStringMultipleInputOperator.CLOSE,
				LifeCycleTrackingMap.CLOSE));
	}

	private static void addSourceRecords(
			StreamTaskMailboxTestHarness<String> testHarness,
			int sourceId,
			int... records) throws Exception {
		InputConfig[] inputs = testHarness.getStreamTask().getConfiguration().getInputs(testHarness.getClass().getClassLoader());
		SourceInputConfig input = (SourceInputConfig) inputs[sourceId];
		OperatorID sourceOperatorID = testHarness.getStreamTask().operatorChain.getSourceOperator(input).getOperatorID();

		// Prepare the source split and assign it to the source reader.
		MockSourceSplit split = new MockSourceSplit(0, 0, records.length);
		for (int record : records) {
			split.addRecord(record);
		}

		// Assign the split to the source reader.
		AddSplitEvent<MockSourceSplit> addSplitEvent =
			new AddSplitEvent<>(Collections.singletonList(split), new MockSourceSplitSerializer());

		testHarness.getStreamTask().dispatchOperatorEvent(
			sourceOperatorID,
			new SerializedValue<>(addSplitEvent));
	}

	private static class LifeCycleTrackingMapToStringMultipleInputOperator
			extends MapToStringMultipleInputOperator {
		public static final String OPEN = "MultipleInputOperator#open";
		public static final String CLOSE = "MultipleInputOperator#close";

		private static final long serialVersionUID = 1L;

		public LifeCycleTrackingMapToStringMultipleInputOperator(StreamOperatorParameters<String> parameters) {
			super(parameters);
		}

		@Override
		public void open() throws Exception {
			LIFE_CYCLE_EVENTS.add(OPEN);
			super.open();
		}

		@Override
		public void close() throws Exception {
			LIFE_CYCLE_EVENTS.add(CLOSE);
			super.close();
		}
	}

	private static class LifeCycleTrackingMapToStringMultipleInputOperatorFactory extends AbstractStreamOperatorFactory<String> {
		@Override
		public <T extends StreamOperator<String>> T createStreamOperator(StreamOperatorParameters<String> parameters) {
			return (T) new LifeCycleTrackingMapToStringMultipleInputOperator(parameters);
		}

		@Override
		public Class<? extends StreamOperator<String>> getStreamOperatorClass(ClassLoader classLoader) {
			return LifeCycleTrackingMapToStringMultipleInputOperator.class;
		}
	}

	private static class LifeCycleTrackingMockSource extends MockSource {
		public LifeCycleTrackingMockSource(Boundedness boundedness, int numSplits) {
			super(boundedness, numSplits);
		}

		@Override
		public SourceReader<Integer, MockSourceSplit> createReader(SourceReaderContext readerContext) {
			LifeCycleTrackingMockSourceReader sourceReader = new LifeCycleTrackingMockSourceReader();
			createdReaders.add(sourceReader);
			return sourceReader;
		}
	}

	private static class LifeCycleTrackingMockSourceReader extends MockSourceReader {
		public static final String START = "SourceReader#start";
		public static final String CLOSE = "SourceReader#close";

		@Override
		public void start() {
			LIFE_CYCLE_EVENTS.add(START);
			super.start();
		}

		@Override
		public void close() throws Exception {
			LIFE_CYCLE_EVENTS.add(CLOSE);
			super.close();
		}
	}

	private static class LifeCycleTrackingMap<T> extends AbstractStreamOperator<T> implements OneInputStreamOperator<T, T> {
		public static final String OPEN = "LifeCycleTrackingMap#open";
		public static final String CLOSE = "LifeCycleTrackingMap#close";

		@Override
		public void processElement(StreamRecord<T> element) throws Exception {
			output.collect(element);
		}

		@Override
		public void open() throws Exception {
			LIFE_CYCLE_EVENTS.add(OPEN);
			super.open();
		}

		@Override
		public void close() throws Exception {
			LIFE_CYCLE_EVENTS.add(CLOSE);
			super.close();
		}
	}
}

