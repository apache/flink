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
package org.apache.flink.streaming.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A utility class that facilitates the testing of custom {@link Trigger Triggers} and
 * {@link WindowAssigner WindowAssigners}.
 *
 * <p>For examples on how to use this class, see
 * {@link org.apache.flink.streaming.runtime.operators.windowing.WindowingTestHarnessTest}.
 *
 * <p>The input elements of type {@code IN} must implement the {@code equals()} method because
 * it is used to compare the expected output to the actual output.
 */
public class WindowingTestHarness<K, IN, W extends Window> {

	private final OneInputStreamOperatorTestHarness<IN, IN> testHarness;

	private final ConcurrentLinkedQueue<Object> expectedOutputs = new ConcurrentLinkedQueue<>();

	private volatile boolean isOpen = false;

	public WindowingTestHarness(WindowAssigner<? super IN, W> windowAssigner,
								TypeInformation<K> keyType,
								TypeInformation<IN> inputType,
								KeySelector<IN, K> keySelector,
								Trigger<? super IN, ? super W> trigger,
								long allowedLateness) throws Exception {

		ListStateDescriptor<IN> windowStateDesc =
				new ListStateDescriptor<>("window-contents", inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<K, IN, Iterable<IN>, IN, W> operator =
			new WindowOperator<>(
				windowAssigner,
				windowAssigner.getWindowSerializer(new ExecutionConfig()),
				keySelector,
				keyType.createSerializer(new ExecutionConfig()),
				windowStateDesc,
				new InternalIterableWindowFunction<>(new PassThroughFunction()),
				trigger,
				allowedLateness);

		testHarness = new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keyType);
	}

	/**
	 * Simulates the processing of a new incoming element.
	 */
	public void processElement(IN element, long timestamp) throws Exception {
		openOperator();
		testHarness.processElement(new StreamRecord<>(element, timestamp));
	}

	/**
	 * Simulates the processing of a new incoming watermark.
	 */
	public void processWatermark(long timestamp) throws Exception {
		openOperator();
		testHarness.processWatermark(new Watermark(timestamp));
	}

	/**
	 * Sets the current processing time to {@code timestamp}.
	 * This is useful when working on processing time.
	 */
	public void setProcessingTime(long timestamp) throws Exception {
		openOperator();
		testHarness.setProcessingTime(timestamp);
	}

	/**
	 * Gets the current output of the windowing operator, as produced by the
	 * synergies between the window assigner and the trigger. This will also
	 * contain the received watermarks.
	 */
	public ConcurrentLinkedQueue<Object> getOutput() throws Exception {
		return testHarness.getOutput();
	}

	/**
	 * Closes the testing window operator.
	 */
	public void close() throws Exception {
		if (isOpen) {
			testHarness.close();
			isOpen = false;
		}
	}

	/**
	 * Adds a watermark to the expected output.
	 *
	 * <p>The expected output should contain the elements and watermarks that we expect the output of the operator to
	 * contain, in the correct order. This will be used to check if the produced output is the expected one, and
	 * thus determine the success or failure of the test.
	 */
	public void addExpectedWatermark(long timestamp) {
		expectedOutputs.add(new Watermark(timestamp));
	}

	/**
	 * Adds an element to the expected output.
	 *
	 * <p>The expected output should contain the elements and watermarks that we expect the output of the operator to
	 * contain, in the correct order. This will be used to check if the produced output is the expected one, and
	 * thus determine the success or failure of the test.
	 */
	public void addExpectedElement(IN element, long timestamp) {
		expectedOutputs.add(new StreamRecord<>(element, timestamp));
	}

	/**
	 * Compares the current produced output with the expected one. The latter contains elements and watermarks added
	 * using the {@link #addExpectedElement(Object, long)} and {@link #addExpectedWatermark(long)} methods.
	 *
	 * <p><b>NOTE:</b> This methods uses an {@code assert()} internally, thus failing the test if the {@code expected} output
     * does not match the {@code actual} one.
	 */
	public void compareActualToExpectedOutput(String errorMessage) {
		TestHarnessUtil.assertOutputEqualsSorted(errorMessage, expectedOutputs, testHarness.getOutput(), new StreamRecordComparator());
	}

	/**
	 * Takes a snapshot of the current state of the operator. This can be used to test fault-tolerance.
	 */
	public OperatorStateHandles snapshot(long checkpointId, long timestamp) throws Exception {
		return testHarness.snapshot(checkpointId, timestamp);
	}

	/**
	 * Resumes execution from the provided {@link OperatorStateHandles}. This is used to test recovery after a failure.
	 */
	public void restore(OperatorStateHandles stateHandles) throws Exception {
		Preconditions.checkArgument(!isOpen,
			"You are trying to restore() while the operator is still open. " +
				"Please call close() first.");

		testHarness.setup();
		testHarness.initializeState(stateHandles);
		openOperator();
	}

	private void openOperator() throws Exception {
		if (!isOpen) {
			testHarness.open();
			isOpen = true;
		}
	}

	private class PassThroughFunction implements WindowFunction<IN, IN, K, W> {
		private static final long serialVersionUID = 1L;

		@Override
		public void apply(K k, W window, Iterable<IN> input, Collector<IN> out) throws Exception {
			for (IN in: input) {
				out.collect(in);
			}
		}
	}

	/**
	 * {@link Comparator} for sorting the expected and actual output by timestamp.
	 */
	@SuppressWarnings("unchecked")
	private class StreamRecordComparator implements Comparator<Object> {
		@Override
		public int compare(Object o1, Object o2) {
			if (o1 instanceof Watermark || o2 instanceof Watermark) {
				return 0;
			} else {
				StreamRecord<Tuple2<String, Integer>> sr0 = (StreamRecord<Tuple2<String, Integer>>) o1;
				StreamRecord<Tuple2<String, Integer>> sr1 = (StreamRecord<Tuple2<String, Integer>>) o2;
				if (sr0.getTimestamp() != sr1.getTimestamp()) {
					return (int) (sr0.getTimestamp() - sr1.getTimestamp());
				}
				int comparison = sr0.getValue().f0.compareTo(sr1.getValue().f0);
				if (comparison != 0) {
					return comparison;
				} else {
					return sr0.getValue().f1 - sr1.getValue().f1;
				}
			}
		}
	}
}
