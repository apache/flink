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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SliceAssigner;
import org.apache.flink.streaming.api.windowing.slicing.Slice;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link SliceOperator}.
 */
@SuppressWarnings("serial")
public class SliceOperatorTest extends TestLogger {

	private static final TypeInformation<Tuple2<String, Integer>> STRING_INT_TUPLE =
		TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){});

	// For counting if close() is called the correct number of times on the SumReducer
	private static AtomicInteger closeCalled = new AtomicInteger(0);

	// late arriving event OutputTag<StreamRecord<IN>>
	private static final OutputTag<Tuple2<String, Integer>> lateOutputTag = new OutputTag<Tuple2<String, Integer>>("late-output") {};

	private static <OUT> OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, OUT> createTestHarness(OneInputStreamOperator<Tuple2<String, Integer>, OUT> operator) throws Exception {
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);
	}

	private void testEventTimeSlicing(OneInputStreamOperator<Tuple2<String, Integer>, Slice<Tuple2<String, Integer>, String, TimeWindow>> operator) throws Exception {
		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Slice<Tuple2<String, Integer>, String, TimeWindow>> testHarness =
			createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3000));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 0));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));

		testHarness.processWatermark(new Watermark(999));
		expectedOutput.add(new Watermark(999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(1999));
		expectedOutput.add(new Watermark(1999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());
		testHarness.close();

		testHarness = createTestHarness(operator);
		expectedOutput.clear();
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processWatermark(new Watermark(2999));
		expectedOutput.add(new StreamRecord<>(new Slice<>(new Tuple2<>("key1", 3), "key1", new TimeWindow(0, 3000)), 2999));
		expectedOutput.add(new StreamRecord<>(new Slice<>(new Tuple2<>("key2", 3), "key2", new TimeWindow(0, 3000)), 2999));
		expectedOutput.add(new Watermark(2999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(3999));
		expectedOutput.add(new Watermark(3999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(new Watermark(4999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(new StreamRecord<>(new Slice<>(new Tuple2<>("key2", 2), "key2", new TimeWindow(3000, 6000)), 5999));
		expectedOutput.add(new Watermark(5999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		// those don't have any effect...
		testHarness.processWatermark(new Watermark(6999));
		testHarness.processWatermark(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));
		expectedOutput.add(new Watermark(7999));

		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testEventTimeSlicingReduce() throws Exception {
		closeCalled.set(0);

		final int windowSize = 3;

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		SliceOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = new SliceOperator<>(
			new TumbleSliceAssigner(Time.of(windowSize, TimeUnit.SECONDS).toMilliseconds(), 0L),
			new TimeWindow.Serializer(),
			new TupleKeySelector(),
			BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
			stateDesc,
			EventTimeTrigger.create(),
			0,
			null /* late data output tag */);
		testEventTimeSlicing(operator);
	}

	private void testEventTimeSlicingIterable(OneInputStreamOperator<Tuple2<String, Integer>, Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow>> operator) throws Exception {
		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow>> testHarness =
			createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3000));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 0));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));

		testHarness.processWatermark(new Watermark(999));
		expectedOutput.add(new Watermark(999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(1999));
		expectedOutput.add(new Watermark(1999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());
		testHarness.close();

		testHarness = createTestHarness(operator);
		expectedOutput.clear();
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processWatermark(new Watermark(2999));
		expectedOutput.add(new StreamRecord<>(new Slice<>(Arrays.asList(
			new Tuple2<>("key1", 1), new Tuple2<>("key1", 1), new Tuple2<>("key1", 1)), "key1", new TimeWindow(0, 3000)), 2999));
		expectedOutput.add(new StreamRecord<>(new Slice<>(Arrays.asList(
			new Tuple2<>("key2", 1), new Tuple2<>("key2", 1), new Tuple2<>("key2", 1)), "key2", new TimeWindow(0, 3000)), 2999));
		expectedOutput.add(new Watermark(2999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new IterableTuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(3999));
		expectedOutput.add(new Watermark(3999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new IterableTuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(new Watermark(4999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new IterableTuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(new StreamRecord<>(new Slice<>(Arrays.asList(
			new Tuple2<>("key2", 1), new Tuple2<>("key2", 1)), "key2", new TimeWindow(3000, 6000)), 5999));
		expectedOutput.add(new Watermark(5999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new IterableTuple2ResultSortComparator());

		// those don't have any effect...
		testHarness.processWatermark(new Watermark(6999));
		testHarness.processWatermark(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));
		expectedOutput.add(new Watermark(7999));

		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new IterableTuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	public void testEventTimeSlicingIterable() throws Exception {
		closeCalled.set(0);

		final int windowSize = 3;

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
			STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		SliceOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, TimeWindow> operator = new SliceOperator<>(
			new TumbleSliceAssigner(Time.of(windowSize, TimeUnit.SECONDS).toMilliseconds(), 0L),
			new TimeWindow.Serializer(),
			new TupleKeySelector(),
			BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
			stateDesc,
			EventTimeTrigger.create(),
			0,
			null /* late data output tag */);

		testEventTimeSlicingIterable(operator);
	}

	// ------------------------------------------------------------------------
	//  UDFs
	// ------------------------------------------------------------------------

	private static class SumReducer implements ReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<String, Integer> reduce(
			Tuple2<String, Integer> value1,
			Tuple2<String, Integer> value2) throws Exception {
			return new Tuple2<>(value2.f0, value1.f1 + value2.f1);
		}
	}

	@SuppressWarnings("unchecked")
	private static class IterableTuple2ResultSortComparator implements Comparator<Object>, Serializable {
		private static RawTuple2ResultSortComparator cmp = new RawTuple2ResultSortComparator();
		@Override
		public int compare(Object o1, Object o2) {
			if (o1 instanceof Watermark || o2 instanceof Watermark) {
				return 0;
			} else {
				StreamRecord<Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow>> sr0 = (StreamRecord<Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow>>) o1;
				StreamRecord<Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow>> sr1 = (StreamRecord<Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow>>) o2;
				Tuple2<String, Integer>[] arr0 = toArray(sr0.getValue().getContent());
				Tuple2<String, Integer>[] arr1 = toArray(sr1.getValue().getContent());
				for (int i = 0; i < Math.min(arr0.length, arr1.length); i++) {
					int val = cmp.compare(arr0[i], arr1[i]);
					if (val != 0) {
						return val;
					}
				}
				if (arr0.length > arr1.length) {
					return cmp.compare(arr0[arr1.length], null);
				} else if (arr1.length > arr0.length) {
					return cmp.compare(null, arr1[arr0.length]);
				} else {
					return 0;
				}
			}
		}

		private static Tuple2<String, Integer>[] toArray(Iterable<Tuple2<String, Integer>> itr) {
			ArrayList<Tuple2<String, Integer>> ret = new ArrayList<>();
			for (Tuple2<String, Integer> t : itr) {
				ret.add(t);
			}
			ret.sort(cmp);
			return ret.toArray(new Tuple2[0]);
		}
	}

	@SuppressWarnings("unchecked")
	private static class Tuple2ResultSortComparator implements Comparator<Object>, Serializable {
		@Override
		public int compare(Object o1, Object o2) {
			if (o1 instanceof Watermark || o2 instanceof Watermark) {
				return 0;
			} else {
				StreamRecord<Slice<Tuple2<String, Integer>, String, TimeWindow>> sr0 = (StreamRecord<Slice<Tuple2<String, Integer>, String, TimeWindow>>) o1;
				StreamRecord<Slice<Tuple2<String, Integer>, String, TimeWindow>> sr1 = (StreamRecord<Slice<Tuple2<String, Integer>, String, TimeWindow>>) o2;
				if (sr0.getTimestamp() != sr1.getTimestamp()) {
					return (int) (sr0.getTimestamp() - sr1.getTimestamp());
				}
				int comp0 = sr0.getValue().getContent().f0.compareTo(sr1.getValue().getContent().f0);
				if (comp0 != 0) {
					return comp0;
				}
				int comp1 = sr0.getValue().getContent().f1.compareTo(sr1.getValue().getContent().f1);
				if (comp1 != 0) {
					return comp1;
				}
				int comp2 = sr0.getValue().getKey().compareTo(sr1.getValue().getKey());
				if (comp2 != 0) {
					return comp2;
				}
				long comp3 = sr0.getValue().getWindow().getStart() - sr1.getValue().getWindow().getStart();
				if (comp3 != 0) {
					return new Long(comp3).intValue();
				}
				long comp4 = sr0.getValue().getWindow().getEnd() - sr1.getValue().getWindow().getEnd();
				return new Long(comp4).intValue();
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static class RawTuple2ResultSortComparator implements Comparator<Object>, Serializable {
		@Override
		public int compare(Object o1, Object o2) {
			if (o1 instanceof Tuple2 && o2 instanceof Tuple2) {
				int comp = ((Tuple2<String, Integer>) o1).f0.compareTo(((Tuple2<String, Integer>) o2).f0);
				if (comp != 0) {
					return comp;
				} else {
					return ((Tuple2<String, Integer>) o1).f1.compareTo(((Tuple2<String, Integer>) o2).f1);
				}
			} else {
				return 0;
			}
		}
	}

	private static class TupleKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}

	private class TumbleSliceAssigner extends SliceAssigner<Object, TimeWindow> {

		private long size;
		private long offset;

		protected TumbleSliceAssigner(long size, long offset) {
			this.size = size;
			this.offset = offset;
		}

		@Override
		public TimeWindow assignSlice(Object element, long timestamp, WindowAssignerContext context) {
			if (timestamp > Long.MIN_VALUE) {
				// Long.MIN_VALUE is currently assigned when no timestamp is present
				long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, size);
				return new TimeWindow(start, start + size);
			} else {
				throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
					"Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
					"'DataStream.assignTimestampsAndWatermarks(...)'?");
			}
		}

		@Override
		public TypeInformation<TimeWindow> getWindowType() {
			return TypeInformation.of(TimeWindow.class);
		}

		@Override
		public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
			return EventTimeTrigger.create();
		}

		@Override
		public String toString() {
			return "TumblingSliceWindow(" + size + ")";
		}

		@Override
		public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
			return new TimeWindow.Serializer();
		}

		@Override
		public boolean isEventTime() {
			return true;
		}
	}
}
