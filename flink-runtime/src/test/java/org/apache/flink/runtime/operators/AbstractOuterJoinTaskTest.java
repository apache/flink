/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.RuntimePairComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.operators.testutils.BinaryOperatorTestBase;
import org.apache.flink.runtime.operators.testutils.DelayingIterator;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.InfiniteIntTupleIterator;
import org.apache.flink.runtime.operators.testutils.UniformIntTupleGenerator;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava18.com.google.common.base.Throwables;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public abstract class AbstractOuterJoinTaskTest extends BinaryOperatorTestBase<FlatJoinFunction<Tuple2<Integer, Integer>,
		Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
	
	private static final long HASH_MEM = 6 * 1024 * 1024;
	
	private static final long SORT_MEM = 3 * 1024 * 1024;
	
	private static final int NUM_SORTER = 2;
	
	private static final long BNLJN_MEM = 10 * PAGE_SIZE;
	
	private final double bnljn_frac;
	
	@SuppressWarnings("unchecked")
	protected final TypeComparator<Tuple2<Integer, Integer>> comparator1 = new TupleComparator<>(
			new int[]{0},
			new TypeComparator<?>[]{new IntComparator(true)},
			new TypeSerializer<?>[]{IntSerializer.INSTANCE}
	);
	
	@SuppressWarnings("unchecked")
	protected final TypeComparator<Tuple2<Integer, Integer>> comparator2 = new TupleComparator<>(
			new int[]{0},
			new TypeComparator<?>[]{new IntComparator(true)},
			new TypeSerializer<?>[]{IntSerializer.INSTANCE}
	);
	
	protected final List<Tuple2<Integer, Integer>> outList = new ArrayList<>();
	
	@SuppressWarnings("unchecked")
	protected final TypeSerializer<Tuple2<Integer, Integer>> serializer = new TupleSerializer<>(
			(Class<Tuple2<Integer, Integer>>) (Class<?>) Tuple2.class,
			new TypeSerializer<?>[]{IntSerializer.INSTANCE, IntSerializer.INSTANCE}
	);
	
	
	public AbstractOuterJoinTaskTest(ExecutionConfig config) {
		super(config, HASH_MEM, NUM_SORTER, SORT_MEM);
		bnljn_frac = (double) BNLJN_MEM / this.getMemoryManager().getMemorySize();
	}
	
	@Test
	public void testSortBoth1OuterJoinTask() throws Exception {
		final int keyCnt1 = 20;
		final int valCnt1 = 1;
		
		final int keyCnt2 = 10;
		final int valCnt2 = 2;
		
		testSortBothOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}
	
	@Test
	public void testSortBoth2OuterJoinTask() throws Exception {
		final int keyCnt1 = 20;
		final int valCnt1 = 1;
		
		final int keyCnt2 = 20;
		final int valCnt2 = 1;
		
		testSortBothOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}
	
	@Test
	public void testSortBoth3OuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 1;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		testSortBothOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}
	
	@Test
	public void testSortBoth4OuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 1;
		
		testSortBothOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}
	
	@Test
	public void testSortBoth5OuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		testSortBothOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}
	
	@Test
	public void testSortBoth6OuterJoinTask() throws Exception {
		int keyCnt1 = 10;
		int valCnt1 = 1;
		
		int keyCnt2 = 20;
		int valCnt2 = 2;
		
		testSortBothOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}
	
	private void testSortBothOuterJoinTask(int keyCnt1, int valCnt1, int keyCnt2, int valCnt2) throws Exception {
		setOutput(this.outList, this.serializer);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(this.getSortDriverStrategy());
		getTaskConfig().setRelativeMemoryDriver(this.bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();
		
		addInputSorted(new UniformIntTupleGenerator(keyCnt1, valCnt1, false), this.serializer, this.comparator1.duplicate());
		addInputSorted(new UniformIntTupleGenerator(keyCnt2, valCnt2, false), this.serializer, this.comparator2.duplicate());
		testDriver(testTask, MockJoinStub.class);
		
		final int expCnt = calculateExpectedCount(keyCnt1, valCnt1, keyCnt2, valCnt2);
		
		Assert.assertTrue("Result set size was " + this.outList.size() + ". Expected was " + expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
	}
	
	@Test
	public void testSortFirstOuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		setOutput(this.outList, this.serializer);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(this.getSortDriverStrategy());
		getTaskConfig().setRelativeMemoryDriver(this.bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();
		
		addInputSorted(new UniformIntTupleGenerator(keyCnt1, valCnt1, false), this.serializer, this.comparator1.duplicate());
		addInput(new UniformIntTupleGenerator(keyCnt2, valCnt2, true), this.serializer);
		testDriver(testTask, MockJoinStub.class);
		
		final int expCnt = calculateExpectedCount(keyCnt1, valCnt1, keyCnt2, valCnt2);
		
		Assert.assertTrue("Result set size was " + this.outList.size() + ". Expected was " + expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
	}
	
	@Test
	public void testSortSecondOuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		setOutput(this.outList, this.serializer);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(this.getSortDriverStrategy());
		getTaskConfig().setRelativeMemoryDriver(this.bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();
		
		addInput(new UniformIntTupleGenerator(keyCnt1, valCnt1, true), this.serializer);
		addInputSorted(new UniformIntTupleGenerator(keyCnt2, valCnt2, false), this.serializer, this.comparator2.duplicate());
		testDriver(testTask, MockJoinStub.class);
		
		final int expCnt = calculateExpectedCount(keyCnt1, valCnt1, keyCnt2, valCnt2);
		
		Assert.assertTrue("Result set size was " + this.outList.size() + ". Expected was " + expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
	}
	
	@Test
	public void testMergeOuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		setOutput(this.outList, this.serializer);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(this.getSortDriverStrategy());
		getTaskConfig().setRelativeMemoryDriver(this.bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();
		
		addInput(new UniformIntTupleGenerator(keyCnt1, valCnt1, true), this.serializer);
		addInput(new UniformIntTupleGenerator(keyCnt2, valCnt2, true), this.serializer);
		
		testDriver(testTask, MockJoinStub.class);
		
		final int expCnt = calculateExpectedCount(keyCnt1, valCnt1, keyCnt2, valCnt2);
		
		Assert.assertTrue("Result set size was " + this.outList.size() + ". Expected was " + expCnt, this.outList.size() == expCnt);
		
		this.outList.clear();
	}

	@Test(expected = ExpectedTestException.class)
	public void testFailingOuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 20;
		
		int keyCnt2 = 20;
		int valCnt2 = 20;
		
		setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(this.getSortDriverStrategy());
		getTaskConfig().setRelativeMemoryDriver(this.bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();
		
		addInput(new UniformIntTupleGenerator(keyCnt1, valCnt1, true), this.serializer);
		addInput(new UniformIntTupleGenerator(keyCnt2, valCnt2, true), this.serializer);
		
		testDriver(testTask, MockFailingJoinStub.class);
	}
	
	@Test
	public void testCancelOuterJoinTaskWhileSort1() throws Exception {
		setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(this.getSortDriverStrategy());
		getTaskConfig().setRelativeMemoryDriver(this.bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();
		
		addInputSorted(new DelayingIterator<>(new InfiniteIntTupleIterator(), 100), this.serializer, this.comparator1.duplicate());
		addInput(new DelayingIterator<>(new InfiniteIntTupleIterator(), 100), this.serializer);
		
		final AtomicReference<Throwable> error = new AtomicReference<>();
		
		final Thread taskRunner = new Thread("Task runner for testCancelOuterJoinTaskWhileSort1()") {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockJoinStub.class);
				} catch (Throwable t) {
					error.set(t);
				}
			}
		};
		taskRunner.start();
		
		Thread.sleep(1000);
		
		cancel();
		taskRunner.interrupt();

		taskRunner.join(60000);
		
		assertFalse("Task thread did not finish within 60 seconds", taskRunner.isAlive());
		
		final Throwable taskError = error.get();
		if (taskError != null) {
			fail("Error in task while canceling:\n" + Throwables.getStackTraceAsString(taskError));
		}
	}
	
	@Test
	public void testCancelOuterJoinTaskWhileSort2() throws Exception {
		setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(this.getSortDriverStrategy());
		getTaskConfig().setRelativeMemoryDriver(this.bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();
		
		addInput(new DelayingIterator<>(new InfiniteIntTupleIterator(), 1), this.serializer);
		addInputSorted(new DelayingIterator<>(new InfiniteIntTupleIterator(), 1), this.serializer, this.comparator2.duplicate());
		
		final AtomicReference<Throwable> error = new AtomicReference<>();
		
		final Thread taskRunner = new Thread("Task runner for testCancelOuterJoinTaskWhileSort2()") {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockJoinStub.class);
				} catch (Throwable t) {
					error.set(t);
				}
			}
		};
		taskRunner.start();
		
		Thread.sleep(1000);
		
		cancel();
		taskRunner.interrupt();
		
		taskRunner.join(60000);
		
		assertFalse("Task thread did not finish within 60 seconds", taskRunner.isAlive());
		
		final Throwable taskError = error.get();
		if (taskError != null) {
			fail("Error in task while canceling:\n" + Throwables.getStackTraceAsString(taskError));
		}
	}
	
	@Test
	public void testCancelOuterJoinTaskWhileRunning() throws Exception {
		setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(this.getSortDriverStrategy());
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();
		
		addInput(new DelayingIterator<>(new InfiniteIntTupleIterator(), 100), this.serializer);
		addInput(new DelayingIterator<>(new InfiniteIntTupleIterator(), 100), this.serializer);
		
		final AtomicReference<Throwable> error = new AtomicReference<>();
		
		final Thread taskRunner = new Thread("Task runner for testCancelOuterJoinTaskWhileRunning()") {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockJoinStub.class);
				} catch (Throwable t) {
					error.set(t);
				}
			}
		};
		taskRunner.start();
		
		Thread.sleep(1000);
		
		cancel();
		taskRunner.interrupt();
		
		taskRunner.join(60000);
		
		assertFalse("Task thread did not finish within 60 seconds", taskRunner.isAlive());
		
		final Throwable taskError = error.get();
		if (taskError != null) {
			fail("Error in task while canceling:\n" + Throwables.getStackTraceAsString(taskError));
		}
	}
	
	protected abstract AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> getOuterJoinDriver();
	
	protected abstract int calculateExpectedCount(int keyCnt1, int valCnt1, int keyCnt2, int valCnt2);

	protected abstract DriverStrategy getSortDriverStrategy();
	
	// =================================================================================================

	@SuppressWarnings("serial")
	public static final class MockJoinStub implements FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
		
		@Override
		public void join(Tuple2<Integer, Integer> first, Tuple2<Integer, Integer> second, Collector<Tuple2<Integer, Integer>> out) throws Exception {
			out.collect(first != null ? first : second);
		}
	}

	@SuppressWarnings("serial")
	public static final class MockFailingJoinStub implements FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
		
		private int cnt = 0;
		
		@Override
		public void join(Tuple2<Integer, Integer> first, Tuple2<Integer, Integer> second, Collector<Tuple2<Integer, Integer>> out) throws Exception {
			if (++this.cnt >= 10) {
				throw new ExpectedTestException();
			}
			out.collect(first != null ? first : second);
		}
	}
}
