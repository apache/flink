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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.RuntimePairComparatorFactory;
import org.apache.flink.runtime.operators.testutils.DelayingIterator;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.InfiniteIntTupleIterator;
import org.apache.flink.runtime.operators.testutils.UniformIntTupleGenerator;

import org.apache.flink.shaded.guava18.com.google.common.base.Throwables;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class LeftOuterJoinTaskTest extends AbstractOuterJoinTaskTest {

	private static final long HASH_MEM = 6*1024*1024;

	private final double hash_frac;

	public LeftOuterJoinTaskTest(ExecutionConfig config) {
		super(config);
		hash_frac = (double)HASH_MEM/this.getMemoryManager().getMemorySize();
	}
	
	@Override
	protected int calculateExpectedCount(int keyCnt1, int valCnt1, int keyCnt2, int valCnt2) {
		return valCnt1 * valCnt2 * Math.min(keyCnt1, keyCnt2) + (keyCnt1 > keyCnt2 ? (keyCnt1 - keyCnt2) * valCnt1 : 0);
	}

	@Override
	protected DriverStrategy getSortDriverStrategy() {
		return DriverStrategy.LEFT_OUTER_MERGE;
	}

	@Override
	protected AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> getOuterJoinDriver() {
		return new LeftOuterJoinDriver<>();
	}

	@Test
	public void testHash1LeftOuterJoinTask() throws Exception {
		final int keyCnt1 = 20;
		final int valCnt1 = 1;

		final int keyCnt2 = 10;
		final int valCnt2 = 2;

		testHashLeftOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testHash2LeftOuterJoinTask() throws Exception {
		final int keyCnt1 = 20;
		final int valCnt1 = 1;

		final int keyCnt2 = 20;
		final int valCnt2 = 1;

		testHashLeftOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testHash3LeftOuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 1;

		int keyCnt2 = 20;
		int valCnt2 = 20;

		testHashLeftOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testHash4LeftOuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 20;

		int keyCnt2 = 20;
		int valCnt2 = 1;

		testHashLeftOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testHash5LeftOuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 20;

		int keyCnt2 = 20;
		int valCnt2 = 20;

		testHashLeftOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testHash6LeftOuterJoinTask() throws Exception {
		int keyCnt1 = 10;
		int valCnt1 = 1;

		int keyCnt2 = 20;
		int valCnt2 = 2;

		testHashLeftOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	private void testHashLeftOuterJoinTask(int keyCnt1, int valCnt1, int keyCnt2, int valCnt2) throws Exception {

		setOutput(this.outList, this.serializer);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(DriverStrategy.LEFT_HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setRelativeMemoryDriver(hash_frac);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();

		addInput(new UniformIntTupleGenerator(keyCnt1, valCnt1, false), this.serializer);
		addInput(new UniformIntTupleGenerator(keyCnt2, valCnt2, false), this.serializer);
		testDriver(testTask, MockJoinStub.class);

		final int expCnt = calculateExpectedCount(keyCnt1, valCnt1, keyCnt2, valCnt2);

		Assert.assertTrue("Result set size was " + this.outList.size() + ". Expected was " + expCnt, this.outList.size() == expCnt);

		this.outList.clear();
	}

	@Test(expected = ExpectedTestException.class)
	public void testFailingHashLeftOuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 20;

		int keyCnt2 = 20;
		int valCnt2 = 20;

		setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(DriverStrategy.LEFT_HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setRelativeMemoryDriver(this.hash_frac);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();

		addInput(new UniformIntTupleGenerator(keyCnt1, valCnt1, true), this.serializer);
		addInput(new UniformIntTupleGenerator(keyCnt2, valCnt2, true), this.serializer);

		testDriver(testTask, MockFailingJoinStub.class);
	}

	@Test
	public void testCancelLeftOuterJoinTaskWhileBuilding() throws Exception {
		setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(DriverStrategy.LEFT_HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setRelativeMemoryDriver(this.hash_frac);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();

		addInput(new UniformIntTupleGenerator(100, 100, true), this.serializer);
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
		taskRunner.join(60000);

		assertFalse("Task thread did not finish within 60 seconds", taskRunner.isAlive());

		final Throwable taskError = error.get();
		if (taskError != null) {
			fail("Error in task while canceling:\n" + Throwables.getStackTraceAsString(taskError));
		}
	}

	@Test
	public void testCancelLeftOuterJoinTaskWhileProbing() throws Exception {
		setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(DriverStrategy.LEFT_HYBRIDHASH_BUILD_SECOND);
		getTaskConfig().setRelativeMemoryDriver(this.hash_frac);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();

		addInput(new DelayingIterator<>(new InfiniteIntTupleIterator(), 100), this.serializer);
		addInput(new UniformIntTupleGenerator(1, 1, true), this.serializer);

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
		taskRunner.join(60000);

		assertFalse("Task thread did not finish within 60 seconds", taskRunner.isAlive());

		final Throwable taskError = error.get();
		if (taskError != null) {
			fail("Error in task while canceling:\n" + Throwables.getStackTraceAsString(taskError));
		}
	}
}
