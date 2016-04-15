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

import com.google.common.base.Throwables;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.RuntimePairComparatorFactory;
import org.apache.flink.runtime.operators.testutils.DelayingIterator;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.InfiniteIntTupleIterator;
import org.apache.flink.runtime.operators.testutils.UniformIntTupleGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class RightOuterJoinTaskTest extends AbstractOuterJoinTaskTest {

	private static final long HASH_MEM = 6*1024*1024;

	private final double hash_frac;
	
	public RightOuterJoinTaskTest(ExecutionConfig config) {
		super(config);
		hash_frac = (double)HASH_MEM/this.getMemoryManager().getMemorySize();
	}

	@Override
	protected DriverStrategy getSortDriverStrategy() {
		return DriverStrategy.RIGHT_OUTER_MERGE;
	}
	
	@Override
	protected int calculateExpectedCount(int keyCnt1, int valCnt1, int keyCnt2, int valCnt2) {
		return valCnt1 * valCnt2 * Math.min(keyCnt1, keyCnt2) + (keyCnt2 > keyCnt1 ? (keyCnt2 - keyCnt1) * valCnt2 : 0);
	}
	
	@Override
	protected AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> getOuterJoinDriver() {
		return new RightOuterJoinDriver<>();
	}

	@Test
	public void testHash1RightOuterJoinTask() throws Exception {
		final int keyCnt1 = 20;
		final int valCnt1 = 1;

		final int keyCnt2 = 10;
		final int valCnt2 = 2;

		testHashRightOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testHash2RightOuterJoinTask() throws Exception {
		final int keyCnt1 = 20;
		final int valCnt1 = 1;

		final int keyCnt2 = 20;
		final int valCnt2 = 1;

		testHashRightOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testHash3RightOuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 1;

		int keyCnt2 = 20;
		int valCnt2 = 20;

		testHashRightOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testHash4RightOuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 20;

		int keyCnt2 = 20;
		int valCnt2 = 1;

		testHashRightOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testHash5RightOuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 20;

		int keyCnt2 = 20;
		int valCnt2 = 20;

		testHashRightOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testHash6RightOuterJoinTask() throws Exception {
		int keyCnt1 = 10;
		int valCnt1 = 1;

		int keyCnt2 = 20;
		int valCnt2 = 2;

		testHashRightOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	private void testHashRightOuterJoinTask(int keyCnt1, int valCnt1, int keyCnt2, int valCnt2) throws Exception {

		setOutput(this.outList, this.serializer);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(DriverStrategy.RIGHT_HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setRelativeMemoryDriver(hash_frac);

		setNumFileHandlesForSort(4);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();

		addInput(new UniformIntTupleGenerator(keyCnt1, valCnt1, false), this.serializer);
		addInput(new UniformIntTupleGenerator(keyCnt2, valCnt2, false), this.serializer);
		testDriver(testTask, MockJoinStub.class);

		final int expCnt = calculateExpectedCount(keyCnt1, valCnt1, keyCnt2, valCnt2);

		Assert.assertTrue("Result set size was " + this.outList.size() + ". Expected was " + expCnt, this.outList.size() == expCnt);

		this.outList.clear();
	}

	@Test(expected = ExpectedTestException.class)
	public void testFailingHashRightOuterJoinTask() throws Exception {
		int keyCnt1 = 20;
		int valCnt1 = 20;

		int keyCnt2 = 20;
		int valCnt2 = 20;

		setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(DriverStrategy.RIGHT_HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setRelativeMemoryDriver(this.hash_frac);
		setNumFileHandlesForSort(4);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();

		addInput(new UniformIntTupleGenerator(keyCnt1, valCnt1, true), this.serializer);
		addInput(new UniformIntTupleGenerator(keyCnt2, valCnt2, true), this.serializer);

		testDriver(testTask, MockFailingJoinStub.class);
	}

	@Test
	public void testCancelRightOuterJoinTaskWhileBuilding() throws Exception {
		setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(DriverStrategy.RIGHT_HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setRelativeMemoryDriver(this.hash_frac);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();

		addInput(new DelayingIterator<>(new InfiniteIntTupleIterator(), 100), this.serializer);
		addInput(new UniformIntTupleGenerator(100, 100, true), this.serializer);

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
	public void testCancelRightOuterJoinTaskWhileProbing() throws Exception {
		setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(DriverStrategy.RIGHT_HYBRIDHASH_BUILD_FIRST);
		getTaskConfig().setRelativeMemoryDriver(this.hash_frac);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();

		addInput(new UniformIntTupleGenerator(1, 1, true), this.serializer);
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
}
