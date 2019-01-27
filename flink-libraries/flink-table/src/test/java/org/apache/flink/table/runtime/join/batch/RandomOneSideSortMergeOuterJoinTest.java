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

package org.apache.flink.table.runtime.join.batch;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.table.codegen.GeneratedSorter;
import org.apache.flink.table.plan.FlinkJoinRelType;
import org.apache.flink.table.runtime.join.batch.Int2HashJoinOperatorTest.MyProjection;
import org.apache.flink.table.runtime.join.batch.RandomSortMergeInnerJoinTest.MyConditionFunction;
import org.apache.flink.table.runtime.sort.InMemorySortTest;
import org.apache.flink.table.runtime.sort.NormalizedKeyComputer;
import org.apache.flink.table.runtime.sort.RecordComparator;
import org.apache.flink.util.MutableObjectIterator;

import org.codehaus.commons.compiler.CompileException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.table.runtime.join.batch.RandomSortMergeInnerJoinTest.join;

/**
 * Test for one side sort merge outer join.
 */
@RunWith(Parameterized.class)
public class RandomOneSideSortMergeOuterJoinTest extends RandomSortMergeOuterJoinTest {

	private boolean leftNeedsSort;

	public RandomOneSideSortMergeOuterJoinTest(boolean leftNeedsSort) {
		this.leftNeedsSort = leftNeedsSort;
	}

	@Parameterized.Parameters
	public static Collection<Boolean> parameters() {
		return Arrays.asList(true, false);
	}

	@Override
	protected StreamOperator getOperator(FlinkJoinRelType type) {
		return new TestMergeJoinOperator(type, leftNeedsSort);
	}

	@Override
	public LinkedBlockingQueue<Object> myJoin(
		StreamOperator operator,
		MutableObjectIterator<Tuple2<Integer, String>> input1,
		MutableObjectIterator<Tuple2<Integer, String>> input2) throws Exception {
		return join(operator, input1, input2, leftNeedsSort);
	}

	@Test
	public void testFullOuterJoinWithHighNumberOfCommonKeys() {
	}

	@Test
	public void testLeftOuterJoinWithHighNumberOfCommonKeys() {
		if (!leftNeedsSort) {
			super.testLeftOuterJoinWithHighNumberOfCommonKeys();
		}
	}

	@Test
	public void testRightOuterJoinWithHighNumberOfCommonKeys() {
		if (leftNeedsSort) {
			super.testRightOuterJoinWithHighNumberOfCommonKeys();
		}
	}

	/**
	 * Override cookGeneratedClasses.
	 */
	static class TestMergeJoinOperator extends OneSideSortMergeJoinOperator {

		private TestMergeJoinOperator(FlinkJoinRelType type, boolean leftNeedsSort) {
			super(32 * 32 * 1024, 32 * 32 * 1024, 0,
				512 * 1024, 512 * 1024, type, leftNeedsSort,
				null, null, null,
				new GeneratedSorter(null, null, null, null),
				new GeneratedSorter(null, null, null, null),
				new boolean[]{true});
		}

		@Override
		protected CookedClasses cookGeneratedClasses(ClassLoader cl) throws CompileException {
			Class<NormalizedKeyComputer> computerClass;
			Class<RecordComparator> comparatorClass;
			try {
				Tuple2<NormalizedKeyComputer, RecordComparator> base =
					InMemorySortTest.getIntSortBase(0, true, "RandomOneSideSortMergeOuterJoinTest");
				computerClass = (Class<NormalizedKeyComputer>) base.f0.getClass();
				comparatorClass = (Class<RecordComparator>) base.f1.getClass();
			} catch (Exception e) {
				throw new RuntimeException();
			}
			return new CookedClasses(
				(Class) MyConditionFunction.class,
				comparatorClass,
				(Class) MyProjection.class,
				(Class) MyProjection.class,
				computerClass,
				comparatorClass
			);
		}
	}
}
