/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.plan.FlinkJoinRelType;
import org.apache.flink.table.runtime.sort.InMemorySortTest;
import org.apache.flink.table.runtime.sort.NormalizedKeyComputer;
import org.apache.flink.table.runtime.sort.RecordComparator;

import org.codehaus.commons.compiler.CompileException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.flink.table.runtime.join.batch.String2HashJoinOperatorTest.newRow;

/**
 * Test for {@link OneSideSortMergeJoinOperator}.
 */
@RunWith(Parameterized.class)
public class String2OneSideSortMergeJoinOperatorTest extends String2MergeJoinOperatorTest {

	private boolean leftNeedsSort;
	private TestData[] data1;

	public String2OneSideSortMergeJoinOperatorTest(boolean leftNeedsSort) {
		this.leftNeedsSort = leftNeedsSort;

		if (leftNeedsSort) {
			data1 = new TestData[] {
				new TestData(newRow("d", "0"), 0, 0),
				new TestData(newRow("a", "0"), 0, 0),
				new TestData(newRow("a", "2"), 1, 0),
				new TestData(newRow("b", "1"), 0, 0),
				new TestData(newRow("b", "4"), 1, 0),
				new TestData(newRow("c", "2"), 1, 0)
			};
		} else {
			data1 = new TestData[] {
				new TestData(newRow("a", "0"), 0, 0),
				new TestData(newRow("a", "2"), 1, 0),
				new TestData(newRow("c", "2"), 1, 0),
				new TestData(newRow("b", "1"), 0, 0),
				new TestData(newRow("d", "0"), 0, 0),
				new TestData(newRow("b", "4"), 1, 0)
			};
		}
	}

	@Parameterized.Parameters
	public static Collection<Boolean> parameters() {
		return Arrays.asList(true, false);
	}

	@Test
	public void testInnerJoinNeedsSort() throws Exception {
		BinaryRow[] expected = new BinaryRow[] {
			newRow("a", "0", "a", "2"),
			newRow("b", "1", "b", "4")
		};

		checkResult(FlinkJoinRelType.INNER, data1, expected);
	}

	@Override
	@Test
	public void testLeftOuterJoin() throws Exception {
		// Outer join where probe needs sort not supported currently
		if (!leftNeedsSort) {
			super.testLeftOuterJoin();
		}
	}

	@Override
	@Test
	public void testLeftOuterJoinFilterNull() throws Exception {
		// Outer join where probe needs sort not supported currently
		if (!leftNeedsSort) {
			super.testLeftOuterJoinFilterNull();
		}
	}

	@Test
	public void testLeftOuterJoinNeedsSort() throws Exception {
		// Outer join where probe needs sort not supported currently
		if (!leftNeedsSort) {
			BinaryRow[] expected = new BinaryRow[] {
				newRow("a", "0", "a", "2"),
				newRow("b", "1", "b", "4"),
				newRow("d", "0", null, null)
			};

			checkResult(FlinkJoinRelType.LEFT, data1, expected);
		}
	}

	@Override
	@Test
	public void testRightOuterJoin() throws Exception {
		// Outer join where probe needs sort not supported currently
		if (leftNeedsSort) {
			super.testRightOuterJoin();
		}
	}

	@Override
	@Test
	public void testRightOuterJoinFilterNull() throws Exception {
		// Outer join where probe needs sort not supported currently
		if (leftNeedsSort) {
			super.testRightOuterJoinFilterNull();
		}
	}

	@Test
	public void testRightOuterJoinNeedsSort() throws Exception {
		// Outer join where probe needs sort not supported currently
		if (leftNeedsSort) {
			BinaryRow[] expected = new BinaryRow[] {
				newRow("a", "0", "a", "2"),
				newRow("b", "1", "b", "4"),
				newRow(null, null, "c", "2")
			};

			checkResult(FlinkJoinRelType.RIGHT, data1, expected);
		}
	}

	@Override
	@Test
	public void testFullOuterJoin() throws Exception {
		// Full outer join not supported currently
	}

	@Override
	@Test
	public void testFullOuterJoinFilterNull() throws Exception {
		// Full outer join not supported currently
	}

	@Override
	protected StreamOperator getOperator(FlinkJoinRelType type) {
		return new TestMergeJoinOperator(type, leftNeedsSort);
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
					InMemorySortTest.getIntSortBase(0, true, "String2OneSideSortMergeJoinOperatorTest");
				computerClass = (Class<NormalizedKeyComputer>) base.f0.getClass();
				comparatorClass = (Class<RecordComparator>) base.f1.getClass();
			} catch (Exception e) {
				throw new RuntimeException();
			}
			return new CookedClasses(
				(Class) RandomSortMergeInnerJoinTest.MyConditionFunction.class,
				comparatorClass,
				(Class) Int2HashJoinOperatorTest.MyProjection.class,
				(Class) Int2HashJoinOperatorTest.MyProjection.class,
				computerClass,
				comparatorClass
			);
		}
	}
}
