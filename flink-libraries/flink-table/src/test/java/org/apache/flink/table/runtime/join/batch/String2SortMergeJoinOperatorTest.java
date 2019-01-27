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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTaskTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.codegen.GeneratedSorter;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.plan.FlinkJoinRelType;
import org.apache.flink.table.runtime.join.batch.RandomSortMergeInnerJoinTest.MyConditionFunction;
import org.apache.flink.table.runtime.join.batch.String2HashJoinOperatorTest.MyProjection;
import org.apache.flink.table.runtime.sort.InMemorySortTest;
import org.apache.flink.table.runtime.sort.NormalizedKeyComputer;
import org.apache.flink.table.runtime.sort.RecordComparator;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.codehaus.commons.compiler.CompileException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.table.runtime.join.batch.String2HashJoinOperatorTest.newRow;
import static org.apache.flink.table.runtime.join.batch.String2HashJoinOperatorTest.transformToBinary;

/**
 * Test for {@link SortMergeJoinOperator}.
 */
@RunWith(Parameterized.class)
public class String2SortMergeJoinOperatorTest {

	private boolean leftIsSmall;
	BaseRowTypeInfo typeInfo = new BaseRowTypeInfo(
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
	private BaseRowTypeInfo joinedInfo = new BaseRowTypeInfo(
			STRING_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO);

	public String2SortMergeJoinOperatorTest(boolean leftIsSmall) {
		this.leftIsSmall = leftIsSmall;
	}

	@Parameterized.Parameters
	public static Collection<Boolean> parameters() {
		return Arrays.asList(true, false);
	}

	@Test
	public void testInnerJoin() throws Exception {
		TestSortMergeJoinOperator joinOperator = new TestSortMergeJoinOperator(FlinkJoinRelType.INNER, leftIsSmall);
		TwoInputStreamTaskTestHarness<BinaryRow, BinaryRow, JoinedRow> testHarness =
				buildSortMergeJoin(joinOperator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		expectedOutput.add(new StreamRecord<>(newRow("a", "02")));
		expectedOutput.add(new StreamRecord<>(newRow("b", "14")));
		testHarness.waitForTaskCompletion();
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));
	}

	@Test
	public void testLeftOuterJoin() throws Exception {
		TestSortMergeJoinOperator joinOperator = new TestSortMergeJoinOperator(FlinkJoinRelType.LEFT, leftIsSmall);
		TwoInputStreamTaskTestHarness<BinaryRow, BinaryRow, JoinedRow> testHarness =
				buildSortMergeJoin(joinOperator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		expectedOutput.add(new StreamRecord<>(newRow("a", "02")));
		expectedOutput.add(new StreamRecord<>(newRow("b", "14")));
		expectedOutput.add(new StreamRecord<>(newRow("d", "0null")));
		testHarness.waitForTaskCompletion();
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));
	}

	@Test
	public void testRightOuterJoin() throws Exception {
		TestSortMergeJoinOperator joinOperator = new TestSortMergeJoinOperator(FlinkJoinRelType.RIGHT, leftIsSmall);
		TwoInputStreamTaskTestHarness<BinaryRow, BinaryRow, JoinedRow> testHarness =
				buildSortMergeJoin(joinOperator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		expectedOutput.add(new StreamRecord<>(newRow("a", "02")));
		expectedOutput.add(new StreamRecord<>(newRow("b", "14")));
		expectedOutput.add(new StreamRecord<>(newRow("c", "2null")));
		testHarness.waitForTaskCompletion();
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));
	}

	@Test
	public void testFullJoin() throws Exception {
		TestSortMergeJoinOperator joinOperator = new TestSortMergeJoinOperator(FlinkJoinRelType.FULL, leftIsSmall);
		TwoInputStreamTaskTestHarness<BinaryRow, BinaryRow, JoinedRow> testHarness =
				buildSortMergeJoin(joinOperator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		expectedOutput.add(new StreamRecord<>(newRow("a", "02")));
		expectedOutput.add(new StreamRecord<>(newRow("b", "14")));
		expectedOutput.add(new StreamRecord<>(newRow("c", "2null")));
		expectedOutput.add(new StreamRecord<>(newRow("d", "0null")));

		testHarness.waitForTaskCompletion();
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));
	}

	private TwoInputStreamTaskTestHarness<BinaryRow, BinaryRow, JoinedRow> buildSortMergeJoin(StreamOperator operator) throws Exception {
		final TwoInputStreamTaskTestHarness<BinaryRow, BinaryRow, JoinedRow> testHarness =
				new TwoInputStreamTaskTestHarness<>(TwoInputStreamTask::new, 2, 2,
					new int[]{1, 2}, typeInfo, (TypeInformation) typeInfo, joinedInfo);

		testHarness.memorySize = 36 * 1024 * 1024;
		testHarness.setupOutputForSingletonOperatorChain();
		testHarness.getStreamConfig().setStreamOperator(operator);
		testHarness.getStreamConfig().setOperatorID(new OperatorID());

		long initialTime = 0L;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		testHarness.processElement(new StreamRecord<>(newRow("a", "0"), initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<>(newRow("d", "0"), initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<>(newRow("a", "2"), initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<>(newRow("b", "1"), initialTime), 0, 1);
		testHarness.processElement(new StreamRecord<>(newRow("c", "2"), initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<>(newRow("b", "4"), initialTime), 1, 0);
		testHarness.endInput();
		return testHarness;
	}

	/**
	 * Override cookGeneratedClasses.
	 */
	static class TestSortMergeJoinOperator extends SortMergeJoinOperator {

		public TestSortMergeJoinOperator(FlinkJoinRelType type, boolean leftIsSmaller) {
			super(32 * 32 * 1024, 32 * 32 * 1024, 0, 1024 * 1024, type, leftIsSmaller,
					null, null, null,
					new GeneratedSorter(null, null, null, null),
					new GeneratedSorter(null, null, null, null),
					new GeneratedSorter(null, null, null, null), new boolean[]{true});
		}

		@Override
		protected CookedClasses cookGeneratedClasses(ClassLoader cl) throws CompileException {
			Class<NormalizedKeyComputer> computerClass;
			Class<RecordComparator> comparatorClass;
			try {
				Tuple2<NormalizedKeyComputer, RecordComparator> base =
						InMemorySortTest.getStringSortBase(0, true, "String2SortMergeJoinOperatorTest");
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
					computerClass,
					comparatorClass,
					comparatorClass
			);
		}
	}
}
