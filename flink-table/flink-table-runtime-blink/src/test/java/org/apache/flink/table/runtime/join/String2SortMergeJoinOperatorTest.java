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

package org.apache.flink.table.runtime.join;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTaskTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.generated.GeneratedJoinCondition;
import org.apache.flink.table.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.generated.GeneratedProjection;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.JoinCondition;
import org.apache.flink.table.generated.NormalizedKeyComputer;
import org.apache.flink.table.generated.Projection;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.runtime.join.String2HashJoinOperatorTest.MyProjection;
import org.apache.flink.table.runtime.sort.StringNormalizedKeyComputer;
import org.apache.flink.table.runtime.sort.StringRecordComparator;
import org.apache.flink.table.type.InternalTypes;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.runtime.join.String2HashJoinOperatorTest.newRow;
import static org.apache.flink.table.runtime.join.String2HashJoinOperatorTest.transformToBinary;

/**
 * Test for {@link SortMergeJoinOperator}.
 */
@RunWith(Parameterized.class)
public class String2SortMergeJoinOperatorTest {

	private boolean leftIsSmall;
	BaseRowTypeInfo typeInfo = new BaseRowTypeInfo(
			InternalTypes.STRING, InternalTypes.STRING);
	private BaseRowTypeInfo joinedInfo = new BaseRowTypeInfo(
			InternalTypes.STRING, InternalTypes.STRING, InternalTypes.STRING, InternalTypes.STRING);

	public String2SortMergeJoinOperatorTest(boolean leftIsSmall) {
		this.leftIsSmall = leftIsSmall;
	}

	@Parameterized.Parameters
	public static Collection<Boolean> parameters() {
		return Arrays.asList(true, false);
	}

	@Test
	public void testInnerJoin() throws Exception {
		StreamOperator joinOperator = newOperator(FlinkJoinType.INNER, leftIsSmall);
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
		StreamOperator joinOperator = newOperator(FlinkJoinType.LEFT, leftIsSmall);
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
		StreamOperator joinOperator = newOperator(FlinkJoinType.RIGHT, leftIsSmall);
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
		StreamOperator joinOperator = newOperator(FlinkJoinType.FULL, leftIsSmall);
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
		testHarness.waitForInputProcessing();

		testHarness.endInput();
		return testHarness;
	}

	static StreamOperator newOperator(FlinkJoinType type, boolean leftIsSmaller) {
		return new SortMergeJoinOperator(
				32 * 32 * 1024, 1024 * 1024, type, leftIsSmaller,
				new GeneratedJoinCondition("", "", new Object[0]) {
					@Override
					public JoinCondition newInstance(ClassLoader classLoader) {
						return (in1, in2) -> true;
					}
				},
				new GeneratedProjection("", "", new Object[0]) {
					@Override
					public Projection newInstance(ClassLoader classLoader) {
						return new MyProjection();
					}
				},
				new GeneratedProjection("", "", new Object[0]) {
					@Override
					public Projection newInstance(ClassLoader classLoader) {
						return new MyProjection();
					}
				},
				new GeneratedNormalizedKeyComputer("", "") {
					@Override
					public NormalizedKeyComputer newInstance(ClassLoader classLoader) {
						return new StringNormalizedKeyComputer();
					}
				},
				new GeneratedRecordComparator("", "", new Object[0]) {
					@Override
					public RecordComparator newInstance(ClassLoader classLoader) {
						return new StringRecordComparator();
					}
				},
				new GeneratedNormalizedKeyComputer("", "") {
					@Override
					public NormalizedKeyComputer newInstance(ClassLoader classLoader) {
						return new StringNormalizedKeyComputer();
					}
				},
				new GeneratedRecordComparator("", "", new Object[0]) {
					@Override
					public RecordComparator newInstance(ClassLoader classLoader) {
						return new StringRecordComparator();
					}
				},
				new GeneratedRecordComparator("", "", new Object[0]) {
					@Override
					public RecordComparator newInstance(ClassLoader classLoader) {
						return new StringRecordComparator();
					}
				},
				new boolean[]{true});
	}
}
