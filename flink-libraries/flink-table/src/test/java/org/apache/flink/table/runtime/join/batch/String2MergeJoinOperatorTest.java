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
import org.apache.flink.table.codegen.GeneratedSorter;
import org.apache.flink.table.dataformat.BaseRow;
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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.table.runtime.join.batch.String2HashJoinOperatorTest.newRow;

/**
 * Test for {@link MergeJoinOperator}.
 */
public class String2MergeJoinOperatorTest {

	private BaseRowTypeInfo typeInfo = new BaseRowTypeInfo(
		BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
	private BaseRowTypeInfo joinedInfo = new BaseRowTypeInfo(
		STRING_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO);

	private TestData[] data1;

	public String2MergeJoinOperatorTest() {
		data1 = new TestData[] {
			new TestData(newRow("a", "0"), 0, 0),
			new TestData(newRow("b", "1"), 0, 0),
			new TestData(newRow("a", "2"), 1, 0),
			new TestData(newRow("d", "0"), 0, 0),
			new TestData(newRow("b", "4"), 1, 0),
			new TestData(newRow("c", "2"), 1, 0)
		};
	}

	@Test
	public void testInnerJoin() throws Exception {
		BinaryRow[] expected = new BinaryRow[] {
			newRow("a", "0", "a", "2"),
			newRow("b", "1", "b", "4")
		};

		checkResult(FlinkJoinRelType.INNER, data1, expected);
	}

	@Test
	public void testInnerJoinEmptyResult() throws Exception {
		TestData[] testData = new TestData[] {
			new TestData(newRow("a", "0"), 0, 0),
			new TestData(newRow("b", "1"), 0, 0),
			new TestData(newRow("c", "2"), 1, 0),
			new TestData(newRow("d", "0"), 0, 0),
			new TestData(newRow("e", "4"), 1, 0),
			new TestData(newRow("f", "2"), 1, 0)
		};

		BinaryRow[] expected = new BinaryRow[] {};

		checkResult(FlinkJoinRelType.INNER, testData, expected);
	}

	@Test
	public void testInnerJoinOneSideFirst() throws Exception {
		TestData[] testData = new TestData[] {
			new TestData(newRow("a", "0"), 1, 0),
			new TestData(newRow("c", "0"), 1, 0),
			new TestData(newRow("c", "1"), 1, 0),
			new TestData(newRow("d", "0"), 1, 0),
			new TestData(newRow("d", "1"), 1, 0),
			new TestData(newRow("d", "2"), 1, 0),
			new TestData(newRow("a", "0"), 0, 0),
			new TestData(newRow("a", "1"), 0, 0),
			new TestData(newRow("b", "0"), 0, 0),
			new TestData(newRow("c", "0"), 0, 0),
			new TestData(newRow("c", "1"), 0, 0),
			new TestData(newRow("c", "2"), 0, 0)
		};

		BinaryRow[] expected = new BinaryRow[] {
			newRow("a", "0", "a", "0"),
			newRow("a", "1", "a", "0"),
			newRow("c", "0", "c", "0"),
			newRow("c", "0", "c", "1"),
			newRow("c", "1", "c", "0"),
			newRow("c", "1", "c", "1"),
			newRow("c", "2", "c", "0"),
			newRow("c", "2", "c", "1")
		};

		checkResult(FlinkJoinRelType.INNER, testData, expected);
	}

	@Test
	public void testInnerJoinFilterNull() throws Exception {
		TestData[] testData = new TestData[] {
			new TestData(newRow("a", "0"), 0, 0),
			new TestData(newRow("c", "0"), 0, 0),
			new TestData(newRow("a", "2"), 1, 0),
			new TestData(newRow(null, "1"), 0, 0),
			new TestData(newRow("b", "4"), 1, 0),
			new TestData(newRow(null, "2"), 1, 0)
		};

		BinaryRow[] expected = new BinaryRow[] {
			newRow("a", "0", "a", "2")
		};

		checkResult(FlinkJoinRelType.INNER, testData, expected);
	}

	@Test
	public void testLeftOuterJoin() throws Exception {
		BinaryRow[] expected = new BinaryRow[] {
			newRow("a", "0", "a", "2"),
			newRow("b", "1", "b", "4"),
			newRow("d", "0", null, null)
		};

		checkResult(FlinkJoinRelType.LEFT, data1, expected);
	}

	@Test
	public void testLeftOuterJoinFilterNull() throws Exception {
		TestData[] testData = new TestData[] {
			new TestData(newRow("a", "0"), 0, 0),
			new TestData(newRow("c", "0"), 0, 0),
			new TestData(newRow("a", "2"), 1, 0),
			new TestData(newRow(null, "1"), 0, 0),
			new TestData(newRow("b", "4"), 1, 0),
			new TestData(newRow(null, "2"), 1, 0)
		};

		BinaryRow[] expected = new BinaryRow[] {
			newRow("a", "0", "a", "2"),
			newRow("c", "0", null, null),
			newRow(null, "1", null, null)
		};

		checkResult(FlinkJoinRelType.LEFT, testData, expected);
	}

	@Test
	public void testRightOuterJoin() throws Exception {
		BinaryRow[] expected = new BinaryRow[] {
			newRow("a", "0", "a", "2"),
			newRow("b", "1", "b", "4"),
			newRow(null, null, "c", "2")
		};

		checkResult(FlinkJoinRelType.RIGHT, data1, expected);
	}

	@Test
	public void testRightOuterJoinFilterNull() throws Exception {
		TestData[] testData = new TestData[] {
			new TestData(newRow("a", "0"), 0, 0),
			new TestData(newRow("c", "0"), 0, 0),
			new TestData(newRow("a", "2"), 1, 0),
			new TestData(newRow(null, "1"), 0, 0),
			new TestData(newRow("b", "4"), 1, 0),
			new TestData(newRow(null, "2"), 1, 0)
		};

		BinaryRow[] expected = new BinaryRow[] {
			newRow("a", "0", "a", "2"),
			newRow(null, null, "b", "4"),
			newRow(null, null, null, "2")
		};

		checkResult(FlinkJoinRelType.RIGHT, testData, expected);
	}

	@Test
	public void testFullOuterJoin() throws Exception {
		BinaryRow[] expected = new BinaryRow[] {
			newRow("a", "0", "a", "2"),
			newRow("b", "1", "b", "4"),
			newRow(null, null, "c", "2"),
			newRow("d", "0", null, null)
		};

		checkResult(FlinkJoinRelType.FULL, data1, expected);
	}

	@Test
	public void testFullOuterJoinFilterNull() throws Exception {
		TestData[] testData = new TestData[] {
			new TestData(newRow("a", "0"), 0, 0),
			new TestData(newRow("c", "0"), 0, 0),
			new TestData(newRow("a", "2"), 1, 0),
			new TestData(newRow(null, "1"), 0, 0),
			new TestData(newRow("b", "4"), 1, 0),
			new TestData(newRow(null, "2"), 1, 0)
		};

		BinaryRow[] expected = new BinaryRow[] {
			newRow("a", "0", "a", "2"),
			newRow(null, null, "b", "4"),
			newRow("c", "0", null, null),
			newRow(null, "1", null, null),
			newRow(null, null, null, "2")
		};

		checkResult(FlinkJoinRelType.FULL, testData, expected);
	}

	protected void checkResult(FlinkJoinRelType type, TestData[] testData, BinaryRow[] expected) throws Exception {
		StreamOperator joinOperator = getOperator(type);
		TwoInputStreamTaskTestHarness<BinaryRow, BinaryRow, JoinedRow> testHarness =
			buildSortMergeJoin(joinOperator, testData);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		for (BinaryRow row : expected) {
			expectedOutput.add(new StreamRecord<>(row));
		}

		testHarness.waitForTaskCompletion();
		Assert.assertArrayEquals(
			"Output was not correct.",
			buildStringArray(expectedOutput.toArray()),
			buildStringArray(transformToBinary(testHarness.getOutput()).toArray()));
	}

	private LinkedBlockingQueue<Object> transformToBinary(LinkedBlockingQueue<Object> output) {
		LinkedBlockingQueue<Object> ret = new LinkedBlockingQueue<>();
		for (Object o : output) {
			BaseRow row = ((StreamRecord<BaseRow>) o).getValue();
			BinaryRow binaryRow = newRow(
				row.isNullAt(0) ? null : row.getString(0),
				row.isNullAt(1) ? null : row.getString(1),
				row.isNullAt(2) ? null : row.getString(2),
				row.isNullAt(3) ? null : row.getString(3));
			ret.add(new StreamRecord(binaryRow));
		}
		return ret;
	}

	private String[] buildStringArray(Object[] arr) {
		ArrayList<String> list = new ArrayList<>();
		for (Object obj : arr) {
			list.add(obj.toString());
		}

		String[] res = list.toArray(new String[0]);
		Arrays.sort(res);
		return res;
	}

	private TwoInputStreamTaskTestHarness<BinaryRow, BinaryRow, JoinedRow> buildSortMergeJoin(
		StreamOperator operator, TestData[] testData) throws Exception {
		final TwoInputStreamTaskTestHarness<BinaryRow, BinaryRow, JoinedRow> testHarness =
			new TwoInputStreamTaskTestHarness<>(
				TwoInputStreamTask::new, 2, 1, new int[]{1, 2}, typeInfo, (TypeInformation) typeInfo,
				joinedInfo);

		testHarness.memorySize = 36 * 1024 * 1024;
		testHarness.getExecutionConfig().enableObjectReuse();
		testHarness.setupOutputForSingletonOperatorChain();
		testHarness.getStreamConfig().setStreamOperator(operator);
		testHarness.getStreamConfig().setOperatorID(new OperatorID());

		long initialTime = 0L;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		for (TestData data : testData) {
			testHarness.processElement(
				new StreamRecord<>(data.row, initialTime), data.inputGate, data.channel);
		}
		testHarness.endInput();
		return testHarness;
	}

	protected StreamOperator getOperator(FlinkJoinRelType type) {
		return new TestMergeJoinOperator(type);
	}

	/**
	 * Override cookGeneratedClasses.
	 */
	static class TestMergeJoinOperator extends MergeJoinOperator {

		private TestMergeJoinOperator(FlinkJoinRelType type) {
			super(32 * 32 * 1024, 32 * 32 * 1024, type,
				null, null, null,
				new GeneratedSorter(null, null, null, null),
				new boolean[]{true});
		}

		@Override
		protected CookedClasses cookGeneratedClasses(ClassLoader cl) throws CompileException {
			Class<RecordComparator> comparatorClass;
			try {
				Tuple2<NormalizedKeyComputer, RecordComparator> base =
					InMemorySortTest.getIntSortBase(0, true, "String2MergeJoinOperatorTest");
				comparatorClass = (Class<RecordComparator>) base.f1.getClass();
			} catch (Exception e) {
				throw new RuntimeException();
			}
			return new CookedClasses(
				(Class) MyConditionFunction.class,
				comparatorClass,
				(Class) MyProjection.class,
				(Class) MyProjection.class
			);
		}
	}

	/**
	 * Test data for operator test.
	 */
	protected class TestData {
		private BinaryRow row;
		private int inputGate;
		private int channel;

		protected TestData(BinaryRow row, int inputGate, int channel) {
			this.row = row;
			this.inputGate = inputGate;
			this.channel = channel;
		}
	}
}
