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

package org.apache.flink.table.runtime.join;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTaskTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.generated.GeneratedJoinCondition;
import org.apache.flink.table.generated.GeneratedProjection;
import org.apache.flink.table.generated.JoinCondition;
import org.apache.flink.table.generated.Projection;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.junit.Test;

import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Test for {@link HashJoinOperator}.
 */
public class String2HashJoinOperatorTest implements Serializable {

	private BaseRowTypeInfo typeInfo = new BaseRowTypeInfo(new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH));
	private BaseRowTypeInfo joinedInfo = new BaseRowTypeInfo(
			new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH));
	private transient TwoInputStreamTaskTestHarness<BinaryRow, BinaryRow, JoinedRow> testHarness;
	private ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
	private long initialTime = 0L;

	public static LinkedBlockingQueue<Object> transformToBinary(LinkedBlockingQueue<Object> output) {
		LinkedBlockingQueue<Object> ret = new LinkedBlockingQueue<>();
		for (Object o : output) {
			BaseRow row = ((StreamRecord<BaseRow>) o).getValue();
			BinaryRow binaryRow;
			if (row.isNullAt(0)) {
				binaryRow = newRow(row.getString(2).toString(), row.getString(3) + "null");
			} else if (row.isNullAt(2)) {
				binaryRow = newRow(row.getString(0).toString(), row.getString(1) + "null");
			} else {
				String value1 = row.getString(1).toString();
				String value2 = row.getString(3).toString();
				binaryRow = newRow(row.getString(0).toString(), value1 + value2);
			}
			ret.add(new StreamRecord(binaryRow));
		}
		return ret;
	}

	private void init(boolean leftOut, boolean rightOut, boolean buildLeft) throws Exception {
		HashJoinType type = HashJoinType.of(buildLeft, leftOut, rightOut);
		HashJoinOperator operator = newOperator(33 * 32 * 1024, type, !buildLeft);
		testHarness = new TwoInputStreamTaskTestHarness<>(
				TwoInputStreamTask::new, 2, 2, new int[]{1, 2}, typeInfo, (TypeInformation) typeInfo, joinedInfo);
		testHarness.memorySize = 36 * 1024 * 1024;
		testHarness.getExecutionConfig().enableObjectReuse();
		testHarness.setupOutputForSingletonOperatorChain();
		testHarness.getStreamConfig().setStreamOperator(operator);
		testHarness.getStreamConfig().setOperatorID(new OperatorID());

		testHarness.invoke();
		testHarness.waitForTaskRunning();
	}

	private void endInput1() throws Exception {
		endInput1(testHarness);
	}

	private void endInput2() throws Exception {
		endInput2(testHarness);
	}

	static void endInput1(TwoInputStreamTaskTestHarness harness) throws Exception {
		HashJoinOperator op =
				(HashJoinOperator) ((OperatorChain) harness.getTask().getStreamStatusMaintainer())
						.getHeadOperator();
		op.endInput1();
	}

	static void endInput2(TwoInputStreamTaskTestHarness harness) throws Exception {
		HashJoinOperator op =
				(HashJoinOperator) ((OperatorChain) harness.getTask().getStreamStatusMaintainer())
						.getHeadOperator();
		op.endInput2();
	}

	@Test
	public void testInnerHashJoin() throws Exception {

		init(false, false, true);

		testHarness.processElement(new StreamRecord<>(newRow("a", "0"), initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<>(newRow("d", "0"), initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<>(newRow("b", "1"), initialTime), 0, 1);

		testHarness.waitForInputProcessing();

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput,
				transformToBinary(testHarness.getOutput()));

		endInput1();
		testHarness.processElement(new StreamRecord<>(newRow("a", "2"), initialTime), 1, 1);

		testHarness.waitForInputProcessing();
		expectedOutput.add(new StreamRecord<>(newRow("a", "02")));
		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput,
				transformToBinary(testHarness.getOutput()));

		testHarness.processElement(new StreamRecord<>(newRow("c", "2"), initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<>(newRow("b", "4"), initialTime), 1, 0);
		expectedOutput.add(new StreamRecord<>(newRow("b", "14")));
		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput,
				transformToBinary(testHarness.getOutput()));

		testHarness.endInput();
		testHarness.waitForTaskCompletion();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput,
				transformToBinary(testHarness.getOutput()));
	}

	@Test
	public void testProbeOuterHashJoin() throws Exception {

		init(true, false, false);

		testHarness.processElement(new StreamRecord<>(newRow("a", "0"), initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<>(newRow("d", "0"), initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<>(newRow("b", "1"), initialTime), 0, 1);

		testHarness.waitForInputProcessing();

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));

		endInput1();
		testHarness.processElement(new StreamRecord<>(newRow("a", "2"), initialTime), 1, 1);
		testHarness.waitForInputProcessing();

		expectedOutput.add(new StreamRecord<>(newRow("a", "20")));
		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));

		testHarness.processElement(new StreamRecord<>(newRow("c", "2"), initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<>(newRow("b", "4"), initialTime), 1, 0);
		expectedOutput.add(new StreamRecord<>(newRow("c", "2null")));
		expectedOutput.add(new StreamRecord<>(newRow("b", "41")));
		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));

		testHarness.endInput();
		testHarness.waitForTaskCompletion();
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));
	}

	@Test
	public void testBuildOuterHashJoin() throws Exception {

		init(false, true, false);

		testHarness.processElement(new StreamRecord<>(newRow("a", "0"), initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<>(newRow("d", "0"), initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<>(newRow("b", "1"), initialTime), 0, 1);

		testHarness.waitForInputProcessing();

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));

		endInput1();
		testHarness.processElement(new StreamRecord<>(newRow("a", "2"), initialTime), 1, 1);
		testHarness.waitForInputProcessing();

		expectedOutput.add(new StreamRecord<>(newRow("a", "20")));
		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));

		testHarness.processElement(new StreamRecord<>(newRow("c", "2"), initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<>(newRow("b", "4"), initialTime), 1, 0);
		expectedOutput.add(new StreamRecord<>(newRow("b", "41")));
		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));

		endInput2();
		testHarness.endInput();
		testHarness.waitForTaskCompletion();
		expectedOutput.add(new StreamRecord<>(newRow("d", "0null")));
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));
	}

	@Test
	public void testFullOuterHashJoin() throws Exception {

		init(true, true, true);

		testHarness.processElement(new StreamRecord<>(newRow("a", "0"), initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<>(newRow("d", "0"), initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<>(newRow("b", "1"), initialTime), 0, 1);

		testHarness.waitForInputProcessing();

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));

		endInput1();
		testHarness.processElement(new StreamRecord<>(newRow("a", "2"), initialTime), 1, 1);
		testHarness.waitForInputProcessing();

		expectedOutput.add(new StreamRecord<>(newRow("a", "02")));
		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));

		testHarness.processElement(new StreamRecord<>(newRow("c", "2"), initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<>(newRow("b", "4"), initialTime), 1, 0);
		expectedOutput.add(new StreamRecord<>(newRow("c", "2null")));
		expectedOutput.add(new StreamRecord<>(newRow("b", "14")));
		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));

		endInput2();
		testHarness.endInput();
		testHarness.waitForTaskCompletion();
		expectedOutput.add(new StreamRecord<>(newRow("d", "0null")));
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));
	}

	/**
	 * my project.
	 */
	public static final class MyProjection implements Projection<BinaryRow, BinaryRow> {

		BinaryRow innerRow = new BinaryRow(1);
		BinaryRowWriter writer = new BinaryRowWriter(innerRow);

		@Override
		public BinaryRow apply(BinaryRow row) {
			writer.reset();
			writer.writeString(0, row.getString(0));
			writer.complete();
			return innerRow;
		}
	}

	public static BinaryRow newRow(String... s) {
		BinaryRow row = new BinaryRow(s.length);
		BinaryRowWriter writer = new BinaryRowWriter(row);

		for (int i = 0; i < s.length; i++) {
			if (s[i] == null) {
				writer.setNullAt(i);
			} else {
				writer.writeString(i, BinaryString.fromString(s[i]));
			}
		}

		writer.complete();
		return row;
	}

	private HashJoinOperator newOperator(long memorySize, HashJoinType type, boolean reverseJoinFunction) {
		return HashJoinOperator.newHashJoinOperator(
				memorySize, memorySize, 0, type,
				new GeneratedJoinCondition("", "", new Object[0]) {
					@Override
					public JoinCondition newInstance(ClassLoader classLoader) {
						return (in1, in2) -> true;
					}
				},
				reverseJoinFunction, new boolean[]{true},
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
				false, 20, 10000,
				10000, RowType.of(new VarCharType(VarCharType.MAX_LENGTH)));
	}
}
