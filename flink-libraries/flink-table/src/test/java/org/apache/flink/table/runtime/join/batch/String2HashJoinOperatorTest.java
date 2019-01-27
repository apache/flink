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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTaskTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.runtime.join.batch.RandomSortMergeInnerJoinTest.MyConditionFunction;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.codehaus.commons.compiler.CompileException;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

/**
 * Test for {@link HashJoinOperator}.
 */
public class String2HashJoinOperatorTest {

	private BaseRowTypeInfo typeInfo = new BaseRowTypeInfo(STRING_TYPE_INFO, STRING_TYPE_INFO);
	private BaseRowTypeInfo joinedInfo = new BaseRowTypeInfo(
			STRING_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO);
	private TwoInputStreamTaskTestHarness<BinaryRow, BinaryRow, JoinedRow> testHarness;
	private ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
	private long initialTime = 0L;

	public static LinkedBlockingQueue<Object> transformToBinary(LinkedBlockingQueue<Object> output) {
		LinkedBlockingQueue<Object> ret = new LinkedBlockingQueue<>();
		for (Object o : output) {
			BaseRow row = ((StreamRecord<BaseRow>) o).getValue();
			BinaryRow binaryRow;
			if (row.isNullAt(0)) {
				binaryRow = newRow(row.getString(2), row.getString(3) + "null");
			} else if (row.isNullAt(2)) {
				binaryRow = newRow(row.getString(0), row.getString(1) + "null");
			} else {
				String value1 = row.getString(1);
				String value2 = row.getString(3);
				binaryRow = newRow(row.getString(0), value1 + value2);
			}
			ret.add(new StreamRecord(binaryRow));
		}
		return ret;
	}

	private void init(boolean leftOut, boolean rightOut, boolean buildLeft) throws Exception {
		HashJoinType type = HashJoinType.of(buildLeft, leftOut, rightOut);
		StreamOperator operator = new TestHashJoinOperator(33 * 32 * 1024, type, !buildLeft);
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

	@Test
	public void testInnerHashJoin() throws Exception {

		init(false, false, true);

		testHarness.processElement(new StreamRecord<>(newRow("a", "0"), initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<>(newRow("d", "0"), initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<>(newRow("a", "2"), initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<>(newRow("b", "1"), initialTime), 0, 1);

		testHarness.waitForInputProcessing(Collections.singletonList(0));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput,
				transformToBinary(testHarness.getOutput()));

		testHarness.endInput(0);
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
		testHarness.processElement(new StreamRecord<>(newRow("a", "2"), initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<>(newRow("b", "1"), initialTime), 0, 1);

		testHarness.waitForInputProcessing(Collections.singletonList(0));

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));

		testHarness.endInput(0);
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
		testHarness.processElement(new StreamRecord<>(newRow("a", "2"), initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<>(newRow("b", "1"), initialTime), 0, 1);

		testHarness.waitForInputProcessing(Collections.singletonList(0));

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));

		testHarness.endInput(0);
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
		testHarness.processElement(new StreamRecord<>(newRow("a", "2"), initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<>(newRow("b", "1"), initialTime), 0, 1);

		testHarness.waitForInputProcessing(Collections.singletonList(0));

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				transformToBinary(testHarness.getOutput()));

		testHarness.endInput(0);
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
	public static final class MyProjection extends Projection<BinaryRow, BinaryRow> {

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
				writer.writeString(i, s[i]);
			}
		}

		writer.complete();
		return row;
	}

	/**
	 * Override cookGeneratedClasses.
	 */
	static class TestHashJoinOperator extends HashJoinOperator {

		public TestHashJoinOperator(
				long memorySize, HashJoinType type,
				boolean reverseJoinFunction) {
			super(new HashJoinParameter(memorySize, memorySize, 0, type, null, reverseJoinFunction,
					new boolean[]{true}, null, null, false, 20, 10000,
					10000, new RowType(DataTypes.STRING)));
		}

		@Override
		protected void cookGeneratedClasses(ClassLoader cl) throws CompileException {
			condFuncClass = (Class) MyConditionFunction.class;
			buildProjectionClass = (Class) MyProjection.class;
			probeProjectionClass = (Class) MyProjection.class;
		}

		@Override
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
			if (buildIter.advanceNext()) {
				if (probeRow != null) {
					collect(buildIter.getRow(), probeRow);
					while (buildIter.advanceNext()) {
						collect(buildIter.getRow(), probeRow);
					}
				} else {
					if (type.isBuildOuter()) { // build side outer join
						collect(buildIter.getRow(), probeSideNullRow);
						while (buildIter.advanceNext()) {
							collect(buildIter.getRow(), probeSideNullRow);
						}
					}
				}
			} else if (probeRow != null && type.isProbeOuter()) {
				collect(buildSideNullRow, probeRow);
			}
		}
	}
}
