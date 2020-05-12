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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTaskTestHarness;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.runtime.util.UniformBinaryRowGenerator;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import static java.lang.Long.valueOf;

/**
 * Random test for {@link HashJoinOperator}.
 */
public class Int2HashJoinOperatorTest implements Serializable {

	//---------------------- build first inner join -----------------------------------------
	@Test
	public void testBuildFirstHashInnerJoin() throws Exception {

		int numKeys = 100;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, false, false, true, numKeys * buildValsPerKey * probeValsPerKey,
				numKeys, 165);
	}

	//---------------------- build first left out join -----------------------------------------
	@Test
	public void testBuildFirstHashLeftOutJoin() throws Exception {

		int numKeys1 = 9;
		int numKeys2 = 10;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, true, false, true, numKeys1 * buildValsPerKey * probeValsPerKey,
				numKeys1, 165);
	}

	//---------------------- build first right out join -----------------------------------------
	@Test
	public void testBuildFirstHashRightOutJoin() throws Exception {

		int numKeys1 = 9;
		int numKeys2 = 10;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, false, true, true, 280, numKeys2, -1);
	}

	//---------------------- build first full out join -----------------------------------------
	@Test
	public void testBuildFirstHashFullOutJoin() throws Exception {

		int numKeys1 = 9;
		int numKeys2 = 10;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, true, true, true, 280, numKeys2, -1);
	}

	//---------------------- build second inner join -----------------------------------------
	@Test
	public void testBuildSecondHashInnerJoin() throws Exception {

		int numKeys = 100;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, false, false, false, numKeys * buildValsPerKey * probeValsPerKey,
				numKeys, 165);
	}

	//---------------------- build second left out join -----------------------------------------
	@Test
	public void testBuildSecondHashLeftOutJoin() throws Exception {

		int numKeys1 = 10;
		int numKeys2 = 9;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, true, false, false, numKeys2 * buildValsPerKey * probeValsPerKey,
				numKeys2, 165);
	}

	//---------------------- build second right out join -----------------------------------------
	@Test
	public void testBuildSecondHashRightOutJoin() throws Exception {

		int numKeys1 = 9;
		int numKeys2 = 10;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, false, true, false,
				numKeys1 * buildValsPerKey * probeValsPerKey, numKeys2, -1);
	}

	//---------------------- build second full out join -----------------------------------------
	@Test
	public void testBuildSecondHashFullOutJoin() throws Exception {

		int numKeys1 = 9;
		int numKeys2 = 10;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, true, true, false, 280, numKeys2, -1);
	}

	@Test
	public void testSemiJoin() throws Exception {

		int numKeys1 = 9;
		int numKeys2 = 10;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		HashJoinType type = HashJoinType.SEMI;
		Object operator = newOperator(33 * 32 * 1024, type, false);
		joinAndAssert(operator, buildInput, probeInput, 90, 9, 45, true);
	}

	@Test
	public void testAntiJoin() throws Exception {

		int numKeys1 = 9;
		int numKeys2 = 10;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		HashJoinType type = HashJoinType.ANTI;
		Object operator = newOperator(33 * 32 * 1024, type, false);
		joinAndAssert(operator, buildInput, probeInput, 10, 1, 45, true);
	}

	@Test
	public void testBuildLeftSemiJoin() throws Exception {

		int numKeys1 = 10;
		int numKeys2 = 9;
		int buildValsPerKey = 10;
		int probeValsPerKey = 3;
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		HashJoinType type = HashJoinType.BUILD_LEFT_SEMI;
		Object operator = newOperator(33 * 32 * 1024, type, false);
		joinAndAssert(operator, buildInput, probeInput, 90, 9, 45, true);
	}

	@Test
	public void testBuildLeftAntiJoin() throws Exception {

		int numKeys1 = 10;
		int numKeys2 = 9;
		int buildValsPerKey = 10;
		int probeValsPerKey = 3;
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		HashJoinType type = HashJoinType.BUILD_LEFT_ANTI;
		Object operator = newOperator(33 * 32 * 1024, type, false);
		joinAndAssert(operator, buildInput, probeInput, 10, 1, 45, true);
	}

	private void buildJoin(
			MutableObjectIterator<BinaryRowData> buildInput,
			MutableObjectIterator<BinaryRowData> probeInput,
			boolean leftOut, boolean rightOut, boolean buildLeft,
			int expectOutSize, int expectOutKeySize, int expectOutVal) throws Exception {
		HashJoinType type = HashJoinType.of(buildLeft, leftOut, rightOut);
		Object operator = newOperator(33 * 32 * 1024, type, !buildLeft);
		joinAndAssert(operator, buildInput, probeInput, expectOutSize, expectOutKeySize, expectOutVal, false);
	}

	@SuppressWarnings("unchecked")
	static void joinAndAssert(
			Object operator,
			MutableObjectIterator<BinaryRowData> input1,
			MutableObjectIterator<BinaryRowData> input2,
			int expectOutSize,
			int expectOutKeySize,
			int expectOutVal,
			boolean semiJoin) throws Exception {
		RowDataTypeInfo typeInfo = new RowDataTypeInfo(new IntType(), new IntType());
		RowDataTypeInfo rowDataTypeInfo = new RowDataTypeInfo(
				new IntType(), new IntType(), new IntType(), new IntType());
		TwoInputStreamTaskTestHarness<BinaryRowData, BinaryRowData, JoinedRowData> testHarness =
			new TwoInputStreamTaskTestHarness<>(
				TwoInputStreamTask::new,
				2, 1, new int[]{1, 2}, typeInfo, (TypeInformation) typeInfo, rowDataTypeInfo);
		testHarness.memorySize = 36 * 1024 * 1024;
		testHarness.getExecutionConfig().enableObjectReuse();
		testHarness.setupOutputForSingletonOperatorChain();
		if (operator instanceof StreamOperator) {
			testHarness.getStreamConfig().setStreamOperator((StreamOperator<?>) operator);
		} else {
			testHarness.getStreamConfig().setStreamOperatorFactory((StreamOperatorFactory<?>) operator);
		}
		testHarness.getStreamConfig().setOperatorID(new OperatorID());
		testHarness.getStreamConfig().setManagedMemoryFraction(0.99);

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		Random random = new Random();
		do {
			BinaryRowData row1 = null;
			BinaryRowData row2 = null;

			if (random.nextInt(2) == 0) {
				row1 = input1.next();
				if (row1 == null) {
					row2 = input2.next();
				}
			} else {
				row2 = input2.next();
				if (row2 == null) {
					row1 = input1.next();
				}
			}

			if (row1 == null && row2 == null) {
				break;
			}

			if (row1 != null) {
				testHarness.processElement(new StreamRecord<>(row1), 0 , 0);
			} else {
				testHarness.processElement(new StreamRecord<>(row2), 1 , 0);
			}
		} while (true);

		testHarness.endInput(0, 0);
		testHarness.endInput(1, 0);

		testHarness.waitForInputProcessing();
		testHarness.waitForTaskCompletion();

		Queue<Object> actual = testHarness.getOutput();

		Assert.assertEquals("Output was not correct.", expectOutSize, actual.size());

		// Don't verify the output value when experOutVal is -1
		if (expectOutVal != -1) {
			if (semiJoin) {
				HashMap<Integer, Long> map = new HashMap<>(expectOutKeySize);

				for (Object o : actual) {
					StreamRecord<RowData> record = (StreamRecord<RowData>) o;
					RowData row = record.getValue();
					int key = row.getInt(0);
					int val = row.getInt(1);
					Long contained = map.get(key);
					if (contained == null) {
						contained = (long) val;
					} else {
						contained = valueOf(contained + val);
					}
					map.put(key, contained);
				}

				Assert.assertEquals("Wrong number of keys", expectOutKeySize, map.size());
				for (Map.Entry<Integer, Long> entry : map.entrySet()) {
					long val = entry.getValue();
					int key = entry.getKey();

					Assert.assertEquals("Wrong number of values in per-key cross product for key " + key,
							expectOutVal, val);
				}
			} else {
				// create the map for validating the results
				HashMap<Integer, Long> map = new HashMap<>(expectOutKeySize);

				for (Object o : actual) {
					StreamRecord<RowData> record = (StreamRecord<RowData>) o;
					RowData row = record.getValue();
					int key = row.isNullAt(0) ? row.getInt(2) : row.getInt(0);

					int val1 = 0;
					int val2 = 0;
					if (!row.isNullAt(1)) {
						val1 = row.getInt(1);
					}
					if (!row.isNullAt(3)) {
						val2 = row.getInt(3);
					}
					int val = val1 + val2;

					Long contained = map.get(key);
					if (contained == null) {
						contained = (long) val;
					} else {
						contained = valueOf(contained + val);
					}
					map.put(key, contained);
				}

				Assert.assertEquals("Wrong number of keys", expectOutKeySize, map.size());
				for (Map.Entry<Integer, Long> entry : map.entrySet()) {
					long val = entry.getValue();
					int key = entry.getKey();

					Assert.assertEquals("Wrong number of values in per-key cross product for key " + key,
							expectOutVal, val);
				}
			}
		}
	}

	/**
	 * my projection.
	 */
	public static final class MyProjection implements Projection<RowData, BinaryRowData> {

		BinaryRowData innerRow = new BinaryRowData(1);
		BinaryRowWriter writer = new BinaryRowWriter(innerRow);

		@Override
		public BinaryRowData apply(RowData row) {
			writer.reset();
			if (row.isNullAt(0)) {
				writer.setNullAt(0);
			} else {
				writer.writeInt(0, row.getInt(0));
			}
			writer.complete();
			return innerRow;
		}
	}

	public Object newOperator(long memorySize, HashJoinType type, boolean reverseJoinFunction) {
		return HashJoinOperator.newHashJoinOperator(
				type,
				new GeneratedJoinCondition("", "", new Object[0]) {
					@Override
					public JoinCondition newInstance(ClassLoader classLoader) {
						return new TrueCondition();
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
				10000, RowType.of(new IntType()));
	}

	/**
	 * Test util.
	 */
	public static class TrueCondition extends AbstractRichFunction implements JoinCondition {

		@Override
		public boolean apply(RowData in1, RowData in2) {
			return true;
		}
	}
}
