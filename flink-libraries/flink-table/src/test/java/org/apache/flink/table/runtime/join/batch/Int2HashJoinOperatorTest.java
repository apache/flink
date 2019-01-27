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
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.dataformat.UniformBinaryRowGenerator;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.MutableObjectIterator;

import org.codehaus.commons.compiler.CompileException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import static java.lang.Long.valueOf;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;

/**
 * Random test for {@link HashJoinOperator}.
 */
public class Int2HashJoinOperatorTest {

	//---------------------- build first inner join -----------------------------------------
	@Test
	public void testBuildFirstHashInnerJoin() throws Exception {

		int numKeys = 100;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;

		// create a build input that gives 300 pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRow> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
		// create a probe input that gives 1000 pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRow> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

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
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRow> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRow> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

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
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRow> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRow> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, false, true, true, 280, numKeys2, -1);
	}

	//---------------------- build first full out join -----------------------------------------
	@Test
	public void testBuildFirstHashFullOutJoin() throws Exception {

		int numKeys1 = 9;
		int numKeys2 = 10;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRow> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRow> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, true, true, true, 280, numKeys2, -1);
	}

	//---------------------- build second inner join -----------------------------------------
	@Test
	public void testBuildSecondHashInnerJoin() throws Exception {

		int numKeys = 100;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;
		// create a build input that gives 300 pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRow> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

		// create a probe input that gives 1000 pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRow> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

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
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRow> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRow> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

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
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRow> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRow> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

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
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRow> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRow> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, true, true, false, 280, numKeys2, -1);
	}

	public void buildJoin(
			MutableObjectIterator<BinaryRow> buildInput,
			MutableObjectIterator<BinaryRow> probeInput,
			boolean leftOut, boolean rightOut, boolean buildLeft,
			int expectOutSize, int expectOutKeySize, int expectOutVal) throws Exception {
		HashJoinType type = HashJoinType.of(buildLeft, leftOut, rightOut);
		StreamOperator operator = new TestHashJoinOperator(33 * 32 * 1024, type, !buildLeft);
		joinAndAssert(operator, buildInput, probeInput, expectOutSize, expectOutKeySize, expectOutVal);
	}

	static void joinAndAssert(
			StreamOperator operator,
			MutableObjectIterator<BinaryRow> input1,
			MutableObjectIterator<BinaryRow> input2,
			int expectOutSize,
			int expectOutKeySize,
			int expectOutVal) throws Exception {
		BaseRowTypeInfo typeInfo = new BaseRowTypeInfo(INT_TYPE_INFO, INT_TYPE_INFO);
		BaseRowTypeInfo baseRowType = new BaseRowTypeInfo(
			INT_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO);
		TwoInputStreamTaskTestHarness<BinaryRow, BinaryRow, JoinedRow> testHarness =
			new TwoInputStreamTaskTestHarness<>(TwoInputStreamTask::new,
				2, 1, new int[]{1, 2}, typeInfo, (TypeInformation) typeInfo, baseRowType);
		testHarness.memorySize = 36 * 1024 * 1024;
		testHarness.getExecutionConfig().enableObjectReuse();
		testHarness.setupOutputForSingletonOperatorChain();
		testHarness.getStreamConfig().setStreamOperator(operator);
		testHarness.getStreamConfig().setOperatorID(new OperatorID());

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		Random random = new Random();
		do {
			BinaryRow row1 = null;
			BinaryRow row2 = null;

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

		testHarness.endInput();
		testHarness.waitForInputProcessing();
		testHarness.waitForTaskCompletion();

		Queue<Object> actual = testHarness.getOutput();

		Assert.assertEquals("Output was not correct.", expectOutSize, actual.size());

		// Don't verify the output value when experOutVal is -1
		if (expectOutVal != -1) {
			// create the map for validating the results
			HashMap<Integer, Long> map = new HashMap<>(expectOutKeySize);

			for (Object o : actual) {
				StreamRecord<BaseRow> record = (StreamRecord<BaseRow>) o;
				BaseRow row = record.getValue();
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

	/**
	 * my projection.
	 */
	public static final class MyProjection extends Projection<BaseRow, BinaryRow> {

		BinaryRow innerRow = new BinaryRow(1);
		BinaryRowWriter writer = new BinaryRowWriter(innerRow);

		@Override
		public BinaryRow apply(BaseRow row) {
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

	/**
	 * Override cookGeneratedClasses.
	 */
	static class TestHashJoinOperator extends HashJoinOperator {

		public TestHashJoinOperator(
				long memorySize, HashJoinType type,
				boolean reverseJoinFunction) {
			super(new HashJoinParameter(memorySize, memorySize, 0, type, null,
					reverseJoinFunction, new boolean[]{true}, null, null,
					false, 20, 10000,
					10000, new RowType(DataTypes.STRING)));
		}

		@Override
		protected void cookGeneratedClasses(ClassLoader cl) throws CompileException {
			condFuncClass = (Class) RandomSortMergeInnerJoinTest.MyConditionFunction.class;
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
