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
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.table.codegen.GeneratedSorter;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.UniformBinaryRowGenerator;
import org.apache.flink.table.plan.FlinkJoinRelType;
import org.apache.flink.table.runtime.join.batch.Int2HashJoinOperatorTest.MyProjection;
import org.apache.flink.table.runtime.join.batch.RandomSortMergeInnerJoinTest.MyConditionFunction;
import org.apache.flink.table.runtime.sort.InMemorySortTest;
import org.apache.flink.table.runtime.sort.NormalizedKeyComputer;
import org.apache.flink.table.runtime.sort.RecordComparator;
import org.apache.flink.util.MutableObjectIterator;

import org.codehaus.commons.compiler.CompileException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Test for {@link MergeJoinOperator}.
 */
public class Int2MergeJoinOperatorTest {

	private MemoryManager memManager;
	private IOManager ioManager;

	@Before
	public void setup() {
		this.memManager = new MemoryManager(36 * 1024 * 1024, 1);
		this.ioManager = new IOManagerAsync();
	}

	@After
	public void tearDown() {
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		this.ioManager.shutdown();
		if (!this.ioManager.isProperlyShutDown()) {
			fail("I/O manager was not property shut down.");
		}
		if (!this.memManager.verifyEmpty()) {
			fail("Not all memory was properly released to the memory manager --> Memory Leak.");
		}
	}

	@Test
	public void testInnerJoin() throws Exception {
		int numKeys = 100;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;

		// create a build input that gives 300 pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRow> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, true);
		// create a probe input that gives 1000 pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRow> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, FlinkJoinRelType.INNER, numKeys * buildValsPerKey * probeValsPerKey,
			numKeys, 165);
	}

	@Test
	public void testInnerJoinSameNumberOfRecordsPerKey() throws Exception {
		int numKeys = 100;
		int buildValsPerKey = 5;
		int probeValsPerKey = 5;

		MutableObjectIterator<BinaryRow> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, true);
		MutableObjectIterator<BinaryRow> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, FlinkJoinRelType.INNER, numKeys * buildValsPerKey * probeValsPerKey,
			numKeys, 100);
	}

	@Test
	public void testInnerJoinSameKey() throws Exception {
		int numKeys = 1;
		int buildValsPerKey = 30;
		int probeValsPerKey = 100;

		MutableObjectIterator<BinaryRow> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, true);
		MutableObjectIterator<BinaryRow> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, FlinkJoinRelType.INNER, numKeys * buildValsPerKey * probeValsPerKey,
			numKeys, 192000);
	}

	@Test
	public void testLeftOutJoin() throws Exception {

		int numKeys1 = 9;
		int numKeys2 = 10;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRow> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRow> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, FlinkJoinRelType.LEFT, numKeys1 * buildValsPerKey * probeValsPerKey,
			numKeys1, 165);
	}

	@Test
	public void testRightOutJoin() throws Exception {
		int numKeys1 = 9;
		int numKeys2 = 10;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRow> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRow> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, FlinkJoinRelType.RIGHT, 280, numKeys2, -1);
	}

	@Test
	public void testFullOutJoin() throws Exception {
		int numKeys1 = 9;
		int numKeys2 = 10;
		int buildValsPerKey = 3;
		int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRow> buildInput = new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRow> probeInput = new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

		buildJoin(buildInput, probeInput, FlinkJoinRelType.FULL, 280, numKeys2, -1);
	}

	private void buildJoin(
		MutableObjectIterator<BinaryRow> input1,
		MutableObjectIterator<BinaryRow> input2,
		FlinkJoinRelType type,
		int expertOutSize, int expertOutKeySize, int expertOutVal) throws Exception {

		Int2HashJoinOperatorTest.joinAndAssert(
			getOperator(type),
			input1, input2, expertOutSize, expertOutKeySize, expertOutVal);
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
					InMemorySortTest.getIntSortBase(0, true, "Int2MergeJoinOperatorTest");
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
}
