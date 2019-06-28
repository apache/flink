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


package org.apache.flink.runtime.operators.hash;

import org.apache.flink.api.common.typeutils.GenericPairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.hash.NonReusingHashJoinIteratorITCase.TupleMatch;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGeneratorIterator;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;

import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import java.util.*;

public abstract class ReOpenableHashTableTestBase extends TestLogger {

	protected static final int PAGE_SIZE = 8 * 1024;
	protected static final long MEMORY_SIZE = PAGE_SIZE * 1000; // 100 Pages.

	protected static final long SEED1 = 561349061987311L;
	protected static final long SEED2 = 231434613412342L;

	protected static final int NUM_PROBES = 3; // number of reopenings of hash join

	protected final AbstractInvokable parentTask = new DummyInvokable();

	protected IOManager ioManager;
	protected MemoryManager memoryManager;

	protected TypeSerializer<Tuple2<Integer, String>> recordSerializer;
	protected TypeComparator<Tuple2<Integer, String>> record1Comparator;
	protected TypeComparator<Tuple2<Integer, String>> record2Comparator;
	protected TypePairComparator<Tuple2<Integer, String>, Tuple2<Integer, String>> recordPairComparator;

	protected TypeSerializer<Tuple2<Integer, Integer>> recordBuildSideAccesssor;
	protected TypeSerializer<Tuple2<Integer, Integer>> recordProbeSideAccesssor;
	protected TypeComparator<Tuple2<Integer, Integer>> recordBuildSideComparator;
	protected TypeComparator<Tuple2<Integer, Integer>> recordProbeSideComparator;
	protected TypePairComparator<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> pactRecordComparator;

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Before
	public void beforeTest() {
		this.recordSerializer = TestData.getIntStringTupleSerializer();

		this.record1Comparator = TestData.getIntStringTupleComparator();
		this.record2Comparator = TestData.getIntStringTupleComparator();
		this.recordPairComparator = new GenericPairComparator(this.record1Comparator, this.record2Comparator);

		this.recordBuildSideAccesssor = TestData.getIntIntTupleSerializer();
		this.recordProbeSideAccesssor = TestData.getIntIntTupleSerializer();
		this.recordBuildSideComparator = TestData.getIntIntTupleComparator();
		this.recordProbeSideComparator = TestData.getIntIntTupleComparator();
		this.pactRecordComparator = new GenericPairComparator(this.recordBuildSideComparator, this.recordProbeSideComparator);

		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1, PAGE_SIZE, MemoryType.HEAP, true);
		this.ioManager = new IOManagerAsync();
	}

	@After
	public void afterTest() throws Exception {
		if (this.ioManager != null) {
			this.ioManager.close();
			this.ioManager = null;
		}

		if (this.memoryManager != null) {
			Assert.assertTrue("Memory Leak: Not all memory has been returned to the memory manager.",
				this.memoryManager.verifyEmpty());
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}

	/**
	 * Test behavior with overflow buckets (Overflow buckets must be initialized correctly
	 * if the input is reopened again)
	 */
	@Test
	public void testOverflow() {

		int buildSize = 1000;
		int probeSize = 1000;
		try {
			TupleGenerator bgen = new TupleGenerator(SEED1, 200, 1024, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
			TupleGenerator pgen = new TupleGenerator(SEED2, 0, 1024, KeyMode.SORTED, ValueMode.FIX_LENGTH);

			final TupleGeneratorIterator buildInput = new TupleGeneratorIterator(bgen, buildSize);
			final TupleGeneratorIterator probeInput = new TupleGeneratorIterator(pgen, probeSize);
			doTest(buildInput,probeInput, bgen, pgen);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}

	/**
	 * Verify proper operation if the build side is spilled to disk.
	 */
	@Test
	public void testDoubleProbeSpilling() {

		int buildSize = 1000;
		int probeSize = 1000;
		try {
			TupleGenerator bgen = new TupleGenerator(SEED1, 0, 1024, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			TupleGenerator pgen = new TupleGenerator(SEED2, 0, 1024, KeyMode.SORTED, ValueMode.FIX_LENGTH);

			final TupleGeneratorIterator buildInput = new TupleGeneratorIterator(bgen, buildSize);
			final TupleGeneratorIterator probeInput = new TupleGeneratorIterator(pgen, probeSize);
			doTest(buildInput,probeInput, bgen, pgen);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}

	/**
	 * This test case verifies that hybrid hash join is able to handle multiple probe phases
	 * when the build side fits completely into memory.
	 */
	@Test
	public void testDoubleProbeInMemory() {

		int buildSize = 1000;
		int probeSize = 1000;
		try {
			TupleGenerator bgen = new TupleGenerator(SEED1, 0, 28, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			TupleGenerator pgen = new TupleGenerator(SEED2, 0, 28, KeyMode.SORTED, ValueMode.FIX_LENGTH);

			final TupleGeneratorIterator buildInput = new TupleGeneratorIterator(bgen, buildSize);
			final TupleGeneratorIterator probeInput = new TupleGeneratorIterator(pgen, probeSize);

			doTest(buildInput,probeInput, bgen, pgen);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}

	abstract protected void doTest(TupleGeneratorIterator buildInput, TupleGeneratorIterator probeInput, TestData.TupleGenerator bgen, TestData.TupleGenerator pgen) throws Exception;

	static Map<Integer, Collection<TupleMatch>> deepCopy(Map<Integer, Collection<TupleMatch>> expectedSecondMatchesMap) {
		Map<Integer, Collection<TupleMatch>> copy = new HashMap<>(expectedSecondMatchesMap.size());
		for(Map.Entry<Integer, Collection<TupleMatch>> entry : expectedSecondMatchesMap.entrySet()) {
			List<TupleMatch> matches = new ArrayList<TupleMatch>(entry.getValue().size());
			for(TupleMatch m : entry.getValue()) {
				matches.add(m);
			}
			copy.put(entry.getKey(), matches);
		}
		return copy;
	}
}
