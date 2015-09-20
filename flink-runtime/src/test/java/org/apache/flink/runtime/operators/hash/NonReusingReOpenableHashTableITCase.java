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


package org.apache.flink.runtime.operators.hash;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.hash.MutableHashTable.HashBucketIterator;
import org.apache.flink.runtime.operators.hash.NonReusingHashMatchIteratorITCase.TupleMatch;
import org.apache.flink.runtime.operators.hash.NonReusingHashMatchIteratorITCase.TupleMatchRemovingJoin;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;
import org.apache.flink.runtime.operators.testutils.UnionIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.GenericPairComparator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.testutils.UniformIntTupleGenerator;

import static org.junit.Assert.fail;

/**
 * Test specialized hash join that keeps the build side data (in memory and on hard disk)
 * This is used for iterative tasks.
 */
@SuppressWarnings("deprecation")
public class NonReusingReOpenableHashTableITCase {

	private static final int PAGE_SIZE = 8 * 1024;
	private static final long MEMORY_SIZE = PAGE_SIZE * 1000; // 100 Pages.

	private static final long SEED1 = 561349061987311L;
	private static final long SEED2 = 231434613412342L;

	private static final int NUM_PROBES = 3; // number of reopenings of hash join

	private final AbstractInvokable parentTask = new DummyInvokable();

	private IOManager ioManager;
	private MemoryManager memoryManager;

	private TypeSerializer<Tuple2<Integer, String>> recordSerializer;
	private TypeComparator<Tuple2<Integer, String>> record1Comparator;
	private TypeComparator<Tuple2<Integer, String>> record2Comparator;
	private TypePairComparator<Tuple2<Integer, String>, Tuple2<Integer, String>> recordPairComparator;




	private static final AbstractInvokable MEM_OWNER = new DummyInvokable();
	private TypeSerializer<Tuple2<Integer, Integer>> recordBuildSideAccesssor;
	private TypeSerializer<Tuple2<Integer, Integer>> recordProbeSideAccesssor;
	private TypeComparator<Tuple2<Integer, Integer>> recordBuildSideComparator;
	private TypeComparator<Tuple2<Integer, Integer>> recordProbeSideComparator;
	private TypePairComparator<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> pactRecordComparator;

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
	public void afterTest() {
		if (this.ioManager != null) {
			this.ioManager.shutdown();
			if (!this.ioManager.isProperlyShutDown()) {
				Assert.fail("I/O manager failed to properly shut down.");
			}
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

			final TestData.TupleGeneratorIterator buildInput = new TestData.TupleGeneratorIterator(bgen, buildSize);
			final TestData.TupleGeneratorIterator probeInput = new TestData.TupleGeneratorIterator(pgen, probeSize);
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

			final TestData.TupleGeneratorIterator buildInput = new TestData.TupleGeneratorIterator(bgen, buildSize);
			final TestData.TupleGeneratorIterator probeInput = new TestData.TupleGeneratorIterator(pgen, probeSize);
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

			final TestData.TupleGeneratorIterator buildInput = new TestData.TupleGeneratorIterator(bgen, buildSize);
			final TestData.TupleGeneratorIterator probeInput = new TestData.TupleGeneratorIterator(pgen, probeSize);

			doTest(buildInput,probeInput, bgen, pgen);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}

	private void doTest(TestData.TupleGeneratorIterator buildInput, TestData.TupleGeneratorIterator probeInput, TupleGenerator bgen, TupleGenerator pgen) throws Exception {
		// collect expected data
		final Map<Integer, Collection<TupleMatch>> expectedFirstMatchesMap = NonReusingHashMatchIteratorITCase.matchSecondTupleFields(NonReusingHashMatchIteratorITCase.collectTupleData(buildInput), NonReusingHashMatchIteratorITCase.collectTupleData(probeInput));

		final List<Map<Integer, Collection<TupleMatch>>> expectedNMatchesMapList = new ArrayList<>(NUM_PROBES);
		final FlatJoinFunction[] nMatcher = new TupleMatchRemovingJoin[NUM_PROBES];
		for(int i = 0; i < NUM_PROBES; i++) {
			Map<Integer, Collection<TupleMatch>> tmp;
			expectedNMatchesMapList.add(tmp = deepCopy(expectedFirstMatchesMap));
			nMatcher[i] = new TupleMatchRemovingJoin(tmp);
		}

		final FlatJoinFunction firstMatcher = new TupleMatchRemovingJoin(expectedFirstMatchesMap);

		final Collector<Tuple2<Integer, String>> collector = new DiscardingOutputCollector<>();

		// reset the generators
		bgen.reset();
		pgen.reset();
		buildInput.reset();
		probeInput.reset();

		// compare with iterator values
		NonReusingBuildFirstReOpenableHashMatchIterator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> iterator =
				new NonReusingBuildFirstReOpenableHashMatchIterator<>(
						buildInput, probeInput, this.recordSerializer, this.record1Comparator,
					this.recordSerializer, this.record2Comparator, this.recordPairComparator,
					this.memoryManager, ioManager, this.parentTask, 1.0, true);

		iterator.open();
		// do first join with both inputs
		while (iterator.callWithNextKey(firstMatcher, collector));

		// assert that each expected match was seen for the first input
		for (Entry<Integer, Collection<TupleMatch>> entry : expectedFirstMatchesMap.entrySet()) {
			if (!entry.getValue().isEmpty()) {
				Assert.fail("Collection for key " + entry.getKey() + " is not empty");
			}
		}

		for(int i = 0; i < NUM_PROBES; i++) {
			pgen.reset();
			probeInput.reset();
			// prepare ..
			iterator.reopenProbe(probeInput);
			// .. and do second join
			while (iterator.callWithNextKey(nMatcher[i], collector));

			// assert that each expected match was seen for the second input
			for (Entry<Integer, Collection<TupleMatch>> entry : expectedNMatchesMapList.get(i).entrySet()) {
				if (!entry.getValue().isEmpty()) {
					Assert.fail("Collection for key " + entry.getKey() + " is not empty");
				}
			}
		}

		iterator.close();
	}

	//
	//
	//	Tests taken from HahTableITCase!
	//
	//

	private MutableObjectIterator<Tuple2<Integer, Integer>> getProbeInput(final int numKeys,
			final int probeValsPerKey, final int repeatedValue1, final int repeatedValue2) {
		MutableObjectIterator<Tuple2<Integer, Integer>> probe1 = new UniformIntTupleGenerator(numKeys, probeValsPerKey, true);
		MutableObjectIterator<Tuple2<Integer, Integer>> probe2 = new TestData.ConstantIntIntTuplesIterator(repeatedValue1, 17, 5);
		MutableObjectIterator<Tuple2<Integer, Integer>> probe3 = new TestData.ConstantIntIntTuplesIterator(repeatedValue2, 23, 5);
		List<MutableObjectIterator<Tuple2<Integer, Integer>>> probes = new ArrayList<>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		return new UnionIterator<>(probes);
	}

	@Test
	public void testSpillingHashJoinWithMassiveCollisions() throws IOException
	{
		// the following two values are known to have a hash-code collision on the initial level.
		// we use them to make sure one partition grows over-proportionally large
		final int REPEATED_VALUE_1 = 40559;
		final int REPEATED_VALUE_2 = 92882;
		final int REPEATED_VALUE_COUNT_BUILD = 200000;
		final int REPEATED_VALUE_COUNT_PROBE = 5;

		final int NUM_KEYS = 1000000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		MutableObjectIterator<Tuple2<Integer, Integer>> build1 = new UniformIntTupleGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<Tuple2<Integer, Integer>> build2 = new TestData.ConstantIntIntTuplesIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<Tuple2<Integer, Integer>> build3 = new TestData.ConstantIntIntTuplesIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<Tuple2<Integer, Integer>>> builds = new ArrayList<>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<Tuple2<Integer, Integer>> buildInput = new UnionIterator<>(builds);




		// allocate the memory for the HashTable
		List<MemorySegment> memSegments;
		try {
			memSegments = this.memoryManager.allocatePages(MEM_OWNER, 896);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}

		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);

		// ----------------------------------------------------------------------------------------

		final ReOpenableMutableHashTable<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> join = new ReOpenableMutableHashTable<>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor,
				this.recordBuildSideComparator, this.recordProbeSideComparator, this.pactRecordComparator,
				memSegments, ioManager, true);

		for (int probe = 0; probe < NUM_PROBES; probe++) {
			// create a probe input that gives 10 million pairs with 10 values sharing a key
			MutableObjectIterator<Tuple2<Integer, Integer>> probeInput = getProbeInput(NUM_KEYS, PROBE_VALS_PER_KEY, REPEATED_VALUE_1, REPEATED_VALUE_2);
			if(probe == 0) {
				join.open(buildInput, probeInput);
			} else {
				join.reopenProbe(probeInput);
			}

			Tuple2<Integer, Integer> record;
			final Tuple2<Integer, Integer> recordReuse = new Tuple2<>();

			while (join.nextRecord()) {
				long numBuildValues = 0;

				final Tuple2<Integer, Integer> probeRec = join.getCurrentProbeRecord();
				Integer key = probeRec.f0;

				HashBucketIterator<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> buildSide = join.getBuildSideIterator();
				if ((record = buildSide.next(recordReuse)) != null) {
					numBuildValues = 1;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.f0);
				}
				else {
					fail("No build side values found for a probe key.");
				}
				while ((record = buildSide.next(record)) != null) {
					numBuildValues++;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.f0);
				}

				Long contained = map.get(key);
				if (contained == null) {
					contained = numBuildValues;
				}
				else {
					contained = contained + numBuildValues;
				}

				map.put(key, contained);
			}
		}

		join.close();

		Assert.assertEquals("Wrong number of keys", NUM_KEYS, map.size());
		for (Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();

			if( key == REPEATED_VALUE_1 || key == REPEATED_VALUE_2) {
				Assert.assertEquals("Wrong number of values in per-key cross product for key " + key,
							(PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT_PROBE) * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT_BUILD) * NUM_PROBES, val);
			} else {
				Assert.assertEquals("Wrong number of values in per-key cross product for key " + key,
							PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY * NUM_PROBES, val);
			}
		}


		// ----------------------------------------------------------------------------------------

		this.memoryManager.release(join.getFreedMemory());
	}

	/*
	 * This test is basically identical to the "testSpillingHashJoinWithMassiveCollisions" test, only that the number
	 * of repeated values (causing bucket collisions) are large enough to make sure that their target partition no longer
	 * fits into memory by itself and needs to be repartitioned in the recursion again.
	 */
	@Test
	public void testSpillingHashJoinWithTwoRecursions() throws IOException
	{
		// the following two values are known to have a hash-code collision on the first recursion level.
		// we use them to make sure one partition grows over-proportionally large
		final int REPEATED_VALUE_1 = 40559;
		final int REPEATED_VALUE_2 = 92882;
		final int REPEATED_VALUE_COUNT_BUILD = 200000;
		final int REPEATED_VALUE_COUNT_PROBE = 5;

		final int NUM_KEYS = 1000000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		MutableObjectIterator<Tuple2<Integer, Integer>> build1 = new UniformIntTupleGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<Tuple2<Integer, Integer>> build2 = new TestData.ConstantIntIntTuplesIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<Tuple2<Integer, Integer>> build3 = new TestData.ConstantIntIntTuplesIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<Tuple2<Integer, Integer>>> builds = new ArrayList<>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<Tuple2<Integer, Integer>> buildInput = new UnionIterator<>(builds);


		// allocate the memory for the HashTable
		List<MemorySegment> memSegments;
		try {
			memSegments = this.memoryManager.allocatePages(MEM_OWNER, 896);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}

		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);

		// ----------------------------------------------------------------------------------------

		final ReOpenableMutableHashTable<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> join = new ReOpenableMutableHashTable<>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor,
				this.recordBuildSideComparator, this.recordProbeSideComparator, this.pactRecordComparator,
				memSegments, ioManager, true);
		
		for (int probe = 0; probe < NUM_PROBES; probe++) {
			// create a probe input that gives 10 million pairs with 10 values sharing a key
			MutableObjectIterator<Tuple2<Integer, Integer>> probeInput = getProbeInput(NUM_KEYS, PROBE_VALS_PER_KEY, REPEATED_VALUE_1, REPEATED_VALUE_2);
			if (probe == 0) {
				join.open(buildInput, probeInput);
			} else {
				join.reopenProbe(probeInput);
			}
			Tuple2<Integer, Integer> record;
			final Tuple2<Integer, Integer> recordReuse = new Tuple2<>();

			while (join.nextRecord()) {
				long numBuildValues = 0;

				final Tuple2<Integer, Integer> probeRec = join.getCurrentProbeRecord();
				Integer key = probeRec.f0;

				HashBucketIterator<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> buildSide = join.getBuildSideIterator();
				if ((record = buildSide.next(recordReuse)) != null) {
					numBuildValues = 1;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.f0);
				}
				else {
					fail("No build side values found for a probe key.");
				}
				while ((record = buildSide.next(recordReuse)) != null) {
					numBuildValues++;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.f0);
				}

				Long contained = map.get(key);
				if (contained == null) {
					contained = numBuildValues;
				}
				else {
					contained = contained + numBuildValues;
				}

				map.put(key, contained);
			}
		}

		join.close();
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, map.size());
		for (Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();

			if( key == REPEATED_VALUE_1 || key == REPEATED_VALUE_2) {
				Assert.assertEquals("Wrong number of values in per-key cross product for key " + key,
							(PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT_PROBE) * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT_BUILD) * NUM_PROBES, val);
			} else {
				Assert.assertEquals("Wrong number of values in per-key cross product for key " + key,
							PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY * NUM_PROBES, val);
			}
		}


		// ----------------------------------------------------------------------------------------

		this.memoryManager.release(join.getFreedMemory());
	}


	static Map<Integer, Collection<TupleMatch>> deepCopy(Map<Integer, Collection<TupleMatch>> expectedSecondMatchesMap) {
		Map<Integer, Collection<TupleMatch>> copy = new HashMap<>(expectedSecondMatchesMap.size());
		for(Entry<Integer, Collection<TupleMatch>> entry : expectedSecondMatchesMap.entrySet()) {
			List<TupleMatch> matches = new ArrayList<TupleMatch>(entry.getValue().size());
			for(TupleMatch m : entry.getValue()) {
				matches.add(m);
			}
			copy.put(entry.getKey(), matches);
		}
		return copy;
	}
	
}
