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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.record.RecordComparator;
import org.apache.flink.api.common.typeutils.record.RecordPairComparator;
import org.apache.flink.api.common.typeutils.record.RecordSerializer;
import org.apache.flink.api.java.record.functions.JoinFunction;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryAllocationException;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.operators.hash.HashMatchIteratorITCase.RecordMatch;
import org.apache.flink.runtime.operators.hash.HashMatchIteratorITCase.RecordMatchRemovingJoin;
import org.apache.flink.runtime.operators.hash.HashTableITCase.ConstantsKeyValuePairsIterator;
import org.apache.flink.runtime.operators.hash.MutableHashTable.HashBucketIterator;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.runtime.operators.testutils.UnionIterator;
import org.apache.flink.runtime.operators.testutils.TestData.Generator;
import org.apache.flink.runtime.operators.testutils.TestData.Key;
import org.apache.flink.runtime.operators.testutils.TestData.Generator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.Generator.ValueMode;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test specialized hash join that keeps the build side data (in memory and on hard disk)
 * This is used for iterative tasks.
 */

public class ReOpenableHashTableITCase {
	
	private static final int PAGE_SIZE = 8 * 1024;
	private static final long MEMORY_SIZE = PAGE_SIZE * 1000; // 100 Pages.

	private static final long SEED1 = 561349061987311L;
	private static final long SEED2 = 231434613412342L;
	
	private static final int NUM_PROBES = 3; // number of reopenings of hash join
	
	private final AbstractInvokable parentTask = new DummyInvokable();

	private IOManager ioManager;
	private MemoryManager memoryManager;
	
	private TypeSerializer<Record> recordSerializer;
	private TypeComparator<Record> record1Comparator;
	private TypeComparator<Record> record2Comparator;
	private TypePairComparator<Record, Record> recordPairComparator;
	
	
	
	
	private static final AbstractInvokable MEM_OWNER = new DummyInvokable();
	private TypeSerializer<Record> recordBuildSideAccesssor;
	private TypeSerializer<Record> recordProbeSideAccesssor;
	private TypeComparator<Record> recordBuildSideComparator;
	private TypeComparator<Record> recordProbeSideComparator;
	private TypePairComparator<Record, Record> pactRecordComparator;

	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest()
	{
		this.recordSerializer = RecordSerializer.get();
		
		this.record1Comparator = new RecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
		this.record2Comparator = new RecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
		this.recordPairComparator = new RecordPairComparator(new int[] {0}, new int[] {0}, new Class[] {TestData.Key.class});
		
		
		final int[] keyPos = new int[] {0};
		final Class<? extends Key>[] keyType = (Class<? extends Key>[]) new Class[] { IntValue.class };
		
		this.recordBuildSideAccesssor = RecordSerializer.get();
		this.recordProbeSideAccesssor = RecordSerializer.get();
		this.recordBuildSideComparator = new RecordComparator(keyPos, keyType);
		this.recordProbeSideComparator = new RecordComparator(keyPos, keyType);
		this.pactRecordComparator = new HashTableITCase.RecordPairComparatorFirstInt();
		
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE,1, PAGE_SIZE);
		this.ioManager = new IOManager();
	}

	@After
	public void afterTest()
	{
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
			Generator bgen = new Generator(SEED1, 200, 1024, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
			Generator pgen = new Generator(SEED2, 0, 1024, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			
			final TestData.GeneratorIterator buildInput = new TestData.GeneratorIterator(bgen, buildSize);
			final TestData.GeneratorIterator probeInput = new TestData.GeneratorIterator(pgen, probeSize);
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
			Generator bgen = new Generator(SEED1, 0, 1024, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			Generator pgen = new Generator(SEED2, 0, 1024, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			
			final TestData.GeneratorIterator buildInput = new TestData.GeneratorIterator(bgen, buildSize);
			final TestData.GeneratorIterator probeInput = new TestData.GeneratorIterator(pgen, probeSize);
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
			Generator bgen = new Generator(SEED1, 0, 28, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			Generator pgen = new Generator(SEED2, 0, 28, KeyMode.SORTED, ValueMode.FIX_LENGTH);
			
			final TestData.GeneratorIterator buildInput = new TestData.GeneratorIterator(bgen, buildSize);
			final TestData.GeneratorIterator probeInput = new TestData.GeneratorIterator(pgen, probeSize);
			
			doTest(buildInput,probeInput, bgen, pgen);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
	
	private void doTest(TestData.GeneratorIterator buildInput, TestData.GeneratorIterator probeInput, Generator bgen, Generator pgen) throws Exception {
		// collect expected data
		final Map<TestData.Key, Collection<RecordMatch>> expectedFirstMatchesMap = HashMatchIteratorITCase.matchRecordValues(
			HashMatchIteratorITCase.collectRecordData(buildInput),
			HashMatchIteratorITCase.collectRecordData(probeInput));
		
		final List<Map<TestData.Key, Collection<RecordMatch>>> expectedNMatchesMapList = new ArrayList<Map<Key,Collection<RecordMatch>>>(NUM_PROBES);
		final JoinFunction[] nMatcher = new RecordMatchRemovingJoin[NUM_PROBES];
		for(int i = 0; i < NUM_PROBES; i++) {
			Map<TestData.Key, Collection<RecordMatch>> tmp;
			expectedNMatchesMapList.add(tmp = deepCopy(expectedFirstMatchesMap));
			nMatcher[i] = new RecordMatchRemovingJoin(tmp);
		}
		
		final JoinFunction firstMatcher = new RecordMatchRemovingJoin(expectedFirstMatchesMap);
		
		final Collector<Record> collector = new DiscardingOutputCollector<Record>();

		// reset the generators
		bgen.reset();
		pgen.reset();
		buildInput.reset();
		probeInput.reset();

		// compare with iterator values
		BuildFirstReOpenableHashMatchIterator<Record, Record, Record> iterator = 
				new BuildFirstReOpenableHashMatchIterator<Record, Record, Record>(
						buildInput, probeInput, this.recordSerializer, this.record1Comparator, 
					this.recordSerializer, this.record2Comparator, this.recordPairComparator,
					this.memoryManager, ioManager, this.parentTask, 1.0);
		
		iterator.open();
		// do first join with both inputs
		while (iterator.callWithNextKey(firstMatcher, collector));

		// assert that each expected match was seen for the first input
		for (Entry<TestData.Key, Collection<RecordMatch>> entry : expectedFirstMatchesMap.entrySet()) {
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
			for (Entry<TestData.Key, Collection<RecordMatch>> entry : expectedNMatchesMapList.get(i).entrySet()) {
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
	
	private final MutableObjectIterator<Record> getProbeInput(final int numKeys,
			final int probeValsPerKey, final int repeatedValue1, final int repeatedValue2) {
		MutableObjectIterator<Record> probe1 = new UniformRecordGenerator(numKeys, probeValsPerKey, true);
		MutableObjectIterator<Record> probe2 = new ConstantsKeyValuePairsIterator(repeatedValue1, 17, 5);
		MutableObjectIterator<Record> probe3 = new ConstantsKeyValuePairsIterator(repeatedValue2, 23, 5);
		List<MutableObjectIterator<Record>> probes = new ArrayList<MutableObjectIterator<Record>>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		return new UnionIterator<Record>(probes);
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
		MutableObjectIterator<Record> build1 = new UniformRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<Record> build2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<Record> build3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<Record>> builds = new ArrayList<MutableObjectIterator<Record>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<Record> buildInput = new UnionIterator<Record>(builds);
	
		
		

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
		
		final ReOpenableMutableHashTable<Record, Record> join = new ReOpenableMutableHashTable<Record, Record>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, 
				this.recordBuildSideComparator, this.recordProbeSideComparator, this.pactRecordComparator,
				memSegments, ioManager);
		
		for(int probe = 0; probe < NUM_PROBES; probe++) {
			// create a probe input that gives 10 million pairs with 10 values sharing a key
			MutableObjectIterator<Record> probeInput = getProbeInput(NUM_KEYS, PROBE_VALS_PER_KEY, REPEATED_VALUE_1, REPEATED_VALUE_2);
			if(probe == 0) {
				join.open(buildInput, probeInput);
			} else {
				join.reopenProbe(probeInput);
			}
		
			Record record;
			final Record recordReuse = new Record();

			while (join.nextRecord())
			{
				int numBuildValues = 0;
		
				final Record probeRec = join.getCurrentProbeRecord();
				int key = probeRec.getField(0, IntValue.class).getValue();
				
				HashBucketIterator<Record, Record> buildSide = join.getBuildSideIterator();
				if ((record = buildSide.next(recordReuse)) != null) {
					numBuildValues = 1;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.getField(0, IntValue.class).getValue()); 
				}
				else {
					fail("No build side values found for a probe key.");
				}
				while ((record = buildSide.next(record)) != null) {
					numBuildValues++;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.getField(0, IntValue.class).getValue());
				}
				
				Long contained = map.get(key);
				if (contained == null) {
					contained = new Long(numBuildValues);
				}
				else {
					contained = new Long(contained.longValue() + numBuildValues);
				}
				
				map.put(key, contained);
			}
		}
		
		join.close();
		
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
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
		MutableObjectIterator<Record> build1 = new UniformRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<Record> build2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<Record> build3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<Record>> builds = new ArrayList<MutableObjectIterator<Record>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<Record> buildInput = new UnionIterator<Record>(builds);
	

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
		
		final ReOpenableMutableHashTable<Record, Record> join = new ReOpenableMutableHashTable<Record, Record>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, 
				this.recordBuildSideComparator, this.recordProbeSideComparator, this.pactRecordComparator,
				memSegments, ioManager);
		for(int probe = 0; probe < NUM_PROBES; probe++) {
			// create a probe input that gives 10 million pairs with 10 values sharing a key
			MutableObjectIterator<Record> probeInput = getProbeInput(NUM_KEYS, PROBE_VALS_PER_KEY, REPEATED_VALUE_1, REPEATED_VALUE_2);
			if(probe == 0) {
				join.open(buildInput, probeInput);
			} else {
				join.reopenProbe(probeInput);
			}
			Record record;
			final Record recordReuse = new Record();

			while (join.nextRecord())
			{	
				int numBuildValues = 0;
				
				final Record probeRec = join.getCurrentProbeRecord();
				int key = probeRec.getField(0, IntValue.class).getValue();
				
				HashBucketIterator<Record, Record> buildSide = join.getBuildSideIterator();
				if ((record = buildSide.next(recordReuse)) != null) {
					numBuildValues = 1;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.getField(0, IntValue.class).getValue()); 
				}
				else {
					fail("No build side values found for a probe key.");
				}
				while ((record = buildSide.next(recordReuse)) != null) {
					numBuildValues++;
					Assert.assertEquals("Probe-side key was different than build-side key.", key, record.getField(0, IntValue.class).getValue());
				}
				
				Long contained = map.get(key);
				if (contained == null) {
					contained = new Long(numBuildValues);
				}
				else {
					contained = new Long(contained.longValue() + numBuildValues);
				}
				
				map.put(key, contained);
			}
		}
		
		join.close();
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
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
	
	
	static Map<Key, Collection<RecordMatch>> deepCopy(Map<Key, Collection<RecordMatch>> expectedSecondMatchesMap) {
		Map<Key, Collection<RecordMatch>> copy = new HashMap<Key, Collection<RecordMatch>>(expectedSecondMatchesMap.size());
		for(Map.Entry<Key, Collection<RecordMatch>> entry : expectedSecondMatchesMap.entrySet()) {
			List<RecordMatch> matches = new ArrayList<RecordMatch>(entry.getValue().size());
			for(RecordMatch m : entry.getValue()) {
				matches.add(m);
			}
			copy.put(entry.getKey(), matches);
		}
		return copy;
	}
	
}
