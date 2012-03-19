/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.hash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.NullKeyFieldException;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.hash.BuildFirstHashMatchIterator;
import eu.stratosphere.pact.runtime.hash.MutableHashTable;
import eu.stratosphere.pact.runtime.hash.MutableHashTable.HashBucketIterator;
import eu.stratosphere.pact.runtime.plugable.PactRecordAccessors;
import eu.stratosphere.pact.runtime.plugable.TypeAccessors;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;
import eu.stratosphere.pact.runtime.test.util.UniformIntPairGenerator;
import eu.stratosphere.pact.runtime.test.util.UnionIterator;
import eu.stratosphere.pact.runtime.test.util.types.IntPair;
import eu.stratosphere.pact.runtime.test.util.types.IntPairAccessors;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 *
 * @author Stephan Ewen
 */
public class HashTableITCase
{
	private static final AbstractInvokable MEM_OWNER = new DummyInvokable();
	
	private TypeAccessors<PactRecord> recordBuildSideAccesssor;
	private TypeAccessors<PactRecord> recordProbeSideAccesssor;
	private TypeComparator<PactRecord, PactRecord> pactRecordComparator;
	
	private TypeAccessors<IntPair> pairBuildSideAccesssor;
	private TypeAccessors<IntPair> pairProbeSideAccesssor;
	private TypeComparator<IntPair, IntPair> pairComparator;
	
	@Before
	public void setup()
	{
		final int[] keyPos = new int[] {0};
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyType = (Class<? extends Key>[]) new Class[] { PactInteger.class };
		
		this.recordBuildSideAccesssor = new PactRecordAccessors(keyPos, keyType);
		this.recordProbeSideAccesssor = new PactRecordAccessors(keyPos, keyType);
		this.pactRecordComparator = new PactRecordComparator();
		
		this.pairBuildSideAccesssor = new IntPairAccessors();
		this.pairProbeSideAccesssor = new IntPairAccessors();
		this.pairComparator = new IntPairComparator();
	}
	
	@Test
	public void testIOBufferCountComputation()
	{
		assertEquals(1, MutableHashTable.getNumWriteBehindBuffers(32));
		assertEquals(1, MutableHashTable.getNumWriteBehindBuffers(33));
		assertEquals(1, MutableHashTable.getNumWriteBehindBuffers(40));
		assertEquals(1, MutableHashTable.getNumWriteBehindBuffers(64));
		assertEquals(1, MutableHashTable.getNumWriteBehindBuffers(127));
		assertEquals(2, MutableHashTable.getNumWriteBehindBuffers(128));
		assertEquals(2, MutableHashTable.getNumWriteBehindBuffers(129));
		assertEquals(2, MutableHashTable.getNumWriteBehindBuffers(511));
		assertEquals(3, MutableHashTable.getNumWriteBehindBuffers(512));
		assertEquals(3, MutableHashTable.getNumWriteBehindBuffers(513));
		assertEquals(3, MutableHashTable.getNumWriteBehindBuffers(2047));
		assertEquals(4, MutableHashTable.getNumWriteBehindBuffers(2048));
		assertEquals(4, MutableHashTable.getNumWriteBehindBuffers(2049));
		assertEquals(4, MutableHashTable.getNumWriteBehindBuffers(8191));
		assertEquals(5, MutableHashTable.getNumWriteBehindBuffers(8192));
		assertEquals(5, MutableHashTable.getNumWriteBehindBuffers(8193));
		assertEquals(5, MutableHashTable.getNumWriteBehindBuffers(32767));
		assertEquals(6, MutableHashTable.getNumWriteBehindBuffers(32768));
		assertEquals(6, MutableHashTable.getNumWriteBehindBuffers(Integer.MAX_VALUE));
	}
	
	@Test
	public void testInMemoryMutableHashTable() throws IOException
	{
		final int NUM_KEYS = 100000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<PactRecord> buildInput = new UniformPactRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<PactRecord> probeInput = new UniformPactRecordGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocateStrict(MEM_OWNER, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		final IOManager ioManager = new IOManager();
		
		// ----------------------------------------------------------------------------------------
		
		final MutableHashTable<PactRecord, PactRecord> join = new MutableHashTable<PactRecord, PactRecord>(
			this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, this.pactRecordComparator,
			memSegments, ioManager);
		join.open(buildInput, probeInput);
		
		final PactRecord record = new PactRecord();
		int numRecordsInJoinResult = 0;
		
		while (join.nextRecord()) {
			HashBucketIterator<PactRecord, PactRecord> buildSide = join.getBuildSideIterator();
			while (buildSide.next(record)) {
				numRecordsInJoinResult++;
			}
		}
		Assert.assertEquals("Wrong number of records in join result.", NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY, numRecordsInJoinResult);
		
		join.close();
		
		
		// ----------------------------------------------------------------------------------------
		
		memMan.release(memSegments);
		
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			fail("I/O manager was not property shut down.");
		}
		if (!memMan.verifyEmpty()) {
			fail("Not all memory was properly released to the memory manager --> Memory Leak.");
		}
	}
	
	@Test
	public void testSpillingHashJoinOneRecursionPerformance() throws IOException
	{
		final int NUM_KEYS = 1000000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<PactRecord> buildInput = new UniformPactRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<PactRecord> probeInput = new UniformPactRecordGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocateStrict(MEM_OWNER, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		IOManager ioManager = new IOManager();
		
		// ----------------------------------------------------------------------------------------
		
		final MutableHashTable<PactRecord, PactRecord> join = new MutableHashTable<PactRecord, PactRecord>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, this.pactRecordComparator,
				memSegments, ioManager);
		join.open(buildInput, probeInput);
		
		final PactRecord record = new PactRecord();
		int numRecordsInJoinResult = 0;
		
		while (join.nextRecord()) {
			HashBucketIterator<PactRecord, PactRecord> buildSide = join.getBuildSideIterator();
			while (buildSide.next(record)) {
				numRecordsInJoinResult++;
			}
		}
		Assert.assertEquals("Wrong number of records in join result.", NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY, numRecordsInJoinResult);
		
		join.close();
		
		// ----------------------------------------------------------------------------------------
		
		memMan.release(memSegments);
		
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			fail("I/O manager was not property shut down.");
		}
		if (!memMan.verifyEmpty()) {
			fail("Not all memory was properly released to the memory manager --> Memory Leak.");
		}
	}
	
	@Test
	public void testSpillingHashJoinOneRecursionValidity() throws IOException
	{
		final int NUM_KEYS = 1000000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<PactRecord> buildInput = new UniformPactRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<PactRecord> probeInput = new UniformPactRecordGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocateStrict(MEM_OWNER, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		IOManager ioManager = new IOManager();
		
		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);
		
		// ----------------------------------------------------------------------------------------
		
		final MutableHashTable<PactRecord, PactRecord> join = new MutableHashTable<PactRecord, PactRecord>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, this.pactRecordComparator,
				memSegments, ioManager);
		join.open(buildInput, probeInput);
	
		final PactRecord record = new PactRecord();
		
		while (join.nextRecord())
		{
			int numBuildValues = 0;
			
			int key = 0;
			
			HashBucketIterator<PactRecord, PactRecord> buildSide = join.getBuildSideIterator();
			if (buildSide.next(record)) {
				numBuildValues = 1;
				key = record.getField(0, PactInteger.class).getValue();
			}
			else {
				fail("No build side values found for a probe key.");
			}
			while (buildSide.next(record)) {
				numBuildValues++;
			}
			
			if (numBuildValues != 3) {
				fail("Other than 3 build values!!!");
			}
			
			PactRecord pr = join.getCurrentProbeRecord();
			Assert.assertEquals("Probe-side key was different than build-side key.", key, pr.getField(0, PactInteger.class).getValue()); 
			
			Long contained = map.get(key);
			if (contained == null) {
				contained = new Long(numBuildValues);
			}
			else {
				contained = new Long(contained.longValue() + (numBuildValues));
			}
			
			map.put(key, contained);
		}
		
		join.close();
		
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();
	
			Assert.assertEquals("Wrong number of values in per-key cross product for key " + key, 
				PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY, val);
		}
		
		
		// ----------------------------------------------------------------------------------------
		
		memMan.release(memSegments);
		
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			fail("I/O manager was not property shut down.");
		}
		if (!memMan.verifyEmpty()) {
			fail("Not all memory was properly released to the memory manager --> Memory Leak.");
		}
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
		MutableObjectIterator<PactRecord> build1 = new UniformPactRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<PactRecord> build2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<PactRecord> build3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<PactRecord>> builds = new ArrayList<MutableObjectIterator<PactRecord>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<PactRecord> buildInput = new UnionIterator<PactRecord>(builds);
	
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<PactRecord> probe1 = new UniformPactRecordGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
		MutableObjectIterator<PactRecord> probe2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, 5);
		MutableObjectIterator<PactRecord> probe3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, 5);
		List<MutableObjectIterator<PactRecord>> probes = new ArrayList<MutableObjectIterator<PactRecord>>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		MutableObjectIterator<PactRecord> probeInput = new UnionIterator<PactRecord>(probes);

		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocateStrict(MEM_OWNER, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		IOManager ioManager = new IOManager();
		
		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);
		
		// ----------------------------------------------------------------------------------------
		
		final MutableHashTable<PactRecord, PactRecord> join = new MutableHashTable<PactRecord, PactRecord>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, this.pactRecordComparator,
				memSegments, ioManager);
		join.open(buildInput, probeInput);
	
		final PactRecord record = new PactRecord();
		
		while (join.nextRecord())
		{	
			int numBuildValues = 0;
	
			final PactRecord probeRec = join.getCurrentProbeRecord();
			int key = probeRec.getField(0, PactInteger.class).getValue();
			
			HashBucketIterator<PactRecord, PactRecord> buildSide = join.getBuildSideIterator();
			if (buildSide.next(record)) {
				numBuildValues = 1;
				Assert.assertEquals("Probe-side key was different than build-side key.", key, record.getField(0, PactInteger.class).getValue()); 
			}
			else {
				fail("No build side values found for a probe key.");
			}
			while (buildSide.next(record)) {
				numBuildValues++;
				Assert.assertEquals("Probe-side key was different than build-side key.", key, record.getField(0, PactInteger.class).getValue());
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
		
		join.close();
		
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();
	
			Assert.assertEquals("Wrong number of values in per-key cross product for key " + key, 
				(key == REPEATED_VALUE_1 || key == REPEATED_VALUE_2) ?
					(PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT_PROBE) * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT_BUILD) : 
					 PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY, val);
		}
		
		
		// ----------------------------------------------------------------------------------------
		
		memMan.release(memSegments);
		
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			fail("I/O manager was not property shut down.");
		}
		if (!memMan.verifyEmpty()) {
			fail("Not all memory was properly released to the memory manager --> Memory Leak.");
		}
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
		MutableObjectIterator<PactRecord> build1 = new UniformPactRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<PactRecord> build2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<PactRecord> build3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<PactRecord>> builds = new ArrayList<MutableObjectIterator<PactRecord>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<PactRecord> buildInput = new UnionIterator<PactRecord>(builds);
	
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<PactRecord> probe1 = new UniformPactRecordGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
		MutableObjectIterator<PactRecord> probe2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, 5);
		MutableObjectIterator<PactRecord> probe3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, 5);
		List<MutableObjectIterator<PactRecord>> probes = new ArrayList<MutableObjectIterator<PactRecord>>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		MutableObjectIterator<PactRecord> probeInput = new UnionIterator<PactRecord>(probes);

		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocateStrict(MEM_OWNER, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		IOManager ioManager = new IOManager();
		
		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);
		
		// ----------------------------------------------------------------------------------------
		
		final MutableHashTable<PactRecord, PactRecord> join = new MutableHashTable<PactRecord, PactRecord>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, this.pactRecordComparator,
				memSegments, ioManager);
		join.open(buildInput, probeInput);
		
		final PactRecord record = new PactRecord();
		
		while (join.nextRecord())
		{	
			int numBuildValues = 0;
			
			final PactRecord probeRec = join.getCurrentProbeRecord();
			int key = probeRec.getField(0, PactInteger.class).getValue();
			
			HashBucketIterator<PactRecord, PactRecord> buildSide = join.getBuildSideIterator();
			if (buildSide.next(record)) {
				numBuildValues = 1;
				Assert.assertEquals("Probe-side key was different than build-side key.", key, record.getField(0, PactInteger.class).getValue()); 
			}
			else {
				fail("No build side values found for a probe key.");
			}
			while (buildSide.next(record)) {
				numBuildValues++;
				Assert.assertEquals("Probe-side key was different than build-side key.", key, record.getField(0, PactInteger.class).getValue());
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
		
		join.close();
		
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();
	
			Assert.assertEquals("Wrong number of values in per-key cross product for key " + key, 
				(key == REPEATED_VALUE_1 || key == REPEATED_VALUE_2) ?
					(PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT_PROBE) * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT_BUILD) : 
					 PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY, val);
		}
		
		
		// ----------------------------------------------------------------------------------------
		
		memMan.release(memSegments);
		
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			fail("I/O manager was not property shut down.");
		}
		if (!memMan.verifyEmpty()) {
			fail("Not all memory was properly released to the memory manager --> Memory Leak.");
		}
	}
	
	/*
	 * This test is basically identical to the "testSpillingHashJoinWithMassiveCollisions" test, only that the number
	 * of repeated values (causing bucket collisions) are large enough to make sure that their target partition no longer
	 * fits into memory by itself and needs to be repartitioned in the recursion again.
	 */
	@Test
	public void testFailingHashJoinTooManyRecursions() throws IOException
	{
		// the following two values are known to have a hash-code collision on the first recursion level.
		// we use them to make sure one partition grows over-proportionally large
		final int REPEATED_VALUE_1 = 40559;
		final int REPEATED_VALUE_2 = 92882;
		final int REPEATED_VALUE_COUNT = 3000000; 
		
		final int NUM_KEYS = 1000000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		MutableObjectIterator<PactRecord> build1 = new UniformPactRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<PactRecord> build2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
		MutableObjectIterator<PactRecord> build3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
		List<MutableObjectIterator<PactRecord>> builds = new ArrayList<MutableObjectIterator<PactRecord>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<PactRecord> buildInput = new UnionIterator<PactRecord>(builds);
	
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<PactRecord> probe1 = new UniformPactRecordGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
		MutableObjectIterator<PactRecord> probe2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
		MutableObjectIterator<PactRecord> probe3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
		List<MutableObjectIterator<PactRecord>> probes = new ArrayList<MutableObjectIterator<PactRecord>>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		MutableObjectIterator<PactRecord> probeInput = new UnionIterator<PactRecord>(probes);
		
		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocateStrict(MEM_OWNER, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		IOManager ioManager = new IOManager();
		
		// ----------------------------------------------------------------------------------------
		
		final MutableHashTable<PactRecord, PactRecord> join = new MutableHashTable<PactRecord, PactRecord>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, this.pactRecordComparator,
				memSegments, ioManager);
		join.open(buildInput, probeInput);
		
		final PactRecord record = new PactRecord();
		
		try {
			while (join.nextRecord())
			{	
				HashBucketIterator<PactRecord, PactRecord> buildSide = join.getBuildSideIterator();
				if (!buildSide.next(record)) {
					fail("No build side values found for a probe key.");
				}
				while (buildSide.next(record));
			}
			
			fail("Hash Join must have failed due to too many recursions.");
		}
		catch (Exception ex) {
			// expected
		}
		
		join.close();
		
		
		// ----------------------------------------------------------------------------------------
		
		memMan.release(memSegments);
		
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			fail("I/O manager was not property shut down.");
		}
		if (!memMan.verifyEmpty()) {
			fail("Not all memory was properly released to the memory manager --> Memory Leak.");
		}
	}
	
	/*
	 * Spills build records, so that probe records are also spilled. But only so
	 * few probe records are used that some partitions remain empty.
	 */
	@Test
	public void testSparseProbeSpilling() throws IOException, MemoryAllocationException
	{
		int reqMem = 4 * 1024 * 1024;

		final int NUM_BUILD_KEYS = 1000000;
		final int NUM_BUILD_VALS = 1;
		final int NUM_PROBE_KEYS = 20;
		final int NUM_PROBE_VALS = 1;

		MutableObjectIterator<PactRecord> buildInput = new UniformPactRecordGenerator(
				NUM_BUILD_KEYS, NUM_BUILD_VALS, false);

		// allocate the memory for the HashTable
		MemoryManager memMan;
		List<MemorySegment> memSegments;
		try {
			memMan = new DefaultMemoryManager(reqMem);
			memSegments = memMan.allocate(MEM_OWNER, reqMem, 64, 32 * 1024);
		} catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}

		// I/O manager should be unnecessary
		IOManager ioManager = new IOManager();

		final MutableHashTable<PactRecord, PactRecord> join = new MutableHashTable<PactRecord, PactRecord>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, this.pactRecordComparator,
				memSegments, ioManager);
		join.open(buildInput, new UniformPactRecordGenerator(NUM_PROBE_KEYS, NUM_PROBE_VALS, true));

		int expectedNumResults = (Math.min(NUM_PROBE_KEYS, NUM_BUILD_KEYS) * NUM_BUILD_VALS)
				* NUM_PROBE_VALS;

		final PactRecord record = new PactRecord();
		int numRecordsInJoinResult = 0;
		
		while (join.nextRecord()) {
			HashBucketIterator<PactRecord, PactRecord> buildSide = join.getBuildSideIterator();
			while (buildSide.next(record)) {
				numRecordsInJoinResult++;
			}
		}
		Assert.assertEquals("Wrong number of records in join result.", expectedNumResults, numRecordsInJoinResult);

		join.close();
	}
	
	/*
	 * This test validates a bug fix against former memory loss in the case where a partition was spilled
	 * during an insert into the same.
	 */
	@Test
	public void validateSpillingDuringInsertion() throws IOException, MemoryAllocationException {
		int reqMem = 2785280;
	
		final int NUM_BUILD_KEYS = 500000;
		final int NUM_BUILD_VALS = 1;
		final int NUM_PROBE_KEYS = 10;
		final int NUM_PROBE_VALS = 1;
		
		MutableObjectIterator<PactRecord> buildInput = new UniformPactRecordGenerator(NUM_BUILD_KEYS, NUM_BUILD_VALS, false);
		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		try {
			memMan = new DefaultMemoryManager(reqMem);
			long memoryAmount = reqMem & ~(((long) BuildFirstHashMatchIterator.HASH_JOIN_PAGE_SIZE) - 1);
			// NOTE: This calculation is erroneous if the total memory is above 63 TiBytes. 
			final int numPages = (int) (memoryAmount / BuildFirstHashMatchIterator.HASH_JOIN_PAGE_SIZE);
			memSegments = memMan.allocateStrict(MEM_OWNER, numPages, BuildFirstHashMatchIterator.HASH_JOIN_PAGE_SIZE);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}		
		
		IOManager ioManager = new IOManager();
				
		final MutableHashTable<PactRecord, PactRecord> join = new MutableHashTable<PactRecord, PactRecord>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, this.pactRecordComparator,
				memSegments, ioManager);
		join.open(buildInput, new UniformPactRecordGenerator(NUM_PROBE_KEYS, NUM_PROBE_VALS, true));
		
		final PactRecord record = new PactRecord();
		int numRecordsInJoinResult = 0;
		
		int expectedNumResults = (Math.min(NUM_PROBE_KEYS, NUM_BUILD_KEYS) * NUM_BUILD_VALS)
		* NUM_PROBE_VALS;
		
		while (join.nextRecord()) {
			HashBucketIterator<PactRecord, PactRecord> buildSide = join.getBuildSideIterator();
			while (buildSide.next(record)) {
				numRecordsInJoinResult++;
			}
		}
		Assert.assertEquals("Wrong number of records in join result.", expectedNumResults, numRecordsInJoinResult);
		
		join.close();
	}
	
	// ============================================================================================
	//                                 Integer Pairs based Tests
	// ============================================================================================
	
	
	@Test
	public void testInMemoryMutableHashTableIntPair() throws IOException
	{
		final int NUM_KEYS = 100000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<IntPair> buildInput = new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<IntPair> probeInput = new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocateStrict(MEM_OWNER, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		final IOManager ioManager = new IOManager();
		
		// ----------------------------------------------------------------------------------------
		
		final MutableHashTable<IntPair, IntPair> join = new MutableHashTable<IntPair, IntPair>(
			this.pairBuildSideAccesssor, this.pairProbeSideAccesssor, this.pairComparator,
			memSegments, ioManager);
		join.open(buildInput, probeInput);
		
		final IntPair record = new IntPair();
		int numRecordsInJoinResult = 0;
		
		while (join.nextRecord()) {
			HashBucketIterator<IntPair, IntPair> buildSide = join.getBuildSideIterator();
			while (buildSide.next(record)) {
				numRecordsInJoinResult++;
			}
		}
		Assert.assertEquals("Wrong number of records in join result.", NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY, numRecordsInJoinResult);
		
		join.close();
		
		
		// ----------------------------------------------------------------------------------------
		
		memMan.release(memSegments);
		
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			fail("I/O manager was not property shut down.");
		}
		if (!memMan.verifyEmpty()) {
			fail("Not all memory was properly released to the memory manager --> Memory Leak.");
		}
	}
	
	@Test
	public void testSpillingHashJoinOneRecursionPerformanceIntPair() throws IOException
	{
		final int NUM_KEYS = 1000000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<IntPair> buildInput = new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<IntPair> probeInput = new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocateStrict(MEM_OWNER, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		IOManager ioManager = new IOManager();
		
		// ----------------------------------------------------------------------------------------
		
		final MutableHashTable<IntPair, IntPair> join = new MutableHashTable<IntPair, IntPair>(
				this.pairBuildSideAccesssor, this.pairProbeSideAccesssor, this.pairComparator,
				memSegments, ioManager);
		join.open(buildInput, probeInput);
		
		final IntPair record = new IntPair();
		int numRecordsInJoinResult = 0;
		
		while (join.nextRecord()) {
			HashBucketIterator<IntPair, IntPair> buildSide = join.getBuildSideIterator();
			while (buildSide.next(record)) {
				numRecordsInJoinResult++;
			}
		}
		Assert.assertEquals("Wrong number of records in join result.", NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY, numRecordsInJoinResult);
		
		join.close();
		
		// ----------------------------------------------------------------------------------------
		
		memMan.release(memSegments);
		
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			fail("I/O manager was not property shut down.");
		}
		if (!memMan.verifyEmpty()) {
			fail("Not all memory was properly released to the memory manager --> Memory Leak.");
		}
	}
	
	@Test
	public void testSpillingHashJoinOneRecursionValidityIntPair() throws IOException
	{
		final int NUM_KEYS = 1000000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<IntPair> buildInput = new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<IntPair> probeInput = new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocateStrict(MEM_OWNER, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		IOManager ioManager = new IOManager();
		
		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);
		
		// ----------------------------------------------------------------------------------------
		
		final MutableHashTable<IntPair, IntPair> join = new MutableHashTable<IntPair, IntPair>(
				this.pairBuildSideAccesssor, this.pairProbeSideAccesssor, this.pairComparator,
				memSegments, ioManager);
		join.open(buildInput, probeInput);
	
		final IntPair record = new IntPair();
		
		while (join.nextRecord())
		{
			int numBuildValues = 0;
			
			int key = 0;
			
			HashBucketIterator<IntPair, IntPair> buildSide = join.getBuildSideIterator();
			if (buildSide.next(record)) {
				numBuildValues = 1;
				key = record.getKey();
			}
			else {
				fail("No build side values found for a probe key.");
			}
			while (buildSide.next(record)) {
				numBuildValues++;
			}
			
			if (numBuildValues != 3) {
				fail("Other than 3 build values!!!");
			}
			
			IntPair pr = join.getCurrentProbeRecord();
			Assert.assertEquals("Probe-side key was different than build-side key.", key, pr.getKey()); 
			
			Long contained = map.get(key);
			if (contained == null) {
				contained = new Long(numBuildValues);
			}
			else {
				contained = new Long(contained.longValue() + (numBuildValues));
			}
			
			map.put(key, contained);
		}
		
		join.close();
		
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();
	
			Assert.assertEquals("Wrong number of values in per-key cross product for key " + key, 
				PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY, val);
		}
		
		
		// ----------------------------------------------------------------------------------------
		
		memMan.release(memSegments);
		
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			fail("I/O manager was not property shut down.");
		}
		if (!memMan.verifyEmpty()) {
			fail("Not all memory was properly released to the memory manager --> Memory Leak.");
		}
	}
	

	@Test
	public void testSpillingHashJoinWithMassiveCollisionsIntPair() throws IOException
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
		MutableObjectIterator<IntPair> build1 = new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<IntPair> build2 = new ConstantsIntPairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<IntPair> build3 = new ConstantsIntPairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<IntPair>> builds = new ArrayList<MutableObjectIterator<IntPair>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<IntPair> buildInput = new UnionIterator<IntPair>(builds);
	
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<IntPair> probe1 = new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
		MutableObjectIterator<IntPair> probe2 = new ConstantsIntPairsIterator(REPEATED_VALUE_1, 17, 5);
		MutableObjectIterator<IntPair> probe3 = new ConstantsIntPairsIterator(REPEATED_VALUE_2, 23, 5);
		List<MutableObjectIterator<IntPair>> probes = new ArrayList<MutableObjectIterator<IntPair>>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		MutableObjectIterator<IntPair> probeInput = new UnionIterator<IntPair>(probes);

		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocateStrict(MEM_OWNER, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		IOManager ioManager = new IOManager();
		
		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);
		
		// ----------------------------------------------------------------------------------------
		
		final MutableHashTable<IntPair, IntPair> join = new MutableHashTable<IntPair, IntPair>(
				this.pairBuildSideAccesssor, this.pairProbeSideAccesssor, this.pairComparator,
				memSegments, ioManager);
		join.open(buildInput, probeInput);
	
		final IntPair record = new IntPair();
		
		while (join.nextRecord())
		{	
			int numBuildValues = 0;
	
			final IntPair probeRec = join.getCurrentProbeRecord();
			int key = probeRec.getKey();
			
			HashBucketIterator<IntPair, IntPair> buildSide = join.getBuildSideIterator();
			if (buildSide.next(record)) {
				numBuildValues = 1;
				Assert.assertEquals("Probe-side key was different than build-side key.", key, record.getKey()); 
			}
			else {
				fail("No build side values found for a probe key.");
			}
			while (buildSide.next(record)) {
				numBuildValues++;
				Assert.assertEquals("Probe-side key was different than build-side key.", key, record.getKey());
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
		
		join.close();
		
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();
	
			Assert.assertEquals("Wrong number of values in per-key cross product for key " + key, 
				(key == REPEATED_VALUE_1 || key == REPEATED_VALUE_2) ?
					(PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT_PROBE) * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT_BUILD) : 
					 PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY, val);
		}
		
		
		// ----------------------------------------------------------------------------------------
		
		memMan.release(memSegments);
		
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			fail("I/O manager was not property shut down.");
		}
		if (!memMan.verifyEmpty()) {
			fail("Not all memory was properly released to the memory manager --> Memory Leak.");
		}
	}
	
	/*
	 * This test is basically identical to the "testSpillingHashJoinWithMassiveCollisions" test, only that the number
	 * of repeated values (causing bucket collisions) are large enough to make sure that their target partition no longer
	 * fits into memory by itself and needs to be repartitioned in the recursion again.
	 */
	@Test
	public void testSpillingHashJoinWithTwoRecursionsIntPair() throws IOException
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
		MutableObjectIterator<IntPair> build1 = new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<IntPair> build2 = new ConstantsIntPairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<IntPair> build3 = new ConstantsIntPairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<IntPair>> builds = new ArrayList<MutableObjectIterator<IntPair>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<IntPair> buildInput = new UnionIterator<IntPair>(builds);
	
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<IntPair> probe1 = new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
		MutableObjectIterator<IntPair> probe2 = new ConstantsIntPairsIterator(REPEATED_VALUE_1, 17, 5);
		MutableObjectIterator<IntPair> probe3 = new ConstantsIntPairsIterator(REPEATED_VALUE_2, 23, 5);
		List<MutableObjectIterator<IntPair>> probes = new ArrayList<MutableObjectIterator<IntPair>>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		MutableObjectIterator<IntPair> probeInput = new UnionIterator<IntPair>(probes);

		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocateStrict(MEM_OWNER, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		IOManager ioManager = new IOManager();
		
		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);
		
		// ----------------------------------------------------------------------------------------
		
		final MutableHashTable<IntPair, IntPair> join = new MutableHashTable<IntPair, IntPair>(
				this.pairBuildSideAccesssor, this.pairProbeSideAccesssor, this.pairComparator,
				memSegments, ioManager);
		join.open(buildInput, probeInput);
		
		final IntPair record = new IntPair();
		
		while (join.nextRecord())
		{	
			int numBuildValues = 0;
			
			final IntPair probeRec = join.getCurrentProbeRecord();
			int key = probeRec.getKey();
			
			HashBucketIterator<IntPair, IntPair> buildSide = join.getBuildSideIterator();
			if (buildSide.next(record)) {
				numBuildValues = 1;
				Assert.assertEquals("Probe-side key was different than build-side key.", key, record.getKey()); 
			}
			else {
				fail("No build side values found for a probe key.");
			}
			while (buildSide.next(record)) {
				numBuildValues++;
				Assert.assertEquals("Probe-side key was different than build-side key.", key, record.getKey());
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
		
		join.close();
		
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();
	
			Assert.assertEquals("Wrong number of values in per-key cross product for key " + key, 
				(key == REPEATED_VALUE_1 || key == REPEATED_VALUE_2) ?
					(PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT_PROBE) * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT_BUILD) : 
					 PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY, val);
		}
		
		
		// ----------------------------------------------------------------------------------------
		
		memMan.release(memSegments);
		
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			fail("I/O manager was not property shut down.");
		}
		if (!memMan.verifyEmpty()) {
			fail("Not all memory was properly released to the memory manager --> Memory Leak.");
		}
	}
	
	/*
	 * This test is basically identical to the "testSpillingHashJoinWithMassiveCollisions" test, only that the number
	 * of repeated values (causing bucket collisions) are large enough to make sure that their target partition no longer
	 * fits into memory by itself and needs to be repartitioned in the recursion again.
	 */
	@Test
	public void testFailingHashJoinTooManyRecursionsIntPair() throws IOException
	{
		// the following two values are known to have a hash-code collision on the first recursion level.
		// we use them to make sure one partition grows over-proportionally large
		final int REPEATED_VALUE_1 = 40559;
		final int REPEATED_VALUE_2 = 92882;
		final int REPEATED_VALUE_COUNT = 3000000; 
		
		final int NUM_KEYS = 1000000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		MutableObjectIterator<IntPair> build1 = new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<IntPair> build2 = new ConstantsIntPairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
		MutableObjectIterator<IntPair> build3 = new ConstantsIntPairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
		List<MutableObjectIterator<IntPair>> builds = new ArrayList<MutableObjectIterator<IntPair>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<IntPair> buildInput = new UnionIterator<IntPair>(builds);
	
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<IntPair> probe1 = new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
		MutableObjectIterator<IntPair> probe2 = new ConstantsIntPairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
		MutableObjectIterator<IntPair> probe3 = new ConstantsIntPairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
		List<MutableObjectIterator<IntPair>> probes = new ArrayList<MutableObjectIterator<IntPair>>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		MutableObjectIterator<IntPair> probeInput = new UnionIterator<IntPair>(probes);
		
		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocateStrict(MEM_OWNER, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		IOManager ioManager = new IOManager();
		
		// ----------------------------------------------------------------------------------------
		
		final MutableHashTable<IntPair, IntPair> join = new MutableHashTable<IntPair, IntPair>(
				this.pairBuildSideAccesssor, this.pairProbeSideAccesssor, this.pairComparator,
				memSegments, ioManager);
		join.open(buildInput, probeInput);
		
		final IntPair record = new IntPair();
		
		try {
			while (join.nextRecord())
			{	
				HashBucketIterator<IntPair, IntPair> buildSide = join.getBuildSideIterator();
				if (!buildSide.next(record)) {
					fail("No build side values found for a probe key.");
				}
				while (buildSide.next(record));
			}
			
			fail("Hash Join must have failed due to too many recursions.");
		}
		catch (Exception ex) {
			// expected
		}
		
		join.close();
		
		
		// ----------------------------------------------------------------------------------------
		
		memMan.release(memSegments);
		
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			fail("I/O manager was not property shut down.");
		}
		if (!memMan.verifyEmpty()) {
			fail("Not all memory was properly released to the memory manager --> Memory Leak.");
		}
	}
	
	/*
	 * Spills build records, so that probe records are also spilled. But only so
	 * few probe records are used that some partitions remain empty.
	 */
	@Test
	public void testSparseProbeSpillingIntPair() throws IOException, MemoryAllocationException
	{
		int reqMem = 4 * 1024 * 1024;

		final int NUM_BUILD_KEYS = 1000000;
		final int NUM_BUILD_VALS = 1;
		final int NUM_PROBE_KEYS = 20;
		final int NUM_PROBE_VALS = 1;

		MutableObjectIterator<IntPair> buildInput = new UniformIntPairGenerator(NUM_BUILD_KEYS, NUM_BUILD_VALS, false);

		// allocate the memory for the HashTable
		MemoryManager memMan;
		List<MemorySegment> memSegments;
		try {
			memMan = new DefaultMemoryManager(reqMem);
			memSegments = memMan.allocate(MEM_OWNER, reqMem, 64, 32 * 1024);
		} catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}

		// I/O manager should be unnecessary
		IOManager ioManager = new IOManager();

		final MutableHashTable<IntPair, IntPair> join = new MutableHashTable<IntPair, IntPair>(
				this.pairBuildSideAccesssor, this.pairProbeSideAccesssor, this.pairComparator,
				memSegments, ioManager);
		join.open(buildInput, new UniformIntPairGenerator(NUM_PROBE_KEYS, NUM_PROBE_VALS, true));

		int expectedNumResults = (Math.min(NUM_PROBE_KEYS, NUM_BUILD_KEYS) * NUM_BUILD_VALS)
				* NUM_PROBE_VALS;

		final IntPair record = new IntPair();
		int numRecordsInJoinResult = 0;
		
		while (join.nextRecord()) {
			HashBucketIterator<IntPair, IntPair> buildSide = join.getBuildSideIterator();
			while (buildSide.next(record)) {
				numRecordsInJoinResult++;
			}
		}
		Assert.assertEquals("Wrong number of records in join result.", expectedNumResults, numRecordsInJoinResult);

		join.close();
	}
	
	/*
	 * This test validates a bug fix against former memory loss in the case where a partition was spilled
	 * during an insert into the same.
	 */
	@Test
	public void validateSpillingDuringInsertionIntPair() throws IOException, MemoryAllocationException {
		int reqMem = 2785280;

		final int NUM_BUILD_KEYS = 500000;
		final int NUM_BUILD_VALS = 1;
		final int NUM_PROBE_KEYS = 10;
		final int NUM_PROBE_VALS = 1;
		
		MutableObjectIterator<IntPair> buildInput = new UniformIntPairGenerator(NUM_BUILD_KEYS, NUM_BUILD_VALS, false);
		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		try {
			memMan = new DefaultMemoryManager(reqMem);
			long memoryAmount = reqMem & ~(((long) BuildFirstHashMatchIterator.HASH_JOIN_PAGE_SIZE) - 1);
			// NOTE: This calculation is erroneous if the total memory is above 63 TiBytes. 
			final int numPages = (int) (memoryAmount / BuildFirstHashMatchIterator.HASH_JOIN_PAGE_SIZE);
			memSegments = memMan.allocateStrict(MEM_OWNER, numPages, BuildFirstHashMatchIterator.HASH_JOIN_PAGE_SIZE);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}		
		
		IOManager ioManager = new IOManager();
				
		final MutableHashTable<IntPair, IntPair> join = new MutableHashTable<IntPair, IntPair>(
				this.pairBuildSideAccesssor, this.pairProbeSideAccesssor, this.pairComparator,
				memSegments, ioManager);
		join.open(buildInput, new UniformIntPairGenerator(NUM_PROBE_KEYS, NUM_PROBE_VALS, true));
		
		final IntPair record = new IntPair();
		int numRecordsInJoinResult = 0;
		
		int expectedNumResults = (Math.min(NUM_PROBE_KEYS, NUM_BUILD_KEYS) * NUM_BUILD_VALS)
		* NUM_PROBE_VALS;
		
		while (join.nextRecord()) {
			HashBucketIterator<IntPair, IntPair> buildSide = join.getBuildSideIterator();
			while (buildSide.next(record)) {
				numRecordsInJoinResult++;
			}
		}
		Assert.assertEquals("Wrong number of records in join result.", expectedNumResults, numRecordsInJoinResult);
		
		join.close();
	}
	
	@Test
	public void testInMemoryReOpen() throws IOException
	{
		final int NUM_KEYS = 100000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<IntPair> buildInput = new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<IntPair> probeInput = new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocateStrict(MEM_OWNER, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		final IOManager ioManager = new IOManager();
		
		// ----------------------------------------------------------------------------------------
		
		final MutableHashTable<IntPair, IntPair> join = new MutableHashTable<IntPair, IntPair>(
			this.pairBuildSideAccesssor, this.pairProbeSideAccesssor, this.pairComparator,
			memSegments, ioManager);
		join.open(buildInput, probeInput);
		
		final IntPair record = new IntPair();
		int numRecordsInJoinResult = 0;
		
		while (join.nextRecord()) {
			HashBucketIterator<IntPair, IntPair> buildSide = join.getBuildSideIterator();
			while (buildSide.next(record)) {
				numRecordsInJoinResult++;
			}
		}
		Assert.assertEquals("Wrong number of records in join result.", NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY, numRecordsInJoinResult);
		
		join.close();

		// ----------------------------------------------------------------------------------------
		// recreate the inputs
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		buildInput = new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		probeInput = new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

		join.open(buildInput, probeInput);
		
		numRecordsInJoinResult = 0;
		
		while (join.nextRecord()) {
			HashBucketIterator<IntPair, IntPair> buildSide = join.getBuildSideIterator();
			while (buildSide.next(record)) {
				numRecordsInJoinResult++;
			}
		}
		Assert.assertEquals("Wrong number of records in join result.", NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY, numRecordsInJoinResult);
		
		join.close();
		
		// ----------------------------------------------------------------------------------------
		
		memMan.release(memSegments);
		
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			fail("I/O manager was not property shut down.");
		}
		if (!memMan.verifyEmpty()) {
			fail("Not all memory was properly released to the memory manager --> Memory Leak.");
		}
	}
	
	// ============================================================================================
	
	/**
	 * An iterator that returns the Key/Value pairs with identical value a given number of times.
	 */
	private static final class ConstantsKeyValuePairsIterator implements MutableObjectIterator<PactRecord>
	{
		private final PactInteger key;
		private final PactInteger value;
		
		private int numLeft;
		
		public ConstantsKeyValuePairsIterator(int key, int value, int count)
		{
			this.key = new PactInteger(key);
			this.value = new PactInteger(value);
			this.numLeft = count;
		}

		@Override
		public boolean next(PactRecord target) {
			if (this.numLeft > 0) {
				this.numLeft--;
				target.clear();
				target.setField(0, this.key);
				target.setField(1, this.value);
				return true;
			}
			else {
				return false;
			}
		}
	}
	
	// ============================================================================================
	
	/**
	 * An iterator that returns the Key/Value pairs with identical value a given number of times.
	 */
	private static final class ConstantsIntPairsIterator implements MutableObjectIterator<IntPair>
	{
		private final int key;
		private final int value;
		
		private int numLeft;
		
		public ConstantsIntPairsIterator(int key, int value, int count)
		{
			this.key = key;
			this.value = value;
			this.numLeft = count;
		}

		@Override
		public boolean next(IntPair target) {
			if (this.numLeft > 0) {
				this.numLeft--;
				target.setKey(this.key);
				target.setValue(this.value);
				return true;
			}
			else {
				return false;
			}
		}
	}
	
	// ============================================================================================
	
	private static final class PactRecordComparator implements TypeComparator<PactRecord, PactRecord>
	{
		private int key;

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#setReference(java.lang.Object, eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2)
		 */
		@Override
		public void setReference(PactRecord reference) {
			try {
				this.key = reference.getField(0, PactInteger.class).getValue();
			} catch (NullPointerException npex) {
				throw new NullKeyFieldException();
			}
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#equalToReference(java.lang.Object, eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2)
		 */
		@Override
		public boolean equalToReference(PactRecord candidate) {
			try {
				return this.key == candidate.getField(0, PactInteger.class).getValue();
			} catch (NullPointerException npex) {
				throw new NullKeyFieldException();
			}
		}
	}
	
	// ============================================================================================

	
	private static final class IntPairComparator implements TypeComparator<IntPair, IntPair>
	{
		private int key;
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#setReference(java.lang.Object, eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2)
		 */
		@Override
		public void setReference(IntPair reference) {
			this.key = reference.getKey();
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#equalToReference(java.lang.Object, eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2)
		 */
		@Override
		public boolean equalToReference(IntPair candidate) {
			return this.key == candidate.getKey();
		}
		
	}
}
