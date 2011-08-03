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
import org.junit.Test;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.hash.HashJoin.HashBucketIterator;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.UnionIterator;
import eu.stratosphere.pact.runtime.util.MutableObjectIterator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 *
 * @author Stephan Ewen
 */
public class HashJoinTest
{
	
	private static final AbstractInvokable MEM_OWNER = new DummyInvokable();
	
	@Test
	public void testIOBufferCountComputation()
	{
		assertEquals(1, HashJoin.getNumWriteBehindBuffers(32));
		assertEquals(1, HashJoin.getNumWriteBehindBuffers(33));
		assertEquals(1, HashJoin.getNumWriteBehindBuffers(40));
		assertEquals(1, HashJoin.getNumWriteBehindBuffers(64));
		assertEquals(1, HashJoin.getNumWriteBehindBuffers(127));
		assertEquals(2, HashJoin.getNumWriteBehindBuffers(128));
		assertEquals(2, HashJoin.getNumWriteBehindBuffers(129));
		assertEquals(2, HashJoin.getNumWriteBehindBuffers(511));
		assertEquals(3, HashJoin.getNumWriteBehindBuffers(512));
		assertEquals(3, HashJoin.getNumWriteBehindBuffers(513));
		assertEquals(3, HashJoin.getNumWriteBehindBuffers(2047));
		assertEquals(4, HashJoin.getNumWriteBehindBuffers(2048));
		assertEquals(4, HashJoin.getNumWriteBehindBuffers(2049));
		assertEquals(4, HashJoin.getNumWriteBehindBuffers(8191));
		assertEquals(5, HashJoin.getNumWriteBehindBuffers(8192));
		assertEquals(5, HashJoin.getNumWriteBehindBuffers(8193));
		assertEquals(5, HashJoin.getNumWriteBehindBuffers(32767));
		assertEquals(6, HashJoin.getNumWriteBehindBuffers(32768));
		assertEquals(6, HashJoin.getNumWriteBehindBuffers(Integer.MAX_VALUE));
	}
	
	@Test
	public void testLog2Computation()
	{
		assertEquals(0, HashJoin.log2floor(1));
		assertEquals(1, HashJoin.log2floor(2));
		assertEquals(1, HashJoin.log2floor(3));
		assertEquals(2, HashJoin.log2floor(4));
		assertEquals(2, HashJoin.log2floor(5));
		assertEquals(2, HashJoin.log2floor(7));
		assertEquals(3, HashJoin.log2floor(8));
		assertEquals(3, HashJoin.log2floor(9));
		assertEquals(4, HashJoin.log2floor(16));
		assertEquals(4, HashJoin.log2floor(17));
		assertEquals(13, HashJoin.log2floor((0x1 << 13) + 1));
		assertEquals(30, HashJoin.log2floor(Integer.MAX_VALUE));
		assertEquals(31, HashJoin.log2floor(-1));
		
		try {
			HashJoin.log2floor(0);
			fail();
		}
		catch (ArithmeticException aex) {}
	}
	
	
	@Test
	public void testInMemoryHashJoin() throws IOException
	{
		final int NUM_KEYS = 100000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<PactRecord> buildInput = new RegularlyGeneratedInputGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<PactRecord> probeInput = new RegularlyGeneratedInputGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocate(MEM_OWNER, 28 * 1024 * 1024, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		final IOManager ioManager = new IOManager();
		
		// ----------------------------------------------------------------------------------------
		
		@SuppressWarnings("unchecked")
		final HashJoin join = new HashJoin(buildInput, probeInput, new int[] {0}, new int[] {0}, new Class[] {PactInteger.class}, memSegments, ioManager);
		join.open();
		
		final PactRecord record = new PactRecord();
		int numRecordsInJoinResult = 0;
		
		while (join.nextRecord()) {
			HashBucketIterator buildSide = join.getBuildSideIterator();
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
		MutableObjectIterator<PactRecord> buildInput = new RegularlyGeneratedInputGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<PactRecord> probeInput = new RegularlyGeneratedInputGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocate(MEM_OWNER, 28 * 1024 * 1024, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		IOManager ioManager = new IOManager();
		
		// ----------------------------------------------------------------------------------------
		
		@SuppressWarnings("unchecked")
		final HashJoin join = new HashJoin(buildInput, probeInput, new int[] {0}, new int[] {0}, new Class[] {PactInteger.class}, memSegments, ioManager);
		join.open();
		
		final PactRecord record = new PactRecord();
		int numRecordsInJoinResult = 0;
		
		while (join.nextRecord()) {
			HashBucketIterator buildSide = join.getBuildSideIterator();
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
		MutableObjectIterator<PactRecord> buildInput = new RegularlyGeneratedInputGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<PactRecord> probeInput = new RegularlyGeneratedInputGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

		// allocate the memory for the HashTable
		MemoryManager memMan; 
		List<MemorySegment> memSegments;
		
		try {
			memMan = new DefaultMemoryManager(32 * 1024 * 1024);
			memSegments = memMan.allocate(MEM_OWNER, 28 * 1024 * 1024, 896, 32 * 1024);
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
		
		@SuppressWarnings("unchecked")
		final HashJoin join = new HashJoin(buildInput, probeInput, new int[] {0}, new int[] {0}, new Class[] {PactInteger.class}, memSegments, ioManager);
		join.open();
	
		final PactRecord record = new PactRecord();
		
		while (join.nextRecord())
		{
			int numBuildValues = 0;
			
			int key = 0;
			
			HashBucketIterator buildSide = join.getBuildSideIterator();
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
		MutableObjectIterator<PactRecord> build1 = new RegularlyGeneratedInputGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<PactRecord> build2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<PactRecord> build3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<PactRecord>> builds = new ArrayList<MutableObjectIterator<PactRecord>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<PactRecord> buildInput = new UnionIterator<PactRecord>(builds);
	
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<PactRecord> probe1 = new RegularlyGeneratedInputGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
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
			memSegments = memMan.allocate(MEM_OWNER, 28 * 1024 * 1024, 896, 32 * 1024);
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
		
		@SuppressWarnings("unchecked")
		final HashJoin join = new HashJoin(buildInput, probeInput, new int[] {0}, new int[] {0}, new Class[] {PactInteger.class}, memSegments, ioManager);
		join.open();
	
		final PactRecord record = new PactRecord();
		
		while (join.nextRecord())
		{	
			int numBuildValues = 0;
	
			final PactRecord probeRec = join.getCurrentProbeRecord();
			int key = probeRec.getField(0, PactInteger.class).getValue();
			
			HashBucketIterator buildSide = join.getBuildSideIterator();
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
		MutableObjectIterator<PactRecord> build1 = new RegularlyGeneratedInputGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<PactRecord> build2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<PactRecord> build3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<PactRecord>> builds = new ArrayList<MutableObjectIterator<PactRecord>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<PactRecord> buildInput = new UnionIterator<PactRecord>(builds);
	
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<PactRecord> probe1 = new RegularlyGeneratedInputGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
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
			memSegments = memMan.allocate(MEM_OWNER, 28 * 1024 * 1024, 896, 32 * 1024);
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
		
		@SuppressWarnings("unchecked")
		final HashJoin join = new HashJoin(buildInput, probeInput, new int[] {0}, new int[] {0}, new Class[] {PactInteger.class}, memSegments, ioManager);
		join.open();
		
		final PactRecord record = new PactRecord();
		
		while (join.nextRecord())
		{	
			int numBuildValues = 0;
			
			final PactRecord probeRec = join.getCurrentProbeRecord();
			int key = probeRec.getField(0, PactInteger.class).getValue();
			
			HashBucketIterator buildSide = join.getBuildSideIterator();
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
		MutableObjectIterator<PactRecord> build1 = new RegularlyGeneratedInputGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<PactRecord> build2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
		MutableObjectIterator<PactRecord> build3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
		List<MutableObjectIterator<PactRecord>> builds = new ArrayList<MutableObjectIterator<PactRecord>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<PactRecord> buildInput = new UnionIterator<PactRecord>(builds);
	
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<PactRecord> probe1 = new RegularlyGeneratedInputGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
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
			memSegments = memMan.allocate(MEM_OWNER, 28 * 1024 * 1024, 896, 32 * 1024);
		}
		catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return;
		}
		
		// create the I/O access for spilling
		IOManager ioManager = new IOManager();
		
		// ----------------------------------------------------------------------------------------
		
		@SuppressWarnings("unchecked")
		final HashJoin join = new HashJoin(buildInput, probeInput, new int[] {0}, new int[] {0}, new Class[] {PactInteger.class}, memSegments, ioManager);
		join.open();
		
		final PactRecord record = new PactRecord();
		
		try {
			while (join.nextRecord())
			{	
				HashBucketIterator buildSide = join.getBuildSideIterator();
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


	// ============================================================================================
	//                                           Utilities
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
}
