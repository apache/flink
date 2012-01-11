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
import java.util.Iterator;
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
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.hash.HashJoin.HashBucketIterator;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.UnionIterator;
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
		Iterator<KeyValuePair<PactInteger, PactInteger>> buildInput = new RegularlyGeneratedInputGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		Iterator<KeyValuePair<PactInteger, PactInteger>> probeInput = new RegularlyGeneratedInputGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

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
		
		final KeyValuePair<PactInteger, PactInteger> pair = new KeyValuePair<PactInteger, PactInteger>(new PactInteger(), new PactInteger());
		
		// ----------------------------------------------------------------------------------------
		
		HashJoin<PactInteger, PactInteger, PactInteger> join = new HashJoin<PactInteger, PactInteger, PactInteger>(buildInput, probeInput, 
				PactInteger.class, PactInteger.class, PactInteger.class, memSegments, ioManager);
		join.open();
		
		int numKeys = 0;
		
		while (join.nextKey()) {
			numKeys++;
			int numBuildValues = 0;
			int numProbeValues = 0;
			
			Iterator<KeyValuePair<PactInteger, PactInteger>> probeIter = join.getProbeSideIterator();
			while (probeIter.hasNext()) {
				numProbeValues++;
				probeIter.next();
			}
			Assert.assertEquals("Wrong number of values from probe-side for a key", PROBE_VALS_PER_KEY, numProbeValues);
			
			HashBucketIterator<PactInteger, PactInteger> buildSide = join.getBuildSideIterator();
			while (buildSide.next(pair)) {
				numBuildValues++;
			}
			
			Assert.assertEquals("Wrong number of values from build-side for a key", BUILD_VALS_PER_KEY, numBuildValues);
			
		}
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, numKeys);
		
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
		Iterator<KeyValuePair<PactInteger, PactInteger>> buildInput = new RegularlyGeneratedInputGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		Iterator<KeyValuePair<PactInteger, PactInteger>> probeInput = new RegularlyGeneratedInputGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

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
		
		final KeyValuePair<PactInteger, PactInteger> pair = new KeyValuePair<PactInteger, PactInteger>(new PactInteger(), new PactInteger());
		
		// ----------------------------------------------------------------------------------------
		
		HashJoin<PactInteger, PactInteger, PactInteger> join = new HashJoin<PactInteger, PactInteger, PactInteger>(buildInput, probeInput, 
				PactInteger.class, PactInteger.class, PactInteger.class, memSegments, ioManager);
		join.open();
		
		int numKeys = 0;
		
		while (join.nextKey()) {
			
			numKeys++;
			Iterator<KeyValuePair<PactInteger, PactInteger>> probeIter = join.getProbeSideIterator();
			while (probeIter.hasNext()) {
				probeIter.next();
			}
			
			HashBucketIterator<PactInteger, PactInteger> buildSide = join.getBuildSideIterator();
			while (buildSide.next(pair));	
		}
		
		join.close();
		
		Assert.assertEquals("Wrong number of keys", NUM_KEYS, numKeys);
		
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
		Iterator<KeyValuePair<PactInteger, PactInteger>> buildInput = new RegularlyGeneratedInputGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		Iterator<KeyValuePair<PactInteger, PactInteger>> probeInput = new RegularlyGeneratedInputGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

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
		
		final KeyValuePair<PactInteger, PactInteger> pair = new KeyValuePair<PactInteger, PactInteger>(new PactInteger(), new PactInteger());
		
		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);
		
		// ----------------------------------------------------------------------------------------
		
		HashJoin<PactInteger, PactInteger, PactInteger> join = new HashJoin<PactInteger, PactInteger, PactInteger>(buildInput, probeInput, 
				PactInteger.class, PactInteger.class, PactInteger.class, memSegments, ioManager);
		
		join.open();

		int numKeyCalls = 0;
		while (join.nextKey())
		{
			numKeyCalls++;
			
			int numBuildValues = 0;
			int numProbeValues = 0;
			
			int key = 0;
			
			HashBucketIterator<PactInteger, PactInteger> buildSide = join.getBuildSideIterator();
			if (buildSide.next(pair)) {
				numBuildValues = 1;
				key = pair.getKey().getValue();
			}
			else {
				fail("No build side values found for a probe key.");
			}
			while (buildSide.next(pair)) {
				numBuildValues++;
			}
			
			Iterator<KeyValuePair<PactInteger, PactInteger>> probeIter = join.getProbeSideIterator();
			while (probeIter.hasNext()) {
				KeyValuePair<PactInteger, PactInteger> nextPair = probeIter.next();
				Assert.assertEquals("Probe-side key was different than build-side key.", key, nextPair.getKey().getValue()); 
				numProbeValues++;
			}
			
			Long contained = map.get(key);
			if (contained == null) {
				contained = new Long(numBuildValues * numProbeValues);
			}
			else {
				contained = new Long(contained.longValue() + (numBuildValues * numProbeValues));
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
		final int REPEATED_VALUE_COUNT = 200000; 
		
		final int NUM_KEYS = 1000000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		Iterator<KeyValuePair<PactInteger, PactInteger>> build1 = new RegularlyGeneratedInputGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		Iterator<KeyValuePair<PactInteger, PactInteger>> build2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
		Iterator<KeyValuePair<PactInteger, PactInteger>> build3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
		List<Iterator<KeyValuePair<PactInteger, PactInteger>>> builds = new ArrayList<Iterator<KeyValuePair<PactInteger,PactInteger>>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		Iterator<KeyValuePair<PactInteger, PactInteger>> buildInput = new UnionIterator<KeyValuePair<PactInteger, PactInteger>>(builds);
	
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		Iterator<KeyValuePair<PactInteger, PactInteger>> probe1 = new RegularlyGeneratedInputGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
		Iterator<KeyValuePair<PactInteger, PactInteger>> probe2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
		Iterator<KeyValuePair<PactInteger, PactInteger>> probe3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
		List<Iterator<KeyValuePair<PactInteger, PactInteger>>> probes = new ArrayList<Iterator<KeyValuePair<PactInteger,PactInteger>>>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		Iterator<KeyValuePair<PactInteger, PactInteger>> probeInput = new UnionIterator<KeyValuePair<PactInteger, PactInteger>>(probes);

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
		
		final KeyValuePair<PactInteger, PactInteger> pair = new KeyValuePair<PactInteger, PactInteger>(new PactInteger(), new PactInteger());
		
		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);
		
		// ----------------------------------------------------------------------------------------
		
		HashJoin<PactInteger, PactInteger, PactInteger> join = new HashJoin<PactInteger, PactInteger, PactInteger>(buildInput, probeInput, 
				PactInteger.class, PactInteger.class, PactInteger.class, memSegments, ioManager);
		
		join.open();
	
		int numKeyCalls = 0;
		while (join.nextKey())
		{
			numKeyCalls++;
			
			int numBuildValues = 0;
			int numProbeValues = 0;
			
			int key = 0;
			
			Iterator<KeyValuePair<PactInteger, PactInteger>> probeIter = join.getProbeSideIterator();
			while (probeIter.hasNext()) {
				KeyValuePair<PactInteger, PactInteger> nextPair = probeIter.next();
				key = nextPair.getKey().getValue();
				numProbeValues++;
			}
			
			HashBucketIterator<PactInteger, PactInteger> buildSide = join.getBuildSideIterator();
			if (buildSide.next(pair)) {
				numBuildValues = 1;
				Assert.assertEquals("Probe-side key was different than build-side key.", key, pair.getKey().getValue()); 
			}
			else {
				fail("No build side values found for a probe key.");
			}
			while (buildSide.next(pair)) {
				numBuildValues++;
				Assert.assertEquals("Probe-side key was different than build-side key.", key, pair.getKey().getValue());
			}
			
			Long contained = map.get(key);
			if (contained == null) {
				contained = new Long(numBuildValues * numProbeValues);
			}
			else {
				contained = new Long(contained.longValue() + (numBuildValues * numProbeValues));
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
					(PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT) * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT) : 
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
		final int REPEATED_VALUE_COUNT = 1000000; 
		
		final int NUM_KEYS = 1000000;
		final int BUILD_VALS_PER_KEY = 3;
		final int PROBE_VALS_PER_KEY = 10;
		
		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		Iterator<KeyValuePair<PactInteger, PactInteger>> build1 = new RegularlyGeneratedInputGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		Iterator<KeyValuePair<PactInteger, PactInteger>> build2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
		Iterator<KeyValuePair<PactInteger, PactInteger>> build3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
		List<Iterator<KeyValuePair<PactInteger, PactInteger>>> builds = new ArrayList<Iterator<KeyValuePair<PactInteger,PactInteger>>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		Iterator<KeyValuePair<PactInteger, PactInteger>> buildInput = new UnionIterator<KeyValuePair<PactInteger, PactInteger>>(builds);
	
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		Iterator<KeyValuePair<PactInteger, PactInteger>> probe1 = new RegularlyGeneratedInputGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
		Iterator<KeyValuePair<PactInteger, PactInteger>> probe2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
		Iterator<KeyValuePair<PactInteger, PactInteger>> probe3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
		List<Iterator<KeyValuePair<PactInteger, PactInteger>>> probes = new ArrayList<Iterator<KeyValuePair<PactInteger,PactInteger>>>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		Iterator<KeyValuePair<PactInteger, PactInteger>> probeInput = new UnionIterator<KeyValuePair<PactInteger, PactInteger>>(probes);

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
		
		final KeyValuePair<PactInteger, PactInteger> pair = new KeyValuePair<PactInteger, PactInteger>(new PactInteger(), new PactInteger());
		
		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);
		
		// ----------------------------------------------------------------------------------------
		
		HashJoin<PactInteger, PactInteger, PactInteger> join = new HashJoin<PactInteger, PactInteger, PactInteger>(buildInput, probeInput, 
				PactInteger.class, PactInteger.class, PactInteger.class, memSegments, ioManager);
		
		join.open();
	
		int numKeyCalls = 0;
		while (join.nextKey())
		{
			numKeyCalls++;
			
			int numBuildValues = 0;
			int numProbeValues = 0;
			
			int key = 0;
			
			Iterator<KeyValuePair<PactInteger, PactInteger>> probeIter = join.getProbeSideIterator();
			while (probeIter.hasNext()) {
				KeyValuePair<PactInteger, PactInteger> nextPair = probeIter.next();
				key = nextPair.getKey().getValue();
				numProbeValues++;
			}
			
			HashBucketIterator<PactInteger, PactInteger> buildSide = join.getBuildSideIterator();
			if (buildSide.next(pair)) {
				numBuildValues = 1;
				Assert.assertEquals("Probe-side key was different than build-side key.", key, pair.getKey().getValue()); 
			}
			else {
				fail("No build side values found for a probe key.");
			}
			while (buildSide.next(pair)) {
				numBuildValues++;
				Assert.assertEquals("Probe-side key was different than build-side key.", key, pair.getKey().getValue());
			}
			
			Long contained = map.get(key);
			if (contained == null) {
				contained = new Long(numBuildValues * numProbeValues);
			}
			else {
				contained = new Long(contained.longValue() + (numBuildValues * numProbeValues));
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
					(PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT) * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT) : 
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
		Iterator<KeyValuePair<PactInteger, PactInteger>> build1 = new RegularlyGeneratedInputGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		Iterator<KeyValuePair<PactInteger, PactInteger>> build2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
		Iterator<KeyValuePair<PactInteger, PactInteger>> build3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
		List<Iterator<KeyValuePair<PactInteger, PactInteger>>> builds = new ArrayList<Iterator<KeyValuePair<PactInteger,PactInteger>>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		Iterator<KeyValuePair<PactInteger, PactInteger>> buildInput = new UnionIterator<KeyValuePair<PactInteger, PactInteger>>(builds);
	
		// create a probe input that gives 10 million pairs with 10 values sharing a key
		Iterator<KeyValuePair<PactInteger, PactInteger>> probe1 = new RegularlyGeneratedInputGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
		Iterator<KeyValuePair<PactInteger, PactInteger>> probe2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
		Iterator<KeyValuePair<PactInteger, PactInteger>> probe3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
		List<Iterator<KeyValuePair<PactInteger, PactInteger>>> probes = new ArrayList<Iterator<KeyValuePair<PactInteger,PactInteger>>>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		Iterator<KeyValuePair<PactInteger, PactInteger>> probeInput = new UnionIterator<KeyValuePair<PactInteger, PactInteger>>(probes);
		
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
		
		final KeyValuePair<PactInteger, PactInteger> pair = new KeyValuePair<PactInteger, PactInteger>(new PactInteger(), new PactInteger());
		
		// ----------------------------------------------------------------------------------------
		
		HashJoin<PactInteger, PactInteger, PactInteger> join = new HashJoin<PactInteger, PactInteger, PactInteger>(buildInput, probeInput, 
				PactInteger.class, PactInteger.class, PactInteger.class, memSegments, ioManager);
		
		join.open();
	
		try {
			while (join.nextKey())
			{
				Iterator<KeyValuePair<PactInteger, PactInteger>> probeIter = join.getProbeSideIterator();
				while (probeIter.hasNext()) {
					probeIter.next();
				}
				
				HashBucketIterator<PactInteger, PactInteger> buildSide = join.getBuildSideIterator();
				if (!buildSide.next(pair)) {
					fail("No build side values found for a probe key.");
				}
				while (buildSide.next(pair));;
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
	
	/**
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

		Iterator<KeyValuePair<PactInteger, PactInteger>> buildInput = new RegularlyGeneratedInputGenerator(
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

		HashJoin<PactInteger, PactInteger, PactInteger> join = new HashJoin<PactInteger, PactInteger, PactInteger>(
				buildInput, new RegularlyGeneratedInputGenerator(
						NUM_PROBE_KEYS, NUM_PROBE_VALS, true),
				PactInteger.class, PactInteger.class, PactInteger.class,
				memSegments, ioManager);
		join.open();

		int expectedNumResults = (Math.min(NUM_PROBE_KEYS, NUM_BUILD_KEYS) * NUM_BUILD_VALS)
				* NUM_PROBE_VALS;
		int numResults = 0;
		final KeyValuePair<PactInteger, PactInteger> pair = 
					new KeyValuePair<PactInteger, PactInteger>(new PactInteger(), new PactInteger());
		while (join.nextKey()) {
			int numBuildValues = 0;
			int numProbeValues = 0;

			Iterator<KeyValuePair<PactInteger, PactInteger>> probeIter = join
					.getProbeSideIterator();
			while (probeIter.hasNext()) {
				numProbeValues++;
				probeIter.next();
			}
			Assert.assertEquals(
					"Wrong number of values from probe-side for a key",
					NUM_PROBE_VALS, numProbeValues);

			HashBucketIterator<PactInteger, PactInteger> buildSide = join
					.getBuildSideIterator();
			while (buildSide.next(pair)) {
				numBuildValues++;
			}

			Assert.assertEquals(
					"Wrong number of values from build-side for a key",
					NUM_BUILD_VALS, numBuildValues);
			numResults += numProbeValues * numBuildValues;
		}
		Assert.assertEquals("Wrong number of results", expectedNumResults,
				numResults);

		join.close();
	}
	
	@Test
	public void testStrangeCase() throws IOException, MemoryAllocationException {
		int reqMem = 2785280;

		final int NUM_BUILD_KEYS = 500000;
		final int NUM_BUILD_VALS = 1;
		final int NUM_PROBE_KEYS = 10;
		final int NUM_PROBE_VALS = 1;
		
		Iterator<KeyValuePair<PactInteger, PactInteger>> buildInput = new RegularlyGeneratedInputGenerator(NUM_BUILD_KEYS, NUM_BUILD_VALS, false);
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
				
		HashJoin<PactInteger, PactInteger, PactInteger> join = new HashJoin<PactInteger, PactInteger, PactInteger>(buildInput,
				new RegularlyGeneratedInputGenerator(NUM_PROBE_KEYS, NUM_PROBE_VALS, true), 
				PactInteger.class, PactInteger.class, PactInteger.class, memSegments, ioManager);
		join.open();
		
		// Second probe phase with new records. This time, all remaining results are to be generated:
		int expectedNumResults = (Math.min(NUM_PROBE_KEYS, NUM_BUILD_KEYS) * NUM_BUILD_VALS) * NUM_PROBE_VALS;
		int numResults = 0;
		final KeyValuePair<PactInteger, PactInteger> pair = new KeyValuePair<PactInteger, PactInteger>(new PactInteger(), new PactInteger());
		while (join.nextKey()) {
			int numBuildValues = 0;
			int numProbeValues = 0;
			
			Iterator<KeyValuePair<PactInteger, PactInteger>> probeIter = join.getProbeSideIterator();
			while (probeIter.hasNext()) {
				numProbeValues++;
				probeIter.next();
			}
			Assert.assertEquals("Wrong number of values from probe-side for a key", NUM_PROBE_VALS, numProbeValues);
			
			HashBucketIterator<PactInteger, PactInteger> buildSide = join.getBuildSideIterator();
			while (buildSide.next(pair)) {
				numBuildValues++;
			}
			
			Assert.assertEquals("Wrong number of values from build-side for a key", NUM_BUILD_VALS, numBuildValues);
			numResults += numProbeValues * numBuildValues;
		}
		Assert.assertEquals("Wrong number of results", expectedNumResults, numResults);
		
		join.close();
	}


	// ============================================================================================
	//                                           Utilities
	// ============================================================================================
	
	
	/**
	 * An iterator that returns the Key/Value pairs with identical value a given number of times.
	 */
	private static final class ConstantsKeyValuePairsIterator implements Iterator<KeyValuePair<PactInteger, PactInteger>>
	{
		private final int key;
		
		private final int value;
		
		private int numLeft;
		
		public ConstantsKeyValuePairsIterator(int key, int value, int count)
		{
			this.key = key;
			this.value = value;
			this.numLeft = count;
		}

		@Override
		public boolean hasNext() {
			return this.numLeft > 0;
		}

		@Override
		public KeyValuePair<PactInteger, PactInteger> next() {
			if (this.numLeft > 0) {
				this.numLeft--;
				return new KeyValuePair<PactInteger, PactInteger>(new PactInteger(this.key), new PactInteger(this.value));
			}
			else {
				throw new UnsupportedOperationException();
			}
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}
	}
}
