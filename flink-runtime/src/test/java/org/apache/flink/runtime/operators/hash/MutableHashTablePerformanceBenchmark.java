/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.hash;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryAllocationException;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.types.StringPair;
import org.apache.flink.runtime.operators.testutils.types.StringPairComparator;
import org.apache.flink.runtime.operators.testutils.types.StringPairPairComparator;
import org.apache.flink.runtime.operators.testutils.types.StringPairSerializer;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

public class MutableHashTablePerformanceBenchmark {
	private static final AbstractInvokable MEM_OWNER = new DummyInvokable();
	
	private MemoryManager memManager;
	private IOManager ioManager;
	
	private TypeSerializer<StringPair> pairBuildSideAccesssor;
	private TypeSerializer<StringPair> pairProbeSideAccesssor;
	private TypeComparator<StringPair> pairBuildSideComparator;
	private TypeComparator<StringPair> pairProbeSideComparator;
	private TypePairComparator<StringPair, StringPair> pairComparator;
	
	private static final String COMMENT = "this comments should contains a 96 byte data, 100 plus another integer value and seperator char.";
	
	
	@Before
	public void setup() {
		this.pairBuildSideAccesssor = new StringPairSerializer();
		this.pairProbeSideAccesssor = new StringPairSerializer();
		this.pairBuildSideComparator = new StringPairComparator();
		this.pairProbeSideComparator = new StringPairComparator();
		this.pairComparator = new StringPairPairComparator();
		
		this.memManager = new DefaultMemoryManager(64 * 1024 * 1024, 1);
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
	public void compareMutableHashTablePerformance1() throws IOException {
		// ----------------------------------------------90% filtered during probe spill phase-----------------------------------------
		// create a build input with 1000000 records with key spread between [0 -- 10000000] with step of 10 for nearby records.
		int buildSize = 1000000;
		int buildStep = 10;
		int buildScope = buildStep * buildSize;
		// create a probe input with 5000000 records with key spread between [0 -- 1000000] with distance of 1 for nearby records.
		int probeSize = 5000000;
		int probeStep = 1;
		int probeScope = buildSize;
		
		int expectedResult = 500000;
		
		long withBloomFilterCost = hybridHashJoin(buildSize, buildStep, buildScope, probeSize, probeStep, probeScope, expectedResult, true);
		long withoutBloomFilterCost = hybridHashJoin(buildSize, buildStep, buildScope, probeSize, probeStep, probeScope, expectedResult, false);
		
		System.out.println("HybridHashJoin2:");
		System.out.println("Build input size: " + 100 * buildSize);
		System.out.println("Probe input size: " + 100 * probeSize);
		System.out.println("Available memory: " + this.memManager.getMemorySize());
		System.out.println("Probe record be filtered before spill: " + (1 - (double)probeScope / buildScope) * 100 + "% percent.");
		System.out.println(String.format("Cost: without bloom filter(%d), with bloom filter(%d)", withoutBloomFilterCost, withBloomFilterCost));
	}
	
	@Test
	public void compareMutableHashTablePerformance2() throws IOException {
		// ----------------------------------------------80% filtered during probe spill phase-----------------------------------------
		// create a build input with 1000000 records with key spread between [0 -- 5000000] with step of 5 for nearby records.
		int buildSize = 1000000;
		int buildStep = 5;
		int buildScope = buildStep * buildSize;
		// create a probe input with 5000000 records with key spread between [0 -- 1000000] with distance of 1 for nearby records.
		int probeSize = 5000000;
		int probeStep = 1;
		int probeScope = buildSize;
		
		int expectedResult = 1000000;
		
		long withBloomFilterCost = hybridHashJoin(buildSize, buildStep, buildScope, probeSize, probeStep, probeScope, expectedResult, true);
		long withoutBloomFilterCost = hybridHashJoin(buildSize, buildStep, buildScope, probeSize, probeStep, probeScope, expectedResult, false);
		
		System.out.println("HybridHashJoin3:");
		System.out.println("Build input size: " + 100 * buildSize);
		System.out.println("Probe input size: " + 100 * probeSize);
		System.out.println("Available memory: " + this.memManager.getMemorySize());
		System.out.println("Probe record be filtered before spill: " + (1 - (double)probeScope / buildScope) * 100 + "% percent.");
		System.out.println(String.format("Cost: without bloom filter(%d), with bloom filter(%d)", withoutBloomFilterCost, withBloomFilterCost));
	}
	
	@Test
	public void compareMutableHashTablePerformance3() throws IOException {
		// ----------------------------------------------50% filtered during probe spill phase-------------------------------------------------
		// create a build input with 1000000 records with key spread between [0 -- 2000000] with step of 2 for nearby records.
		int buildSize = 1000000;
		int buildStep = 2;
		int buildScope = buildStep * buildSize;
		// create a probe input with 5000000 records with key spread between [0 -- 1000000] with distance of 1 for nearby records.
		int probeSize = 5000000;
		int probeStep = 1;
		int probeScope = buildSize;
		
		int expectedResult = 2500000;
		
		long withoutBloomFilterCost = hybridHashJoin(buildSize, buildStep, buildScope, probeSize, probeStep, probeScope, expectedResult, false);
		long withBloomFilterCost = hybridHashJoin(buildSize, buildStep, buildScope, probeSize, probeStep, probeScope, expectedResult, true);
		
		System.out.println("HybridHashJoin4:");
		System.out.println("Build input size: " + 100 * buildSize);
		System.out.println("Probe input size: " + 100 * probeSize);
		System.out.println("Available memory: " + this.memManager.getMemorySize());
		System.out.println("Probe record be filtered before spill: " + (1 - (double)probeScope / buildScope) * 100 + "% percent.");
		System.out.println(String.format("Cost: without bloom filter(%d), with bloom filter(%d)", withoutBloomFilterCost, withBloomFilterCost));
	}
	
	@Test
	public void compareMutableHashTablePerformance4() throws IOException {
		// ----------------------------------------------0% filtered during probe spill phase-----------------------------------------
		// create a build input with 1000000 records with key spread between [0 -- 1000000] with step of 1 for nearby records.
		int buildSize = 1000000;
		int buildStep = 1;
		int buildScope = buildStep * buildSize;
		// create a probe input with 5000000 records with key spread between [0 -- 1000000] with distance of 1 for nearby records.
		int probeSize = 5000000;
		int probeStep = 1;
		int probeScope = buildSize;
		
		int expectedResult = probeSize / buildStep;
		
		long withBloomFilterCost = hybridHashJoin(buildSize, buildStep, buildScope, probeSize, probeStep, probeScope, expectedResult, true);
		long withoutBloomFilterCost = hybridHashJoin(buildSize, buildStep, buildScope, probeSize, probeStep, probeScope, expectedResult, false);
		
		System.out.println("HybridHashJoin5:");
		System.out.println("Build input size: " + 100 * buildSize);
		System.out.println("Probe input size: " + 100 * probeSize);
		System.out.println("Available memory: " + this.memManager.getMemorySize());
		System.out.println("Probe record be filtered before spill: " + (1 - (double)probeScope / buildScope) * 100 + "% percent.");
		System.out.println(String.format("Cost: without bloom filter(%d), with bloom filter(%d)", withoutBloomFilterCost, withBloomFilterCost));
	}
	
	private long hybridHashJoin(int buildSize, int buildStep, int buildScope, int probeSize,
		int probeStep, int probeScope, int expectedResultSize, boolean enableBloomFilter) throws IOException {
		
		InputIterator buildIterator = new InputIterator(buildSize, buildStep, buildScope);
		InputIterator probeIterator = new InputIterator(probeSize, probeStep, probeScope);
		
		// allocate the memory for the HashTable
		List<MemorySegment> memSegments;
		try {
			// 33 is minimum number of pages required to perform hash join this inputs
			memSegments = this.memManager.allocatePages(MEM_OWNER, (int) (this.memManager.getMemorySize() / this.memManager.getPageSize()));
		} catch (MemoryAllocationException maex) {
			fail("Memory for the Join could not be provided.");
			return -1;
		}
		
		// ----------------------------------------------------------------------------------------
		
		long start = System.currentTimeMillis();
		final MutableHashTable<StringPair, StringPair> join = new MutableHashTable<StringPair, StringPair>(
			this.pairBuildSideAccesssor, this.pairProbeSideAccesssor,
			this.pairBuildSideComparator, this.pairProbeSideComparator, this.pairComparator,
			memSegments, ioManager, enableBloomFilter);
		join.open(buildIterator, probeIterator);
		
		final StringPair recordReuse = new StringPair();
		int numRecordsInJoinResult = 0;
		
		while (join.nextRecord()) {
			MutableHashTable.HashBucketIterator<StringPair, StringPair> buildSide = join.getBuildSideIterator();
			while (buildSide.next(recordReuse) != null) {
				numRecordsInJoinResult++;
			}
		}
		Assert.assertEquals("Wrong number of records in join result.", expectedResultSize, numRecordsInJoinResult);
		
		join.close();
		long cost = System.currentTimeMillis() - start;
		// ----------------------------------------------------------------------------------------
		
		this.memManager.release(join.getFreedMemory());
		return cost;
	}
	
	
	static class InputIterator implements MutableObjectIterator<StringPair> {
		
		private int numLeft;
		private int distance;
		private int scope;
		
		public InputIterator(int size, int distance, int scope) {
			this.numLeft = size;
			this.distance = distance;
			this.scope = scope;
		}
		
		@Override
		public StringPair next(StringPair reuse) throws IOException {
			if (this.numLeft > 0) {
				numLeft--;
				int currentKey = (numLeft * distance) % scope;
				reuse.setKey(Integer.toString(currentKey));
				reuse.setValue(COMMENT);
				return reuse;
			} else {
				return null;
			}
		}
		
		@Override
		public StringPair next() throws IOException {
			return next(new StringPair());
		}
	}
}
