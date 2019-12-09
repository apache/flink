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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.UniformIntTupleGenerator;
import org.apache.flink.runtime.operators.testutils.UnionIterator;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

public class ReOpenableHashTableITCase extends TestLogger {

	private static final int PAGE_SIZE = 8 * 1024;
	private static final long MEMORY_SIZE = PAGE_SIZE * 1000; // 100 Pages.

	private static final int NUM_PROBES = 3; // number of reopenings of hash join

	private IOManager ioManager;
	private MemoryManager memoryManager;

	private static final AbstractInvokable MEM_OWNER = new DummyInvokable();
	private TypeSerializer<Tuple2<Integer, Integer>> recordBuildSideAccesssor;
	private TypeSerializer<Tuple2<Integer, Integer>> recordProbeSideAccesssor;
	private TypeComparator<Tuple2<Integer, Integer>> recordBuildSideComparator;
	private TypeComparator<Tuple2<Integer, Integer>> recordProbeSideComparator;
	private TypePairComparator<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> pactRecordComparator;

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Before
	public void beforeTest() {
		this.recordBuildSideAccesssor = TestData.getIntIntTupleSerializer();
		this.recordProbeSideAccesssor = TestData.getIntIntTupleSerializer();
		this.recordBuildSideComparator = TestData.getIntIntTupleComparator();
		this.recordProbeSideComparator = TestData.getIntIntTupleComparator();
		this.pactRecordComparator = new GenericPairComparator(this.recordBuildSideComparator, this.recordProbeSideComparator);

		this.memoryManager = MemoryManagerBuilder
			.newBuilder()
			.setMemorySize(MEMORY_SIZE)
			.setPageSize(PAGE_SIZE)
			.build();
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

				MutableObjectIterator<Tuple2<Integer, Integer>> buildSide = join.getBuildSideIterator();
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
}
