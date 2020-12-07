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

package org.apache.flink.table.runtime.hashtable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.UnionIterator;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.operators.join.HashJoinType;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.table.runtime.util.UniformBinaryRowGenerator;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Hash table it case for binary row.
 */
@RunWith(Parameterized.class)
public class BinaryHashTableTest {

	private static final int PAGE_SIZE = 32 * 1024;
	private IOManager ioManager;
	private BinaryRowDataSerializer buildSideSerializer;
	private BinaryRowDataSerializer probeSideSerializer;

	private boolean useCompress;
	private Configuration conf;

	public BinaryHashTableTest(boolean useCompress) {
		this.useCompress = useCompress;
	}

	@Parameterized.Parameters(name = "useCompress-{0}")
	public static List<Boolean> getVarSeg() {
		return Arrays.asList(true, false);
	}

	@Before
	public void setup() {
		TypeInformation[] types = new TypeInformation[]{Types.INT, Types.INT};
		this.buildSideSerializer = new BinaryRowDataSerializer(types.length);
		this.probeSideSerializer = new BinaryRowDataSerializer(types.length);

		this.ioManager = new IOManagerAsync();

		conf = new Configuration();
		conf.setBoolean(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED, useCompress);
	}

	@After
	public void tearDown() throws Exception {
		// shut down I/O manager and Memory Manager and verify the correct shutdown
		this.ioManager.close();
	}

	@Test
	public void testIOBufferCountComputation() {
		assertEquals(1, BinaryHashTable.getNumWriteBehindBuffers(32));
		assertEquals(1, BinaryHashTable.getNumWriteBehindBuffers(33));
		assertEquals(1, BinaryHashTable.getNumWriteBehindBuffers(40));
		assertEquals(1, BinaryHashTable.getNumWriteBehindBuffers(64));
		assertEquals(1, BinaryHashTable.getNumWriteBehindBuffers(127));
		assertEquals(2, BinaryHashTable.getNumWriteBehindBuffers(128));
		assertEquals(2, BinaryHashTable.getNumWriteBehindBuffers(129));
		assertEquals(2, BinaryHashTable.getNumWriteBehindBuffers(511));
		assertEquals(3, BinaryHashTable.getNumWriteBehindBuffers(512));
		assertEquals(3, BinaryHashTable.getNumWriteBehindBuffers(513));
		assertEquals(3, BinaryHashTable.getNumWriteBehindBuffers(2047));
		assertEquals(4, BinaryHashTable.getNumWriteBehindBuffers(2048));
		assertEquals(4, BinaryHashTable.getNumWriteBehindBuffers(2049));
		assertEquals(4, BinaryHashTable.getNumWriteBehindBuffers(8191));
		assertEquals(5, BinaryHashTable.getNumWriteBehindBuffers(8192));
		assertEquals(5, BinaryHashTable.getNumWriteBehindBuffers(8193));
		assertEquals(5, BinaryHashTable.getNumWriteBehindBuffers(32767));
		assertEquals(6, BinaryHashTable.getNumWriteBehindBuffers(32768));
		assertEquals(6, BinaryHashTable.getNumWriteBehindBuffers(Integer.MAX_VALUE));
	}

	@Test
	public void testInMemoryMutableHashTable() throws IOException {
		final int numKeys = 100000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);
		MemoryManager memManager = MemoryManagerBuilder.newBuilder().setMemorySize(896 * PAGE_SIZE).build();
		// ----------------------------------------------------------------------------------------
		final BinaryHashTable table = newBinaryHashTable(
				this.buildSideSerializer, this.probeSideSerializer,
				new MyProjection(), new MyProjection(), memManager,
				100 * PAGE_SIZE, ioManager);

		int numRecordsInJoinResult = join(table, buildInput, probeInput);
		Assert.assertEquals("Wrong number of records in join result.", numKeys * buildValsPerKey * probeValsPerKey, numRecordsInJoinResult);

		table.close();

		table.free();
	}

	private int join(
			BinaryHashTable table,
			MutableObjectIterator<BinaryRowData> buildInput,
			MutableObjectIterator<BinaryRowData> probeInput) throws IOException {
		return join(table, buildInput, probeInput, false);
	}

	private int join(
			BinaryHashTable table,
			MutableObjectIterator<BinaryRowData> buildInput,
			MutableObjectIterator<BinaryRowData> probeInput,
			boolean buildOuterJoin) throws IOException {
		int count = 0;

		BinaryRowData reuseBuildSizeRow = buildSideSerializer.createInstance();
		BinaryRowData buildRow;
		while ((buildRow = buildInput.next(reuseBuildSizeRow)) != null) {
			table.putBuildRow(buildRow);
		}
		table.endBuild();

		BinaryRowData probeRow = probeSideSerializer.createInstance();
		while ((probeRow = probeInput.next(probeRow)) != null) {
			if (table.tryProbe(probeRow)){
				count += joinWithNextKey(table, buildOuterJoin);
			}
		}

		while (table.nextMatching()){
			count += joinWithNextKey(table, buildOuterJoin);
		}
		return count;
	}

	private int joinWithNextKey(BinaryHashTable table, boolean buildOuterJoin) throws IOException {
		int count = 0;
		final RowIterator<BinaryRowData> buildIterator = table.getBuildSideIterator();
		final RowData probeRow = table.getCurrentProbeRow();
		BinaryRowData buildRow;

		buildRow = buildIterator.advanceNext() ? buildIterator.getRow() : null;
		// get the first build side value
		if (probeRow != null && buildRow != null) {
			count++;
			while (buildIterator.advanceNext()) {
				count++;
			}
		} else {
			if (buildOuterJoin && probeRow == null && buildRow != null) {
				count++;
				while (buildIterator.advanceNext()) {
					count++;
				}
			}
		}
		return count;
	}

	@Test
	public void testSpillingHashJoinOneRecursionPerformance() throws IOException {
		final int numKeys = 1000000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		// allocate the memory for the HashTable
		MemoryManager memManager = MemoryManagerBuilder
			.newBuilder()
			.setMemorySize(200 * PAGE_SIZE)
			.setPageSize(PAGE_SIZE)
			.build();
		final BinaryHashTable table = newBinaryHashTable(
				this.buildSideSerializer,
				this.probeSideSerializer,
				new MyProjection(),
				new MyProjection(),
				memManager,
				100 * PAGE_SIZE,
				ioManager);

		// ----------------------------------------------------------------------------------------

		int numRecordsInJoinResult = join(table, buildInput, probeInput);

		Assert.assertEquals("Wrong number of records in join result.", numKeys * buildValsPerKey * probeValsPerKey, numRecordsInJoinResult);

		table.close();

		// ----------------------------------------------------------------------------------------

		table.free();
	}

	@Test
	public void testSpillingHashJoinOneRecursionValidity() throws IOException {
		final int numKeys = 1000000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<>(numKeys);

		// ----------------------------------------------------------------------------------------
		MemoryManager memManager = MemoryManagerBuilder.newBuilder().setMemorySize(896 * PAGE_SIZE).build();
		final BinaryHashTable table = newBinaryHashTable(
				this.buildSideSerializer, this.probeSideSerializer,
				new MyProjection(), new MyProjection(), memManager,
				100 * PAGE_SIZE, ioManager);
		final BinaryRowData recordReuse = new BinaryRowData(2);

		BinaryRowData buildRow = buildSideSerializer.createInstance();
		while ((buildRow = buildInput.next(buildRow)) != null) {
			table.putBuildRow(buildRow);
		}
		table.endBuild();

		BinaryRowData probeRow = probeSideSerializer.createInstance();
		while ((probeRow = probeInput.next(probeRow)) != null) {
			if (table.tryProbe(probeRow)){
				testJoin(table, map);
			}
		}

		while (table.nextMatching()){
			testJoin(table, map);
		}

		table.close();

		Assert.assertEquals("Wrong number of keys", numKeys, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();

			Assert.assertEquals("Wrong number of values in per-key cross product for key " + key,
					probeValsPerKey * buildValsPerKey, val);
		}

		// ----------------------------------------------------------------------------------------

		table.free();
	}

	@Test
	public void testSpillingHashJoinWithMassiveCollisions() throws IOException {
		// the following two values are known to have a hash-code collision on the initial level.
		// we use them to make sure one partition grows over-proportionally large
		final int repeatedValue1 = 40559;
		final int repeatedValue2 = 92882;
		final int repeatedValueCountBuild = 200000;
		final int repeatedValueCountProbe = 5;

		final int numKeys = 1000000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		MutableObjectIterator<BinaryRowData> build1 = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
		MutableObjectIterator<BinaryRowData> build2 = new ConstantsKeyValuePairsIterator(repeatedValue1, 17, repeatedValueCountBuild);
		MutableObjectIterator<BinaryRowData> build3 = new ConstantsKeyValuePairsIterator(repeatedValue2, 23, repeatedValueCountBuild);
		List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probe1 = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probe2 = new ConstantsKeyValuePairsIterator(repeatedValue1, 17, 5);
		MutableObjectIterator<BinaryRowData> probe3 = new ConstantsKeyValuePairsIterator(repeatedValue2, 23, 5);
		List<MutableObjectIterator<BinaryRowData>> probes = new ArrayList<>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		MutableObjectIterator<BinaryRowData> probeInput = new UnionIterator<>(probes);

		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<>(numKeys);
		MemoryManager memManager = MemoryManagerBuilder.newBuilder().setMemorySize(896 * PAGE_SIZE).build();
		// ----------------------------------------------------------------------------------------

		final BinaryHashTable table = newBinaryHashTable(
				this.buildSideSerializer, this.probeSideSerializer,
				new MyProjection(), new MyProjection(), memManager,
				896 * PAGE_SIZE, ioManager);

		final BinaryRowData recordReuse = new BinaryRowData(2);

		BinaryRowData buildRow = buildSideSerializer.createInstance();
		while ((buildRow = buildInput.next(buildRow)) != null) {
			table.putBuildRow(buildRow);
		}
		table.endBuild();

		BinaryRowData probeRow = probeSideSerializer.createInstance();
		while ((probeRow = probeInput.next(probeRow)) != null) {
			if (table.tryProbe(probeRow)){
				testJoin(table, map);
			}
		}

		while (table.nextMatching()){
			testJoin(table, map);
		}

		table.close();

		Assert.assertEquals("Wrong number of keys", numKeys, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();

			Assert.assertEquals("Wrong number of values in per-key cross product for key " + key,
					(key == repeatedValue1 || key == repeatedValue2) ?
							(probeValsPerKey + repeatedValueCountProbe) * (buildValsPerKey + repeatedValueCountBuild) :
							probeValsPerKey * buildValsPerKey, val);
		}

		// ----------------------------------------------------------------------------------------

		table.free();
	}

	private void testJoin(BinaryHashTable table, HashMap<Integer, Long> map) throws IOException {
		BinaryRowData record;
		int numBuildValues = 0;

		final RowData probeRec = table.getCurrentProbeRow();
		int key = probeRec.getInt(0);

		RowIterator<BinaryRowData> buildSide = table.getBuildSideIterator();
		if (buildSide.advanceNext()) {
			numBuildValues = 1;
			record = buildSide.getRow();
			assertEquals("Probe-side key was different than build-side key.", key, record.getInt(0));
		} else {
			fail("No build side values found for a probe key.");
		}
		while (buildSide.advanceNext()) {
			numBuildValues++;
			record = buildSide.getRow();
			assertEquals("Probe-side key was different than build-side key.", key, record.getInt(0));
		}

		Long contained = map.get(key);
		if (contained == null) {
			contained = (long) numBuildValues;
		} else {
			contained = contained + numBuildValues;
		}

		map.put(key, contained);
	}

	/*
	 * This test is basically identical to the "testSpillingHashJoinWithMassiveCollisions" test, only that the number
	 * of repeated values (causing bucket collisions) are large enough to make sure that their target partition no longer
	 * fits into memory by itself and needs to be repartitioned in the recursion again.
	 */
	@Test
	public void testSpillingHashJoinWithTwoRecursions() throws IOException {
		// the following two values are known to have a hash-code collision on the first recursion level.
		// we use them to make sure one partition grows over-proportionally large
		final int repeatedValue1 = 40559;
		final int repeatedValue2 = 92882;
		final int repeatedValueCountBuild = 200000;
		final int repeatedValueCountProbe = 5;

		final int numKeys = 1000000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		MutableObjectIterator<BinaryRowData> build1 = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
		MutableObjectIterator<BinaryRowData> build2 = new ConstantsKeyValuePairsIterator(repeatedValue1, 17, repeatedValueCountBuild);
		MutableObjectIterator<BinaryRowData> build3 = new ConstantsKeyValuePairsIterator(repeatedValue2, 23, repeatedValueCountBuild);
		List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probe1 = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probe2 = new ConstantsKeyValuePairsIterator(repeatedValue1, 17, 5);
		MutableObjectIterator<BinaryRowData> probe3 = new ConstantsKeyValuePairsIterator(repeatedValue2, 23, 5);
		List<MutableObjectIterator<BinaryRowData>> probes = new ArrayList<>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		MutableObjectIterator<BinaryRowData> probeInput = new UnionIterator<>(probes);

		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<>(numKeys);

		// ----------------------------------------------------------------------------------------
		MemoryManager memManager = MemoryManagerBuilder.newBuilder().setMemorySize(896 * PAGE_SIZE).build();
		final BinaryHashTable table = newBinaryHashTable(
				this.buildSideSerializer, this.probeSideSerializer,
				new MyProjection(), new MyProjection(), memManager,
				896 * PAGE_SIZE, ioManager);
		final BinaryRowData recordReuse = new BinaryRowData(2);

		BinaryRowData buildRow = buildSideSerializer.createInstance();
		while ((buildRow = buildInput.next(buildRow)) != null) {
			table.putBuildRow(buildRow);
		}
		table.endBuild();

		BinaryRowData probeRow = probeSideSerializer.createInstance();
		while ((probeRow = probeInput.next(probeRow)) != null) {
			if (table.tryProbe(probeRow)){
				testJoin(table, map);
			}
		}

		while (table.nextMatching()){
			testJoin(table, map);
		}

		table.close();

		Assert.assertEquals("Wrong number of keys", numKeys, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();

			Assert.assertEquals("Wrong number of values in per-key cross product for key " + key,
					(key == repeatedValue1 || key == repeatedValue2) ?
							(probeValsPerKey + repeatedValueCountProbe) * (buildValsPerKey + repeatedValueCountBuild) :
							probeValsPerKey * buildValsPerKey, val);
		}

		// ----------------------------------------------------------------------------------------

		table.free();
	}

	/*
	 * This test is basically identical to the "testSpillingHashJoinWithMassiveCollisions" test, only that the number
	 * of repeated values (causing bucket collisions) are large enough to make sure that their target partition no longer
	 * fits into memory by itself and needs to be repartitioned in the recursion again.
	 */
	@Test
	public void testFailingHashJoinTooManyRecursions() throws IOException {
		// the following two values are known to have a hash-code collision on the first recursion level.
		// we use them to make sure one partition grows over-proportionally large
		final int repeatedValue1 = 40559;
		final int repeatedValue2 = 92882;
		final int repeatedValueCount = 3000000;

		final int numKeys = 1000000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		MutableObjectIterator<BinaryRowData> build1 = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
		MutableObjectIterator<BinaryRowData> build2 = new ConstantsKeyValuePairsIterator(repeatedValue1, 17, repeatedValueCount);
		MutableObjectIterator<BinaryRowData> build3 = new ConstantsKeyValuePairsIterator(repeatedValue2, 23, repeatedValueCount);
		List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probe1 = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probe2 = new ConstantsKeyValuePairsIterator(repeatedValue1, 17, repeatedValueCount);
		MutableObjectIterator<BinaryRowData> probe3 = new ConstantsKeyValuePairsIterator(repeatedValue2, 23, repeatedValueCount);
		List<MutableObjectIterator<BinaryRowData>> probes = new ArrayList<>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		MutableObjectIterator<BinaryRowData> probeInput = new UnionIterator<>(probes);
		// ----------------------------------------------------------------------------------------
		MemoryManager memManager = MemoryManagerBuilder.newBuilder().setMemorySize(896 * PAGE_SIZE).build();
		final BinaryHashTable table = newBinaryHashTable(
				this.buildSideSerializer, this.probeSideSerializer,
				new MyProjection(), new MyProjection(), memManager,
				896 * PAGE_SIZE, ioManager);

		try {
			join(table, buildInput, probeInput);
			fail("Hash Join must have failed due to too many recursions.");
		} catch (Exception ex) {
			// expected
		}

		table.close();

		// ----------------------------------------------------------------------------------------

		table.free();
	}

	/*
	 * Spills build records, so that probe records are also spilled. But only so
	 * few probe records are used that some partitions remain empty.
	 */
	@Test
	public void testSparseProbeSpilling() throws IOException, MemoryAllocationException {
		final int numBuildKeys = 1000000;
		final int numBuildVals = 1;
		final int numProbeKeys = 20;
		final int numProbeVals = 1;

		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(
				numBuildKeys, numBuildVals, false);
		MemoryManager memManager = MemoryManagerBuilder.newBuilder().setMemorySize(128 * PAGE_SIZE).build();
		final BinaryHashTable table = newBinaryHashTable(
				this.buildSideSerializer, this.probeSideSerializer,
				new MyProjection(), new MyProjection(), memManager,
				100 * PAGE_SIZE, ioManager);

		int expectedNumResults = (Math.min(numProbeKeys, numBuildKeys) * numBuildVals)
				* numProbeVals;

		int numRecordsInJoinResult = join(table, buildInput, new UniformBinaryRowGenerator(numProbeKeys, numProbeVals, true));

		Assert.assertEquals("Wrong number of records in join result.", expectedNumResults, numRecordsInJoinResult);

		table.close();

		table.free();
	}

	/*
	 * Same test as {@link #testSparseProbeSpilling} but using a build-side outer join
	 * that requires spilled build-side records to be returned and counted.
	 */
	@Test
	public void testSparseProbeSpillingWithOuterJoin() throws IOException {
		final int numBuildKeys = 1000000;
		final int numBuildVals = 1;
		final int numProbeKeys = 20;
		final int numProbeVals = 1;

		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(
				numBuildKeys, numBuildVals, false);
		MemoryManager memManager = MemoryManagerBuilder.newBuilder().setMemorySize(96 * PAGE_SIZE).build();
		final BinaryHashTable table = new BinaryHashTable(conf, new Object(),
				this.buildSideSerializer, this.probeSideSerializer,
				new MyProjection(), new MyProjection(), memManager, 96 * PAGE_SIZE,
				ioManager, 24, 200000, true, HashJoinType.BUILD_OUTER, null, true, new boolean[] {true}, false);

		int expectedNumResults =
				(Math.max(numProbeKeys, numBuildKeys) * numBuildVals) * numProbeVals;

		int numRecordsInJoinResult = join(table, buildInput, new UniformBinaryRowGenerator(numProbeKeys, numProbeVals, true), true);

		Assert.assertEquals("Wrong number of records in join result.", expectedNumResults, numRecordsInJoinResult);

		table.close();
		table.free();
	}

	/*
	 * This test validates a bug fix against former memory loss in the case where a partition was spilled
	 * during an insert into the same.
	 */
	@Test
	public void validateSpillingDuringInsertion() throws IOException, MemoryAllocationException {
		final int numBuildKeys = 500000;
		final int numBuildVals = 1;
		final int numProbeKeys = 10;
		final int numProbeVals = 1;

		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numBuildKeys, numBuildVals, false);
		MemoryManager memManager = MemoryManagerBuilder.newBuilder().setMemorySize(85 * PAGE_SIZE).build();
		final BinaryHashTable table = newBinaryHashTable(
				this.buildSideSerializer, this.probeSideSerializer,
				new MyProjection(), new MyProjection(), memManager,
				85 * PAGE_SIZE, ioManager);

		int expectedNumResults = (Math.min(numProbeKeys, numBuildKeys) * numBuildVals)
				* numProbeVals;

		int numRecordsInJoinResult = join(table, buildInput, new UniformBinaryRowGenerator(numProbeKeys, numProbeVals, true));

		Assert.assertEquals("Wrong number of records in join result.", expectedNumResults, numRecordsInJoinResult);

		table.close();

		table.free();
	}

	@Test
	public void testBucketsNotFulfillSegment() throws Exception {
		final int numKeys = 10000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 30000 pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

		// create a probe input that gives 100000 pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		// allocate the memory for the HashTable
		MemoryManager memManager = MemoryManagerBuilder.newBuilder().setMemorySize(35 * PAGE_SIZE).build();
		// ----------------------------------------------------------------------------------------

		final BinaryHashTable table = new BinaryHashTable(conf, new Object(),
				this.buildSideSerializer, this.probeSideSerializer,
				new MyProjection(), new MyProjection(),
				memManager, 35 * PAGE_SIZE, ioManager, 24, 200000,
				true, HashJoinType.INNER, null, false, new boolean[]{true}, false);

		// For FLINK-2545, the buckets data may not fulfill it's buffer, for example, the buffer may contains 256 buckets,
		// while hash table only assign 250 bucket on it. The unused buffer bytes may contains arbitrary data, which may
		// influence hash table if forget to skip it. To mock this, put the invalid bucket data(partition=1, inMemory=true, count=-1)
		// at the end of buffer.
		int totalPages = table.getInternalPool().freePages();
		for (int i = 0; i < totalPages; i++) {
			MemorySegment segment = table.getInternalPool().nextSegment();
			int newBucketOffset = segment.size() - 128;
			// initialize the header fields
			segment.put(newBucketOffset, (byte) 0);
			segment.put(newBucketOffset + 1, (byte) 0);
			segment.putShort(newBucketOffset + 2, (short) -1);
			segment.putLong(newBucketOffset + 4, ~0x0L);
			table.returnPage(segment);
		}

		int numRecordsInJoinResult = join(table, buildInput, probeInput);

		Assert.assertEquals("Wrong number of records in join result.", numKeys * buildValsPerKey * probeValsPerKey, numRecordsInJoinResult);

		table.close();
		table.free();
	}

	@Test
	public void testHashWithBuildSideOuterJoin1() throws Exception {
		final int numKeys = 20000;
		final int buildValsPerKey = 1;
		final int probeValsPerKey = 1;

		// create a build input that gives 40000 pairs with 1 values sharing the same key
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(2 * numKeys, buildValsPerKey, false);

		// create a probe input that gives 20000 pairs with 1 values sharing a key
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);
		MemoryManager memManager = MemoryManagerBuilder.newBuilder().setMemorySize(35 * PAGE_SIZE).build();
		// allocate the memory for the HashTable
		final BinaryHashTable table = new BinaryHashTable(conf, new Object(),
				this.buildSideSerializer, this.probeSideSerializer,
				new MyProjection(), new MyProjection(),
				memManager, 35 * PAGE_SIZE, ioManager, 24, 200000, true,
				HashJoinType.BUILD_OUTER, null, true,
				new boolean[]{true}, false);

		int numRecordsInJoinResult = join(table, buildInput, probeInput, true);

		Assert.assertEquals("Wrong number of records in join result.", 2 * numKeys * buildValsPerKey * probeValsPerKey, numRecordsInJoinResult);

		table.close();
		table.free();
	}

	@Test
	public void testHashWithBuildSideOuterJoin2() throws Exception {
		final int numKeys = 40000;
		final int buildValsPerKey = 2;
		final int probeValsPerKey = 1;

		// The keys of probe and build sides are overlapped, so there would be none unmatched build elements
		// after probe phase, make sure build side outer join works well in this case.

		// create a build input that gives 80000 pairs with 2 values sharing the same key
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

		// create a probe input that gives 40000 pairs with 1 values sharing a key
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		// allocate the memory for the HashTable
		MemoryManager memManager = MemoryManagerBuilder.newBuilder().setMemorySize(35 * PAGE_SIZE).build();
		final BinaryHashTable table = newBinaryHashTable(
				this.buildSideSerializer, this.probeSideSerializer,
				new MyProjection(), new MyProjection(), memManager,
				33 * PAGE_SIZE, ioManager);

		// ----------------------------------------------------------------------------------------

		int numRecordsInJoinResult = join(table, buildInput, probeInput, true);
		Assert.assertEquals("Wrong number of records in join result.", numKeys * buildValsPerKey * probeValsPerKey, numRecordsInJoinResult);

		table.close();
		table.free();
	}

	@Test
	public void testRepeatBuildJoin() throws Exception {
		final int numKeys = 500;
		final int probeValsPerKey = 1;
		MemoryManager memManager = MemoryManagerBuilder.newBuilder().setMemorySize(40 * PAGE_SIZE).build();
		MutableObjectIterator<BinaryRowData> buildInput = new MutableObjectIterator<BinaryRowData>() {

			int cnt = 0;

			@Override
			public BinaryRowData next(BinaryRowData reuse) throws IOException {
				return next();
			}

			@Override
			public BinaryRowData next() throws IOException {
				cnt++;
				if (cnt > numKeys) {
					return null;
				}
				BinaryRowData row = new BinaryRowData(2);
				BinaryRowWriter writer = new BinaryRowWriter(row);
				writer.writeInt(0, 1);
				writer.writeInt(1, 1);
				writer.complete();
				return row;
			}
		};
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		final BinaryHashTable table = new BinaryHashTable(conf, new Object(),
				buildSideSerializer, probeSideSerializer, new MyProjection(), new MyProjection(), memManager,
				40 * PAGE_SIZE, ioManager, 24, 200000,
				true, HashJoinType.INNER, null, false, new boolean[] {true}, true);

		int numRecordsInJoinResult = join(table, buildInput, probeInput, true);
		Assert.assertEquals("Wrong number of records in join result.",
				1, numRecordsInJoinResult);

		table.close();
		table.free();
	}

	@Test
	public void testRepeatBuildJoinWithSpill() throws Exception {
		final int numKeys = 30000;
		final int numRows = 300000;
		final int probeValsPerKey = 1;

		MutableObjectIterator<BinaryRowData> buildInput = new MutableObjectIterator<BinaryRowData>() {

			int cnt = 0;

			@Override
			public BinaryRowData next(BinaryRowData reuse) throws IOException {
				return next();
			}

			@Override
			public BinaryRowData next() throws IOException {
				cnt++;
				if (cnt > numRows) {
					return null;
				}
				int value = cnt % numKeys;
				BinaryRowData row = new BinaryRowData(2);
				BinaryRowWriter writer = new BinaryRowWriter(row);
				writer.writeInt(0, value);
				writer.writeInt(1, value);
				writer.complete();
				return row;
			}
		};
		MemoryManager memManager = MemoryManagerBuilder.newBuilder().setMemorySize(35 * PAGE_SIZE).build();
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		final BinaryHashTable table = new BinaryHashTable(
				conf,
				new Object(),
				buildSideSerializer,
				probeSideSerializer,
				new MyProjection(),
				new MyProjection(),
				memManager,
				35 * PAGE_SIZE,
				ioManager,
				24,
				200000,
				true,
				HashJoinType.INNER,
				null,
				false,
				new boolean[] {true}, true);

		int numRecordsInJoinResult = join(table, buildInput, probeInput, true);
		Assert.assertTrue("Wrong number of records in join result.",
				numRecordsInJoinResult < numRows);

		table.close();
		table.free();
	}

	@Test
	public void testBinaryHashBucketAreaNotEnoughMem() throws IOException {
		MemoryManager memManager = MemoryManagerBuilder.newBuilder().setMemorySize(35 * PAGE_SIZE).build();
		BinaryHashTable table = newBinaryHashTable(
				this.buildSideSerializer, this.probeSideSerializer,
				new MyProjection(), new MyProjection(), memManager,
				35 * PAGE_SIZE, ioManager);
		BinaryHashBucketArea area = new BinaryHashBucketArea(table, 100, 1, false);
		for (int i = 0; i < 100000; i++) {
			area.insertToBucket(i, i, true);
		}
		area.freeMemory();
		table.close();
		Assert.assertEquals(35, table.getInternalPool().freePages());
	}

	// ============================================================================================

	/**
	 * An iterator that returns the Key/Value pairs with identical value a given number of times.
	 */
	public static final class ConstantsKeyValuePairsIterator implements MutableObjectIterator<BinaryRowData> {

		private final IntValue key;
		private final IntValue value;

		private int numLeft;

		public ConstantsKeyValuePairsIterator(int key, int value, int count) {
			this.key = new IntValue(key);
			this.value = new IntValue(value);
			this.numLeft = count;
		}

		@Override
		public BinaryRowData next(BinaryRowData reuse) {
			if (this.numLeft > 0) {
				this.numLeft--;

				BinaryRowWriter writer = new BinaryRowWriter(reuse);
				writer.writeInt(0, this.key.getValue());
				writer.writeInt(1, this.value.getValue());
				writer.complete();
				return reuse;
			} else {
				return null;
			}
		}

		@Override
		public BinaryRowData next() {
			return next(new BinaryRowData(2));
		}
	}

	private BinaryHashTable newBinaryHashTable(
			BinaryRowDataSerializer buildSideSerializer,
			BinaryRowDataSerializer probeSideSerializer,
			Projection<RowData, BinaryRowData> buildSideProjection,
			Projection<RowData, BinaryRowData> probeSideProjection,
			MemoryManager memoryManager,
			long memory,
			IOManager ioManager) {
		return new BinaryHashTable(
				conf,
				new Object(),
				buildSideSerializer,
				probeSideSerializer,
				buildSideProjection,
				probeSideProjection,
				memoryManager,
				memory,
				ioManager,
				24,
				200000,
				true,
				HashJoinType.INNER,
				null,
				false,
				new boolean[]{true}, false);
	}

	private static final class MyProjection implements Projection<RowData, BinaryRowData> {

		BinaryRowData innerRow = new BinaryRowData(1);
		BinaryRowWriter writer = new BinaryRowWriter(innerRow);

		@Override
		public BinaryRowData apply(RowData row) {
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
}
