package eu.stratosphere.pact.runtime.hash;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypePairComparator;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.hash.HashMatchIteratorITCase.PactRecordMatchRemovingMatcher;
import eu.stratosphere.pact.runtime.hash.HashMatchIteratorITCase.RecordMatch;
import eu.stratosphere.pact.runtime.hash.HashTableITCase.ConstantsKeyValuePairsIterator;
import eu.stratosphere.pact.runtime.hash.MutableHashTable.HashBucketIterator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordPairComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializer;
import eu.stratosphere.pact.runtime.test.util.DiscardingOutputCollector;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.UniformPactRecordGenerator;
import eu.stratosphere.pact.runtime.test.util.UnionIterator;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator;
import eu.stratosphere.pact.runtime.test.util.TestData.Key;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;

/**
 * Test specialized hash join that keeps the build side data (in memory and on hard disk)
 * This is used for iterative tasks.
 */

public class ReOpenableHashTableITCase {
	
	private static final int PAGE_SIZE = 8 * 1024;
	private static final long MEMORY_SIZE = PAGE_SIZE * 1000; // 100 Pages.

	private static final long SEED1 = 561349061987311L;
	private static final long SEED2 = 231434613412342L;
	
	private static final int NUM_PROBES = 5; // number of reopenings of hash join
	
	private final AbstractTask parentTask = new DummyInvokable();

	private IOManager ioManager;
	private MemoryManager memoryManager;
	
	private TypeSerializer<PactRecord> recordSerializer;
	private TypeComparator<PactRecord> record1Comparator;
	private TypeComparator<PactRecord> record2Comparator;
	private TypePairComparator<PactRecord, PactRecord> recordPairComparator;
	
	
	
	
	private static final AbstractInvokable MEM_OWNER = new DummyInvokable();
	private TypeSerializer<PactRecord> recordBuildSideAccesssor;
	private TypeSerializer<PactRecord> recordProbeSideAccesssor;
	private TypeComparator<PactRecord> recordBuildSideComparator;
	private TypeComparator<PactRecord> recordProbeSideComparator;
	private TypePairComparator<PactRecord, PactRecord> pactRecordComparator;
	

	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest()
	{
		this.recordSerializer = PactRecordSerializer.get();
		
		this.record1Comparator = new PactRecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
		this.record2Comparator = new PactRecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
		this.recordPairComparator = new PactRecordPairComparator(new int[] {0}, new int[] {0}, new Class[] {TestData.Key.class});
		
		
		final int[] keyPos = new int[] {0};
		final Class<? extends Key>[] keyType = (Class<? extends Key>[]) new Class[] { PactInteger.class };
		
		this.recordBuildSideAccesssor = PactRecordSerializer.get();
		this.recordProbeSideAccesssor = PactRecordSerializer.get();
		this.recordBuildSideComparator = new PactRecordComparator(keyPos, keyType);
		this.recordProbeSideComparator = new PactRecordComparator(keyPos, keyType);
		this.pactRecordComparator = new HashTableITCase.PactRecordPairComparatorFirstInt();
		
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, PAGE_SIZE);
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
		final MatchStub[] nMatcher = new PactRecordMatchRemovingMatcher[NUM_PROBES];
		for(int i = 0; i < NUM_PROBES; i++) {
			Map<TestData.Key, Collection<RecordMatch>> tmp;
			expectedNMatchesMapList.add(tmp = deepCopy(expectedFirstMatchesMap));
			nMatcher[i] = new PactRecordMatchRemovingMatcher(tmp);
		}
		
		final MatchStub firstMatcher = new PactRecordMatchRemovingMatcher(expectedFirstMatchesMap);
		
		final Collector<PactRecord> collector = new DiscardingOutputCollector();

		// reset the generators
		bgen.reset();
		pgen.reset();
		buildInput.reset();
		probeInput.reset();

		// compare with iterator values
		BuildFirstReOpenableHashMatchIterator<PactRecord, PactRecord, PactRecord> iterator = 
				new BuildFirstReOpenableHashMatchIterator<PactRecord, PactRecord, PactRecord>(
						buildInput, probeInput, this.recordSerializer, this.record1Comparator, 
					this.recordSerializer, this.record2Comparator, this.recordPairComparator,
					this.memoryManager, ioManager, this.parentTask, MEMORY_SIZE);
		
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
	
	private final MutableObjectIterator<PactRecord> getProbeInput(final int numKeys,
			final int probeValsPerKey, final int repeatedValue1, final int repeatedValue2) {
		MutableObjectIterator<PactRecord> probe1 = new UniformPactRecordGenerator(numKeys, probeValsPerKey, true);
		MutableObjectIterator<PactRecord> probe2 = new ConstantsKeyValuePairsIterator(repeatedValue1, 17, 5);
		MutableObjectIterator<PactRecord> probe3 = new ConstantsKeyValuePairsIterator(repeatedValue2, 23, 5);
		List<MutableObjectIterator<PactRecord>> probes = new ArrayList<MutableObjectIterator<PactRecord>>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		return new UnionIterator<PactRecord>(probes);
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
		
		final ReOpenableMutableHashTable<PactRecord, PactRecord> join = new ReOpenableMutableHashTable<PactRecord, PactRecord>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, 
				this.recordBuildSideComparator, this.recordProbeSideComparator, this.pactRecordComparator,
				memSegments, ioManager);
		
		for(int probe = 0; probe < NUM_PROBES; probe++) {
			// create a probe input that gives 10 million pairs with 10 values sharing a key
			MutableObjectIterator<PactRecord> probeInput = getProbeInput(NUM_KEYS, PROBE_VALS_PER_KEY, REPEATED_VALUE_1, REPEATED_VALUE_2);
			if(probe == 0) {
				join.open(buildInput, probeInput);
			} else {
				join.reopenProbe(probeInput);
			}
		
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
		MutableObjectIterator<PactRecord> build1 = new UniformPactRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
		MutableObjectIterator<PactRecord> build2 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
		MutableObjectIterator<PactRecord> build3 = new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
		List<MutableObjectIterator<PactRecord>> builds = new ArrayList<MutableObjectIterator<PactRecord>>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<PactRecord> buildInput = new UnionIterator<PactRecord>(builds);
	

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
		
		final ReOpenableMutableHashTable<PactRecord, PactRecord> join = new ReOpenableMutableHashTable<PactRecord, PactRecord>(
				this.recordBuildSideAccesssor, this.recordProbeSideAccesssor, 
				this.recordBuildSideComparator, this.recordProbeSideComparator, this.pactRecordComparator,
				memSegments, ioManager);
		for(int probe = 0; probe < NUM_PROBES; probe++) {
			// create a probe input that gives 10 million pairs with 10 values sharing a key
			MutableObjectIterator<PactRecord> probeInput = getProbeInput(NUM_KEYS, PROBE_VALS_PER_KEY, REPEATED_VALUE_1, REPEATED_VALUE_2);
			if(probe == 0) {
				join.open(buildInput, probeInput);
			} else {
				join.reopenProbe(probeInput);
			}
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
