package eu.stratosphere.pact.runtime.hash;

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
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypePairComparator;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.hash.HashMatchIteratorITCase.PactRecordMatchRemovingMatcher;
import eu.stratosphere.pact.runtime.hash.HashMatchIteratorITCase.RecordMatch;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordPairComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializer;
import eu.stratosphere.pact.runtime.test.util.DiscardingOutputCollector;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator;
import eu.stratosphere.pact.runtime.test.util.TestData.Key;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;

/**
 * Test specialized hash join that keeps the build side data (in memory and on hard disk)
 * This is used for iterative tasks.
 */

public class ReOpenableHashTableTest {
	
	private static final int PAGE_SIZE = 8 * 1024;
	private static final long MEMORY_SIZE = PAGE_SIZE * 100; // 100 Pages.

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
	

	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest()
	{
		this.recordSerializer = PactRecordSerializer.get();
		
		this.record1Comparator = new PactRecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
		this.record2Comparator = new PactRecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
		
		this.recordPairComparator = new PactRecordPairComparator(new int[] {0}, new int[] {0}, new Class[] {TestData.Key.class});
		
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
