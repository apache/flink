/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.sort;

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

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordComparator;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordPairComparator;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordSerializer;
import eu.stratosphere.pact.runtime.test.util.DiscardingOutputCollector;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

/**
 */
public class SortMergeMatchIteratorITCase
{
	// total memory
	private static final int MEMORY_SIZE = 1024 * 1024 * 16;
	private static final int PAGES_FOR_BNLJN = 2;

	// the size of the left and right inputs
	private static final int INPUT_1_SIZE = 20000;

	private static final int INPUT_2_SIZE = 1000;

	// random seeds for the left and right input data generators
	private static final long SEED1 = 561349061987311L;

	private static final long SEED2 = 231434613412342L;
	
	// dummy abstract task
	private final AbstractTask parentTask = new DummyInvokable();

	private IOManager ioManager;
	private MemoryManager memoryManager;
	
	private TypeSerializer<Record> serializer1;
	private TypeSerializer<Record> serializer2;
	private TypeComparator<Record> comparator1;
	private TypeComparator<Record> comparator2;
	private TypePairComparator<Record, Record> pairComparator;
	

	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest()
	{
		this.serializer1 = RecordSerializer.get();
		this.serializer2 = RecordSerializer.get();
		this.comparator1 = new RecordComparator(new int[] {0}, new Class[]{TestData.Key.class});
		this.comparator2 = new RecordComparator(new int[] {0}, new Class[]{TestData.Key.class});
		this.pairComparator = new RecordPairComparator(new int[] {0}, new int[] {0}, new Class[]{TestData.Key.class});
		
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, 1);
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


	
	@Test
	public void testMerge() {
		try {
			
			final TestData.Generator generator1 = new Generator(SEED1, 500, 4096, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);
			final TestData.Generator generator2 = new Generator(SEED2, 500, 2048, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);

			final TestData.GeneratorIterator input1 = new TestData.GeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.GeneratorIterator input2 = new TestData.GeneratorIterator(generator2, INPUT_2_SIZE);
			
			// collect expected data
			final Map<TestData.Key, Collection<Match>> expectedMatchesMap = matchValues(
				collectData(input1),
				collectData(input2));
			
			final JoinFunction matcher = new MatchRemovingMatcher(expectedMatchesMap);
			
			final Collector<Record> collector = new DiscardingOutputCollector<Record>();
	
			// reset the generators
			generator1.reset();
			generator2.reset();
			input1.reset();
			input2.reset();
	
			// compare with iterator values
			MergeMatchIterator<Record, Record, Record> iterator = 
				new MergeMatchIterator<Record, Record, Record>(
					input1, input2, this.serializer1, this.comparator1, this.serializer2, this.comparator2,
					this.pairComparator, this.memoryManager, this.ioManager, PAGES_FOR_BNLJN, this.parentTask);
	
			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<TestData.Key, Collection<Match>> entry : expectedMatchesMap.entrySet()) {
				Assert.assertTrue("Collection for key " + entry.getKey() + " is not empty", entry.getValue().isEmpty());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
	
	@Test
	public void testMergeWithHighNumberOfCommonKeys()
	{
		// the size of the left and right inputs
		final int INPUT_1_SIZE = 200;
		final int INPUT_2_SIZE = 100;
		
		final int INPUT_1_DUPLICATES = 10;
		final int INPUT_2_DUPLICATES = 4000;
		final int DUPLICATE_KEY = 13;
		
		try {
			final TestData.Generator generator1 = new Generator(SEED1, 500, 4096, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);
			final TestData.Generator generator2 = new Generator(SEED2, 500, 2048, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);
			
			final TestData.GeneratorIterator gen1Iter = new TestData.GeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.GeneratorIterator gen2Iter = new TestData.GeneratorIterator(generator2, INPUT_2_SIZE);
			
			final TestData.ConstantValueIterator const1Iter = new TestData.ConstantValueIterator(DUPLICATE_KEY, "LEFT String for Duplicate Keys", INPUT_1_DUPLICATES);
			final TestData.ConstantValueIterator const2Iter = new TestData.ConstantValueIterator(DUPLICATE_KEY, "RIGHT String for Duplicate Keys", INPUT_2_DUPLICATES);
			
			final List<MutableObjectIterator<Record>> inList1 = new ArrayList<MutableObjectIterator<Record>>();
			inList1.add(gen1Iter);
			inList1.add(const1Iter);
			
			final List<MutableObjectIterator<Record>> inList2 = new ArrayList<MutableObjectIterator<Record>>();
			inList2.add(gen2Iter);
			inList2.add(const2Iter);
			
			MutableObjectIterator<Record> input1 = new MergeIterator<Record>(inList1, serializer1, comparator1.duplicate());
			MutableObjectIterator<Record> input2 = new MergeIterator<Record>(inList2, serializer2, comparator2.duplicate());
			
			// collect expected data
			final Map<TestData.Key, Collection<Match>> expectedMatchesMap = matchValues(
				collectData(input1),
				collectData(input2));
			
			// re-create the whole thing for actual processing
			
			// reset the generators and iterators
			generator1.reset();
			generator2.reset();
			const1Iter.reset();
			const2Iter.reset();
			gen1Iter.reset();
			gen2Iter.reset();
			
			inList1.clear();
			inList1.add(gen1Iter);
			inList1.add(const1Iter);
			
			inList2.clear();
			inList2.add(gen2Iter);
			inList2.add(const2Iter);
	
			input1 = new MergeIterator<Record>(inList1, serializer1, comparator1.duplicate());
			input2 = new MergeIterator<Record>(inList2, serializer2, comparator2.duplicate());
			
			final JoinFunction matcher = new MatchRemovingMatcher(expectedMatchesMap);
			
			final Collector<Record> collector = new DiscardingOutputCollector<Record>();
	
			
			// we create this sort-merge iterator with little memory for the block-nested-loops fall-back to make sure it
			// needs to spill for the duplicate keys
			MergeMatchIterator<Record, Record, Record> iterator = 
				new MergeMatchIterator<Record, Record, Record>(
					input1, input2, this.serializer1, this.comparator1, this.serializer2, this.comparator2,
					this.pairComparator, this.memoryManager, this.ioManager, PAGES_FOR_BNLJN, this.parentTask);
	
			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<TestData.Key, Collection<Match>> entry : expectedMatchesMap.entrySet()) {
				if (!entry.getValue().isEmpty()) {
					Assert.fail("Collection for key " + entry.getKey() + " is not empty");
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
	
	
	
	// --------------------------------------------------------------------------------------------
	//                                    Utilities
	// --------------------------------------------------------------------------------------------

	private Map<TestData.Key, Collection<Match>> matchValues(
			Map<TestData.Key, Collection<TestData.Value>> leftMap,
			Map<TestData.Key, Collection<TestData.Value>> rightMap)
	{
		Map<TestData.Key, Collection<Match>> map = new HashMap<TestData.Key, Collection<Match>>();

		for (TestData.Key key : leftMap.keySet()) {
			Collection<TestData.Value> leftValues = leftMap.get(key);
			Collection<TestData.Value> rightValues = rightMap.get(key);

			if (rightValues == null) {
				continue;
			}

			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<Match>());
			}

			Collection<Match> matchedValues = map.get(key);

			for (TestData.Value leftValue : leftValues) {
				for (TestData.Value rightValue : rightValues) {
					matchedValues.add(new Match(leftValue, rightValue));
				}
			}
		}

		return map;
	}

	
	private Map<TestData.Key, Collection<TestData.Value>> collectData(MutableObjectIterator<Record> iter)
	throws Exception
	{
		Map<TestData.Key, Collection<TestData.Value>> map = new HashMap<TestData.Key, Collection<TestData.Value>>();
		Record pair = new Record();
		
		while ((pair = iter.next(pair)) != null) {
			TestData.Key key = pair.getField(0, TestData.Key.class);
			
			if (!map.containsKey(key)) {
				map.put(new TestData.Key(key.getKey()), new ArrayList<TestData.Value>());
			}

			Collection<TestData.Value> values = map.get(key);
			values.add(new TestData.Value(pair.getField(1, TestData.Value.class).getValue()));
		}

		return map;
	}

	/**
	 * Private class used for storage of the expected matches in a hashmap.
	 */
	private static class Match {
		private final Value left;

		private final Value right;

		public Match(Value left, Value right) {
			this.left = left;
			this.right = right;
		}

		@Override
		public boolean equals(Object obj) {
			Match o = (Match) obj;
			return this.left.equals(o.left) && this.right.equals(o.right);
		}
		
		@Override
		public int hashCode() {
			return this.left.hashCode() ^ this.right.hashCode();
		}

		@Override
		public String toString() {
			return left + ", " + right;
		}
	}
	
	private static final class MatchRemovingMatcher extends JoinFunction {
		private static final long serialVersionUID = 1L;
		
		private final Map<TestData.Key, Collection<Match>> toRemoveFrom;
		
		protected MatchRemovingMatcher(Map<TestData.Key, Collection<Match>> map) {
			this.toRemoveFrom = map;
		}
		
		@Override
		public void join(Record rec1, Record rec2, Collector<Record> out)
		{
			TestData.Key key = rec1.getField(0, TestData.Key.class);
			TestData.Value value1 = rec1.getField(1, TestData.Value.class);
			TestData.Value value2 = rec2.getField(1, TestData.Value.class);
			
			Collection<Match> matches = this.toRemoveFrom.get(key);
			if (matches == null) {
				Assert.fail("Match " + key + " - " + value1 + ":" + value2 + " is unexpected.");
			}
			
			boolean contained = matches.remove(new Match(value1, value2));
			if (!contained) {
				Assert.fail("Produced match was not contained: " + key + " - " + value1 + ":" + value2);
			}
			if (matches.isEmpty()) {
				this.toRemoveFrom.remove(key);
			}
		}
	}
}
