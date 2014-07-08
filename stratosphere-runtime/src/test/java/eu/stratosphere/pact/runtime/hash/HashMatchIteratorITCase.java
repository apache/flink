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

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericJoiner;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordComparator;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordPairComparator;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordSerializer;
import eu.stratosphere.pact.runtime.test.util.DiscardingOutputCollector;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;
import eu.stratosphere.pact.runtime.test.util.UniformIntPairGenerator;
import eu.stratosphere.pact.runtime.test.util.UnionIterator;
import eu.stratosphere.pact.runtime.test.util.types.IntPair;
import eu.stratosphere.pact.runtime.test.util.types.IntPairComparator;
import eu.stratosphere.pact.runtime.test.util.types.IntPairSerializer;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.NullKeyFieldException;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

@SuppressWarnings("serial")
public class HashMatchIteratorITCase {
	
	private static final int MEMORY_SIZE = 16000000;		// total memory

	private static final int INPUT_1_SIZE = 20000;
	private static final int INPUT_2_SIZE = 1000;

	private static final long SEED1 = 561349061987311L;
	private static final long SEED2 = 231434613412342L;
	
	private final AbstractInvokable parentTask = new DummyInvokable();

	private IOManager ioManager;
	private MemoryManager memoryManager;
	
	private TypeSerializer<Record> recordSerializer;
	private TypeComparator<Record> record1Comparator;
	private TypeComparator<Record> record2Comparator;
	private TypePairComparator<Record, Record> recordPairComparator;
	
	private TypeSerializer<IntPair> pairSerializer;
	private TypeComparator<IntPair> pairComparator;
	private TypePairComparator<IntPair, Record> pairRecordPairComparator;
	private TypePairComparator<Record, IntPair> recordPairPairComparator;


	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest() {
		this.recordSerializer = RecordSerializer.get();
		
		this.record1Comparator = new RecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
		this.record2Comparator = new RecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
		
		this.recordPairComparator = new RecordPairComparator(new int[] {0}, new int[] {0}, new Class[] {TestData.Key.class});
		
		this.pairSerializer = new IntPairSerializer();
		this.pairComparator = new IntPairComparator();
		this.pairRecordPairComparator = new IntPairRecordPairComparator();
		this.recordPairPairComparator = new RecordIntPairPairComparator();
		
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
	public void testBuildFirst() {
		try {
			Generator generator1 = new Generator(SEED1, 500, 4096, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			Generator generator2 = new Generator(SEED2, 500, 2048, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			
			final TestData.GeneratorIterator input1 = new TestData.GeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.GeneratorIterator input2 = new TestData.GeneratorIterator(generator2, INPUT_2_SIZE);
			
			// collect expected data
			final Map<TestData.Key, Collection<RecordMatch>> expectedMatchesMap = matchRecordValues(
				collectRecordData(input1),
				collectRecordData(input2));
			
			final JoinFunction matcher = new RecordMatchRemovingJoin(expectedMatchesMap);
			final Collector<Record> collector = new DiscardingOutputCollector<Record>();
	
			// reset the generators
			generator1.reset();
			generator2.reset();
			input1.reset();
			input2.reset();
	
			// compare with iterator values
			BuildFirstHashMatchIterator<Record, Record, Record> iterator = 
					new BuildFirstHashMatchIterator<Record, Record, Record>(
						input1, input2, this.recordSerializer, this.record1Comparator, 
						this.recordSerializer, this.record2Comparator, this.recordPairComparator,
						this.memoryManager, ioManager, this.parentTask, 1.0);
			
			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<TestData.Key, Collection<RecordMatch>> entry : expectedMatchesMap.entrySet()) {
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
	
	@Test
	public void testBuildFirstWithHighNumberOfCommonKeys()
	{
		// the size of the left and right inputs
		final int INPUT_1_SIZE = 200;
		final int INPUT_2_SIZE = 100;
		
		final int INPUT_1_DUPLICATES = 10;
		final int INPUT_2_DUPLICATES = 2000;
		final int DUPLICATE_KEY = 13;
		
		try {
			Generator generator1 = new Generator(SEED1, 500, 4096, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			Generator generator2 = new Generator(SEED2, 500, 2048, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			
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
			
			MutableObjectIterator<Record> input1 = new UnionIterator<Record>(inList1);
			MutableObjectIterator<Record> input2 = new UnionIterator<Record>(inList2);
			
			
			// collect expected data
			final Map<TestData.Key, Collection<RecordMatch>> expectedMatchesMap = matchRecordValues(
				collectRecordData(input1),
				collectRecordData(input2));
			
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
	
			input1 = new UnionIterator<Record>(inList1);
			input2 = new UnionIterator<Record>(inList2);
			
			final JoinFunction matcher = new RecordMatchRemovingJoin(expectedMatchesMap);
			final Collector<Record> collector = new DiscardingOutputCollector<Record>();
	
			BuildFirstHashMatchIterator<Record, Record, Record> iterator = 
					new BuildFirstHashMatchIterator<Record, Record, Record>(
						input1, input2, this.recordSerializer, this.record1Comparator, 
						this.recordSerializer, this.record2Comparator, this.recordPairComparator,
						this.memoryManager, ioManager, this.parentTask, 1.0);

			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<TestData.Key, Collection<RecordMatch>> entry : expectedMatchesMap.entrySet()) {
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
	
	@Test
	public void testBuildSecond() {
		try {
			Generator generator1 = new Generator(SEED1, 500, 4096, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			Generator generator2 = new Generator(SEED2, 500, 2048, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			
			final TestData.GeneratorIterator input1 = new TestData.GeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.GeneratorIterator input2 = new TestData.GeneratorIterator(generator2, INPUT_2_SIZE);
			
			// collect expected data
			final Map<TestData.Key, Collection<RecordMatch>> expectedMatchesMap = matchRecordValues(
				collectRecordData(input1),
				collectRecordData(input2));
			
			final JoinFunction matcher = new RecordMatchRemovingJoin(expectedMatchesMap);
			final Collector<Record> collector = new DiscardingOutputCollector<Record>();
	
			// reset the generators
			generator1.reset();
			generator2.reset();
			input1.reset();
			input2.reset();
	
			// compare with iterator values			
			BuildSecondHashMatchIterator<Record, Record, Record> iterator = 
				new BuildSecondHashMatchIterator<Record, Record, Record>(
					input1, input2, this.recordSerializer, this.record1Comparator, 
					this.recordSerializer, this.record2Comparator, this.recordPairComparator,
					this.memoryManager, ioManager, this.parentTask, 1.0);

			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<TestData.Key, Collection<RecordMatch>> entry : expectedMatchesMap.entrySet()) {
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
	
	@Test
	public void testBuildSecondWithHighNumberOfCommonKeys()
	{
		// the size of the left and right inputs
		final int INPUT_1_SIZE = 200;
		final int INPUT_2_SIZE = 100;
		
		final int INPUT_1_DUPLICATES = 10;
		final int INPUT_2_DUPLICATES = 2000;
		final int DUPLICATE_KEY = 13;
		
		try {
			Generator generator1 = new Generator(SEED1, 500, 4096, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			Generator generator2 = new Generator(SEED2, 500, 2048, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			
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
			
			MutableObjectIterator<Record> input1 = new UnionIterator<Record>(inList1);
			MutableObjectIterator<Record> input2 = new UnionIterator<Record>(inList2);
			
			
			// collect expected data
			final Map<TestData.Key, Collection<RecordMatch>> expectedMatchesMap = matchRecordValues(
				collectRecordData(input1),
				collectRecordData(input2));
			
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
	
			input1 = new UnionIterator<Record>(inList1);
			input2 = new UnionIterator<Record>(inList2);
			
			final JoinFunction matcher = new RecordMatchRemovingJoin(expectedMatchesMap);
			final Collector<Record> collector = new DiscardingOutputCollector<Record>();
	
			BuildSecondHashMatchIterator<Record, Record, Record> iterator = 
				new BuildSecondHashMatchIterator<Record, Record, Record>(
					input1, input2, this.recordSerializer, this.record1Comparator, 
					this.recordSerializer, this.record2Comparator, this.recordPairComparator,
					this.memoryManager, ioManager, this.parentTask, 1.0);
			
			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<TestData.Key, Collection<RecordMatch>> entry : expectedMatchesMap.entrySet()) {
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
	
	@Test
	public void testBuildFirstWithMixedDataTypes() {
		try {
			MutableObjectIterator<IntPair> input1 = new UniformIntPairGenerator(500, 40, false);
			
			final Generator generator2 = new Generator(SEED2, 500, 2048, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			final TestData.GeneratorIterator input2 = new TestData.GeneratorIterator(generator2, INPUT_2_SIZE);
			
			// collect expected data
			final Map<TestData.Key, Collection<RecordIntPairMatch>> expectedMatchesMap = matchRecordIntPairValues(
				collectIntPairData(input1),
				collectRecordData(input2));
			
			final GenericJoiner<IntPair, Record, Record> matcher = new RecordIntPairMatchRemovingMatcher(expectedMatchesMap);
			final Collector<Record> collector = new DiscardingOutputCollector<Record>();
	
			// reset the generators
			input1 = new UniformIntPairGenerator(500, 40, false);
			generator2.reset();
			input2.reset();
	
			// compare with iterator values
			BuildSecondHashMatchIterator<IntPair, Record, Record> iterator = 
					new BuildSecondHashMatchIterator<IntPair, Record, Record>(
						input1, input2, this.pairSerializer, this.pairComparator, 
						this.recordSerializer, this.record2Comparator, this.pairRecordPairComparator,
						this.memoryManager, this.ioManager, this.parentTask, 1.0);
			
			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();;
	
			// assert that each expected match was seen
			for (Entry<TestData.Key, Collection<RecordIntPairMatch>> entry : expectedMatchesMap.entrySet()) {
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
	
	@Test
	public void testBuildSecondWithMixedDataTypes() {
		try {
			MutableObjectIterator<IntPair> input1 = new UniformIntPairGenerator(500, 40, false);
			
			final Generator generator2 = new Generator(SEED2, 500, 2048, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			final TestData.GeneratorIterator input2 = new TestData.GeneratorIterator(generator2, INPUT_2_SIZE);
			
			// collect expected data
			final Map<TestData.Key, Collection<RecordIntPairMatch>> expectedMatchesMap = matchRecordIntPairValues(
				collectIntPairData(input1),
				collectRecordData(input2));
			
			final GenericJoiner<IntPair, Record, Record> matcher = new RecordIntPairMatchRemovingMatcher(expectedMatchesMap);
			final Collector<Record> collector = new DiscardingOutputCollector<Record>();
	
			// reset the generators
			input1 = new UniformIntPairGenerator(500, 40, false);
			generator2.reset();
			input2.reset();
	
			// compare with iterator values
			BuildFirstHashMatchIterator<IntPair, Record, Record> iterator = 
					new BuildFirstHashMatchIterator<IntPair, Record, Record>(
						input1, input2, this.pairSerializer, this.pairComparator, 
						this.recordSerializer, this.record2Comparator, this.recordPairPairComparator,
						this.memoryManager, this.ioManager, this.parentTask, 1.0);
			
			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();;
	
			// assert that each expected match was seen
			for (Entry<TestData.Key, Collection<RecordIntPairMatch>> entry : expectedMatchesMap.entrySet()) {
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

	
	
	static Map<TestData.Key, Collection<RecordMatch>> matchRecordValues(
			Map<TestData.Key, Collection<TestData.Value>> leftMap,
			Map<TestData.Key, Collection<TestData.Value>> rightMap)
	{
		Map<TestData.Key, Collection<RecordMatch>> map = new HashMap<TestData.Key, Collection<RecordMatch>>();

		for (TestData.Key key : leftMap.keySet()) {
			Collection<TestData.Value> leftValues = leftMap.get(key);
			Collection<TestData.Value> rightValues = rightMap.get(key);

			if (rightValues == null) {
				continue;
			}

			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<RecordMatch>());
			}

			Collection<RecordMatch> matchedValues = map.get(key);

			for (TestData.Value leftValue : leftValues) {
				for (TestData.Value rightValue : rightValues) {
					matchedValues.add(new RecordMatch(leftValue, rightValue));
				}
			}
		}

		return map;
	}
	
	static Map<TestData.Key, Collection<RecordIntPairMatch>> matchRecordIntPairValues(
		Map<Integer, Collection<Integer>> leftMap,
		Map<TestData.Key, Collection<TestData.Value>> rightMap)
	{
		final Map<TestData.Key, Collection<RecordIntPairMatch>> map = new HashMap<TestData.Key, Collection<RecordIntPairMatch>>();
	
		for (Integer i : leftMap.keySet()) {
			
			final TestData.Key key = new TestData.Key(i.intValue());
			
			final Collection<Integer> leftValues = leftMap.get(i);
			final Collection<TestData.Value> rightValues = rightMap.get(key);
	
			if (rightValues == null) {
				continue;
			}
	
			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<RecordIntPairMatch>());
			}
	
			final Collection<RecordIntPairMatch> matchedValues = map.get(key);
	
			for (Integer v : leftValues) {
				for (TestData.Value val : rightValues) {
					matchedValues.add(new RecordIntPairMatch(v, val));
				}
			}
		}
	
		return map;
	}

	
	static Map<TestData.Key, Collection<TestData.Value>> collectRecordData(MutableObjectIterator<Record> iter)
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
	
	static Map<Integer, Collection<Integer>> collectIntPairData(MutableObjectIterator<IntPair> iter)
	throws Exception
	{
		Map<Integer, Collection<Integer>> map = new HashMap<Integer, Collection<Integer>>();
		IntPair pair = new IntPair();
		
		while ((pair = iter.next(pair)) != null) {

			final int key = pair.getKey();
			final int value = pair.getValue();
			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<Integer>());
			}

			Collection<Integer> values = map.get(key);
			values.add(value);
		}

		return map;
	}

	/**
	 * Private class used for storage of the expected matches in a hash-map.
	 */
	static class RecordMatch {
		
		private final Value left;
		private final Value right;

		public RecordMatch(Value left, Value right) {
			this.left = left;
			this.right = right;
		}

		@Override
		public boolean equals(Object obj) {
			RecordMatch o = (RecordMatch) obj;
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
	
	/**
	 * Private class used for storage of the expected matches in a hash-map.
	 */
	static class RecordIntPairMatch
	{
		private final int left;
		private final Value right;

		public RecordIntPairMatch(int left, Value right) {
			this.left = left;
			this.right = right;
		}

		@Override
		public boolean equals(Object obj) {
			RecordIntPairMatch o = (RecordIntPairMatch) obj;
			return this.left == o.left && this.right.equals(o.right);
		}
		
		@Override
		public int hashCode() {
			return this.left ^ this.right.hashCode();
		}

		@Override
		public String toString() {
			return left + ", " + right;
		}
	}
	
	static final class RecordMatchRemovingJoin extends JoinFunction
	{
		private final Map<TestData.Key, Collection<RecordMatch>> toRemoveFrom;
		
		protected RecordMatchRemovingJoin(Map<TestData.Key, Collection<RecordMatch>> map) {
			this.toRemoveFrom = map;
		}
		
		@Override
		public void join(Record rec1, Record rec2, Collector<Record> out)
		{
			TestData.Key key = rec1.getField(0, TestData.Key.class);
			TestData.Value value1 = rec1.getField(1, TestData.Value.class);
			TestData.Value value2 = rec2.getField(1, TestData.Value.class);
			//System.err.println("rec1 key = "+key+"  rec2 key= "+rec2.getField(0, TestData.Key.class));
			Collection<RecordMatch> matches = this.toRemoveFrom.get(key);
			if (matches == null) {
				Assert.fail("Match " + key + " - " + value1 + ":" + value2 + " is unexpected.");
			}
			
			Assert.assertTrue("Produced match was not contained: " + key + " - " + value1 + ":" + value2,
				matches.remove(new RecordMatch(value1, value2)));
			
			if (matches.isEmpty()) {
				this.toRemoveFrom.remove(key);
			}
		}
	}
	
	static final class RecordIntPairMatchRemovingMatcher extends AbstractFunction implements GenericJoiner<IntPair, Record, Record>
	{
		private final Map<TestData.Key, Collection<RecordIntPairMatch>> toRemoveFrom;
		
		protected RecordIntPairMatchRemovingMatcher(Map<TestData.Key, Collection<RecordIntPairMatch>> map) {
			this.toRemoveFrom = map;
		}
		
		@Override
		public void join(IntPair rec1, Record rec2, Collector<Record> out)
		{
			final int k = rec1.getKey();
			final int v = rec1.getValue(); 
			
			final TestData.Key key = rec2.getField(0, TestData.Key.class);
			final TestData.Value value = rec2.getField(1, TestData.Value.class);
			
			Assert.assertTrue("Key does not match for matching IntPair Record combination.", k == key.getKey()); 
			
			Collection<RecordIntPairMatch> matches = this.toRemoveFrom.get(key);
			if (matches == null) {
				Assert.fail("Match " + key + " - " + v + ":" + value + " is unexpected.");
			}
			
			Assert.assertTrue("Produced match was not contained: " + key + " - " + v + ":" + value,
				matches.remove(new RecordIntPairMatch(v, value)));
			
			if (matches.isEmpty()) {
				this.toRemoveFrom.remove(key);
			}
		}
	}
	
	static final class IntPairRecordPairComparator extends TypePairComparator<IntPair, Record>
	{
		private int reference;
		
		@Override
		public void setReference(IntPair reference) {
			this.reference = reference.getKey();	
		}

		@Override
		public boolean equalToReference(Record candidate) {
			try {
				final IntValue i = candidate.getField(0, IntValue.class);
				return i.getValue() == this.reference;
			} catch (NullPointerException npex) {
				throw new NullKeyFieldException();
			}
		}

		@Override
		public int compareToReference(Record candidate) {
			try {
				final IntValue i = candidate.getField(0, IntValue.class);
				return i.getValue() - this.reference;
			} catch (NullPointerException npex) {
				throw new NullKeyFieldException();
			}
		}
	}
	
	static final class RecordIntPairPairComparator extends TypePairComparator<Record, IntPair>
	{
		private int reference;
		
		@Override
		public void setReference(Record reference) {
			this.reference = reference.getField(0, IntValue.class).getValue();
		}

		@Override
		public boolean equalToReference(IntPair candidate) {
			return this.reference == candidate.getKey();
		}

		@Override
		public int compareToReference(IntPair candidate) {
			return candidate.getKey() - this.reference;
		}
	}
}
