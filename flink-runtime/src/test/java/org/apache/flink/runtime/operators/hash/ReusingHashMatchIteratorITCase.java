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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.GenericPairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.UniformIntPairGenerator;
import org.apache.flink.runtime.operators.testutils.UnionIterator;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;
import org.apache.flink.runtime.operators.testutils.types.IntPair;
import org.apache.flink.runtime.operators.testutils.types.IntPairSerializer;
import org.apache.flink.types.NullKeyFieldException;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings({"serial", "deprecation"})
public class ReusingHashMatchIteratorITCase {
	
	private static final int MEMORY_SIZE = 16000000;		// total memory

	private static final int INPUT_1_SIZE = 20000;
	private static final int INPUT_2_SIZE = 1000;

	private static final long SEED1 = 561349061987311L;
	private static final long SEED2 = 231434613412342L;
	
	private final AbstractInvokable parentTask = new DummyInvokable();

	private IOManager ioManager;
	private MemoryManager memoryManager;
	
	private TypeSerializer<Tuple2<Integer, String>> recordSerializer;
	private TypeComparator<Tuple2<Integer, String>> record1Comparator;
	private TypeComparator<Tuple2<Integer, String>> record2Comparator;
	private TypePairComparator<Tuple2<Integer, String>, Tuple2<Integer, String>> recordPairComparator;
	
	private TypeSerializer<IntPair> pairSerializer;
	private TypeComparator<IntPair> pairComparator;
	private TypePairComparator<IntPair, Tuple2<Integer, String>> pairRecordPairComparator;
	private TypePairComparator<Tuple2<Integer, String>, IntPair> recordPairPairComparator;


	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest() {
		this.recordSerializer = TestData.getIntStringTupleSerializer();
		
		this.record1Comparator = TestData.getIntStringTupleComparator();
		this.record2Comparator = TestData.getIntStringTupleComparator();
		
		this.recordPairComparator = new GenericPairComparator(this.record1Comparator, this.record2Comparator);
		
		this.pairSerializer = new IntPairSerializer();
		this.pairComparator = new TestData.IntPairComparator();
		this.pairRecordPairComparator = new IntPairTuplePairComparator();
		this.recordPairPairComparator = new TupleIntPairPairComparator();
		
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1);
		this.ioManager = new IOManagerAsync();
	}

	@After
	public void afterTest() {
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
			TestData.TupleGenerator generator1 = new TestData.TupleGenerator(SEED1, 500, 4096, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			TestData.TupleGenerator generator2 = new TestData.TupleGenerator(SEED2, 500, 2048, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			
			final TestData.TupleGeneratorIterator input1 = new TestData.TupleGeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.TupleGeneratorIterator input2 = new TestData.TupleGeneratorIterator(generator2, INPUT_2_SIZE);
			
			// collect expected data
			final Map<Integer, Collection<TupleMatch>> expectedMatchesMap = matchSecondTupleFields(
				collectTupleData(input1),
				collectTupleData(input2));
			
			final FlatJoinFunction matcher = new TupleMatchRemovingJoin(expectedMatchesMap);
			final Collector<Tuple2<Integer, String>> collector = new DiscardingOutputCollector<>();
	
			// reset the generators
			generator1.reset();
			generator2.reset();
			input1.reset();
			input2.reset();
	
			// compare with iterator values
			ReusingBuildFirstHashMatchIterator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> iterator =
					new ReusingBuildFirstHashMatchIterator<>(
						input1, input2, this.recordSerializer, this.record1Comparator, 
						this.recordSerializer, this.record2Comparator, this.recordPairComparator,
						this.memoryManager, ioManager, this.parentTask, 1.0, true);
			
			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<Integer, Collection<TupleMatch>> entry : expectedMatchesMap.entrySet()) {
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
			TestData.TupleGenerator generator1 = new TestData.TupleGenerator(SEED1, 500, 4096, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			TestData.TupleGenerator generator2 = new TestData.TupleGenerator(SEED2, 500, 2048, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			
			final TestData.TupleGeneratorIterator gen1Iter = new TestData.TupleGeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.TupleGeneratorIterator gen2Iter = new TestData.TupleGeneratorIterator(generator2, INPUT_2_SIZE);
			
			final TestData.TupleConstantValueIterator const1Iter = new TestData.TupleConstantValueIterator(DUPLICATE_KEY, "LEFT String for Duplicate Keys", INPUT_1_DUPLICATES);
			final TestData.TupleConstantValueIterator const2Iter = new TestData.TupleConstantValueIterator(DUPLICATE_KEY, "RIGHT String for Duplicate Keys", INPUT_2_DUPLICATES);
			
			final List<MutableObjectIterator<Tuple2<Integer, String>>> inList1 = new ArrayList<>();
			inList1.add(gen1Iter);
			inList1.add(const1Iter);
			
			final List<MutableObjectIterator<Tuple2<Integer, String>>> inList2 = new ArrayList<>();
			inList2.add(gen2Iter);
			inList2.add(const2Iter);
			
			MutableObjectIterator<Tuple2<Integer, String>> input1 = new UnionIterator<>(inList1);
			MutableObjectIterator<Tuple2<Integer, String>> input2 = new UnionIterator<>(inList2);
			
			
			// collect expected data
			final Map<Integer, Collection<TupleMatch>> expectedMatchesMap = matchSecondTupleFields(
				collectTupleData(input1),
				collectTupleData(input2));
			
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
	
			input1 = new UnionIterator<Tuple2<Integer, String>>(inList1);
			input2 = new UnionIterator<Tuple2<Integer, String>>(inList2);
			
			final FlatJoinFunction matcher = new TupleMatchRemovingJoin(expectedMatchesMap);
			final Collector<Tuple2<Integer, String>> collector = new DiscardingOutputCollector<>();
	
			ReusingBuildFirstHashMatchIterator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> iterator =
					new ReusingBuildFirstHashMatchIterator<>(
						input1, input2, this.recordSerializer, this.record1Comparator, 
						this.recordSerializer, this.record2Comparator, this.recordPairComparator,
						this.memoryManager, ioManager, this.parentTask, 1.0, true);

			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<Integer, Collection<TupleMatch>> entry : expectedMatchesMap.entrySet()) {
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
			TestData.TupleGenerator generator1 = new TestData.TupleGenerator(SEED1, 500, 4096, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			TestData.TupleGenerator generator2 = new TestData.TupleGenerator(SEED2, 500, 2048, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			
			final TestData.TupleGeneratorIterator input1 = new TestData.TupleGeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.TupleGeneratorIterator input2 = new TestData.TupleGeneratorIterator(generator2, INPUT_2_SIZE);
			
			// collect expected data
			final Map<Integer, Collection<TupleMatch>> expectedMatchesMap = matchSecondTupleFields(
				collectTupleData(input1),
				collectTupleData(input2));
			
			final FlatJoinFunction matcher = new TupleMatchRemovingJoin(expectedMatchesMap);
			final Collector<Tuple2<Integer, String>> collector = new DiscardingOutputCollector<>();
	
			// reset the generators
			generator1.reset();
			generator2.reset();
			input1.reset();
			input2.reset();
	
			// compare with iterator values			
			ReusingBuildSecondHashMatchIterator<Tuple2<Integer, String>,Tuple2<Integer, String> ,Tuple2<Integer, String> > iterator =
				new ReusingBuildSecondHashMatchIterator<>(
					input1, input2, this.recordSerializer, this.record1Comparator, 
					this.recordSerializer, this.record2Comparator, this.recordPairComparator,
					this.memoryManager, ioManager, this.parentTask, 1.0, true);

			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<Integer, Collection<TupleMatch>> entry : expectedMatchesMap.entrySet()) {
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
			TestData.TupleGenerator generator1 = new TestData.TupleGenerator(SEED1, 500, 4096, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			TestData.TupleGenerator generator2 = new TestData.TupleGenerator(SEED2, 500, 2048, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			
			final TestData.TupleGeneratorIterator gen1Iter = new TestData.TupleGeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.TupleGeneratorIterator gen2Iter = new TestData.TupleGeneratorIterator(generator2, INPUT_2_SIZE);
			
			final TestData.TupleConstantValueIterator const1Iter = new TestData.TupleConstantValueIterator(DUPLICATE_KEY, "LEFT String for Duplicate Keys", INPUT_1_DUPLICATES);
			final TestData.TupleConstantValueIterator const2Iter = new TestData.TupleConstantValueIterator(DUPLICATE_KEY, "RIGHT String for Duplicate Keys", INPUT_2_DUPLICATES);
			
			final List<MutableObjectIterator<Tuple2<Integer, String>>> inList1 = new ArrayList<>();
			inList1.add(gen1Iter);
			inList1.add(const1Iter);
			
			final List<MutableObjectIterator<Tuple2<Integer, String>>> inList2 = new ArrayList<>();
			inList2.add(gen2Iter);
			inList2.add(const2Iter);
			
			MutableObjectIterator<Tuple2<Integer, String>> input1 = new UnionIterator<>(inList1);
			MutableObjectIterator<Tuple2<Integer, String>> input2 = new UnionIterator<>(inList2);
			
			
			// collect expected data
			final Map<Integer, Collection<TupleMatch>> expectedMatchesMap = matchSecondTupleFields(
				collectTupleData(input1),
				collectTupleData(input2));
			
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
	
			input1 = new UnionIterator<>(inList1);
			input2 = new UnionIterator<>(inList2);
			
			final FlatJoinFunction matcher = new TupleMatchRemovingJoin(expectedMatchesMap);
			final Collector<Tuple2<Integer, String>> collector = new DiscardingOutputCollector<>();
	
			ReusingBuildSecondHashMatchIterator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> iterator =
				new ReusingBuildSecondHashMatchIterator<>(
					input1, input2, this.recordSerializer, this.record1Comparator, 
					this.recordSerializer, this.record2Comparator, this.recordPairComparator,
					this.memoryManager, ioManager, this.parentTask, 1.0, true);
			
			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<Integer, Collection<TupleMatch>> entry : expectedMatchesMap.entrySet()) {
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
			
			final TestData.TupleGenerator generator2 = new TestData.TupleGenerator(SEED2, 500, 2048, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			final TestData.TupleGeneratorIterator input2 = new TestData.TupleGeneratorIterator(generator2, INPUT_2_SIZE);
			
			// collect expected data
			final Map<Integer, Collection<TupleIntPairMatch>> expectedMatchesMap = matchTupleIntPairValues(
				collectIntPairData(input1),
				collectTupleData(input2));
			
			final FlatJoinFunction<IntPair, Tuple2<Integer, String>, Tuple2<Integer, String>> matcher = new TupleIntPairMatchRemovingMatcher(expectedMatchesMap);
			final Collector<Tuple2<Integer, String>> collector = new DiscardingOutputCollector<>();
	
			// reset the generators
			input1 = new UniformIntPairGenerator(500, 40, false);
			generator2.reset();
			input2.reset();
	
			// compare with iterator values
			ReusingBuildSecondHashMatchIterator<IntPair, Tuple2<Integer, String>, Tuple2<Integer, String>> iterator =
					new ReusingBuildSecondHashMatchIterator<>(
						input1, input2, this.pairSerializer, this.pairComparator,
						this.recordSerializer, this.record2Comparator, this.pairRecordPairComparator,
						this.memoryManager, this.ioManager, this.parentTask, 1.0, true);
			
			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<Integer, Collection<TupleIntPairMatch>> entry : expectedMatchesMap.entrySet()) {
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
			
			final TestData.TupleGenerator generator2 = new TestData.TupleGenerator(SEED2, 500, 2048, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			final TestData.TupleGeneratorIterator input2 = new TestData.TupleGeneratorIterator(generator2, INPUT_2_SIZE);
			
			// collect expected data
			final Map<Integer, Collection<TupleIntPairMatch>> expectedMatchesMap = matchTupleIntPairValues(
				collectIntPairData(input1),
				collectTupleData(input2));
			
			final FlatJoinFunction<IntPair, Tuple2<Integer, String>, Tuple2<Integer, String>> matcher = new TupleIntPairMatchRemovingMatcher(expectedMatchesMap);
			final Collector<Tuple2<Integer, String>> collector = new DiscardingOutputCollector<>();
	
			// reset the generators
			input1 = new UniformIntPairGenerator(500, 40, false);
			generator2.reset();
			input2.reset();
	
			// compare with iterator values
			ReusingBuildFirstHashMatchIterator<IntPair, Tuple2<Integer, String>, Tuple2<Integer, String>> iterator =
					new ReusingBuildFirstHashMatchIterator<>(
						input1, input2, this.pairSerializer, this.pairComparator, 
						this.recordSerializer, this.record2Comparator, this.recordPairPairComparator,
						this.memoryManager, this.ioManager, this.parentTask, 1.0, true);
			
			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<Integer, Collection<TupleIntPairMatch>> entry : expectedMatchesMap.entrySet()) {
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

	
	
	static Map<Integer, Collection<TupleMatch>> matchSecondTupleFields(
			Map<Integer, Collection<String>> leftMap,
			Map<Integer, Collection<String>> rightMap)
	{
		Map<Integer, Collection<TupleMatch>> map = new HashMap<>();

		for (Integer key : leftMap.keySet()) {
			Collection<String> leftValues = leftMap.get(key);
			Collection<String> rightValues = rightMap.get(key);

			if (rightValues == null) {
				continue;
			}

			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<TupleMatch>());
			}

			Collection<TupleMatch> matchedValues = map.get(key);

			for (String leftValue : leftValues) {
				for (String rightValue : rightValues) {
					matchedValues.add(new TupleMatch(leftValue, rightValue));
				}
			}
		}

		return map;
	}
	
	static Map<Integer, Collection<TupleIntPairMatch>> matchTupleIntPairValues(
		Map<Integer, Collection<Integer>> leftMap,
		Map<Integer, Collection<String>> rightMap)
	{
		final Map<Integer, Collection<TupleIntPairMatch>> map = new HashMap<>();
	
		for (Integer i : leftMap.keySet()) {
			
			final Integer key = new Integer(i.intValue());
			
			final Collection<Integer> leftValues = leftMap.get(i);
			final Collection<String> rightValues = rightMap.get(key);
	
			if (rightValues == null) {
				continue;
			}
	
			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<TupleIntPairMatch>());
			}
	
			final Collection<TupleIntPairMatch> matchedValues = map.get(key);
	
			for (Integer v : leftValues) {
				for (String val : rightValues) {
					matchedValues.add(new TupleIntPairMatch(v, val));
				}
			}
		}
	
		return map;
	}

	
	static Map<Integer, Collection<String>> collectTupleData(MutableObjectIterator<Tuple2<Integer, String>> iter)
	throws Exception
	{
		Map<Integer, Collection<String>> map = new HashMap<>();
		Tuple2<Integer, String> pair = new Tuple2<>();
		
		while ((pair = iter.next(pair)) != null) {

			Integer key = pair.f0;
			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<String>());
			}

			Collection<String> values = map.get(key);
			values.add(pair.f1);
		}

		return map;
	}
	
	static Map<Integer, Collection<Integer>> collectIntPairData(MutableObjectIterator<IntPair> iter)
	throws Exception
	{
		Map<Integer, Collection<Integer>> map = new HashMap<>();
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
	static class TupleMatch {
		
		private final String left;
		private final String right;

		public TupleMatch(String left, String right) {
			this.left = left;
			this.right = right;
		}

		@Override
		public boolean equals(Object obj) {
			TupleMatch o = (TupleMatch) obj;
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
	static class TupleIntPairMatch
	{
		private final int left;
		private final String right;

		public TupleIntPairMatch(int left, String right) {
			this.left = left;
			this.right = right;
		}

		@Override
		public boolean equals(Object obj) {
			TupleIntPairMatch o = (TupleIntPairMatch) obj;
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
	
	static final class TupleMatchRemovingJoin implements FlatJoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>>
	{
		private final Map<Integer, Collection<TupleMatch>> toRemoveFrom;
		
		protected TupleMatchRemovingJoin(Map<Integer, Collection<TupleMatch>> map) {
			this.toRemoveFrom = map;
		}
		
		@Override
		public void join(Tuple2<Integer, String> rec1, Tuple2<Integer, String> rec2, Collector<Tuple2<Integer, String>> out) throws Exception
		{
			Integer key = rec1.f0;
			String value1 = rec1.f1;
			String value2 = rec2.f1;
			//System.err.println("rec1 key = "+key+"  rec2 key= "+rec2.getField(0, TestData.Key.class));
			Collection<TupleMatch> matches = this.toRemoveFrom.get(key);
			if (matches == null) {
				Assert.fail("Match " + key + " - " + value1 + ":" + value2 + " is unexpected.");
			}
			
			Assert.assertTrue("Produced match was not contained: " + key + " - " + value1 + ":" + value2,
				matches.remove(new TupleMatch(value1, value2)));
			
			if (matches.isEmpty()) {
				this.toRemoveFrom.remove(key);
			}
		}
	}
	
	static final class TupleIntPairMatchRemovingMatcher extends AbstractRichFunction implements FlatJoinFunction<IntPair, Tuple2<Integer, String>, Tuple2<Integer, String>>
	{
		private final Map<Integer, Collection<TupleIntPairMatch>> toRemoveFrom;
		
		protected TupleIntPairMatchRemovingMatcher(Map<Integer, Collection<TupleIntPairMatch>> map) {
			this.toRemoveFrom = map;
		}
		
		@Override
		public void join(IntPair rec1, Tuple2<Integer, String> rec2, Collector<Tuple2<Integer, String>> out) throws Exception
		{
			final int k = rec1.getKey();
			final int v = rec1.getValue(); 
			
			final Integer key = rec2.f0;
			final String value = rec2.f1;
			
			Assert.assertTrue("Key does not match for matching IntPair Tuple combination.", k == key); 
			
			Collection<TupleIntPairMatch> matches = this.toRemoveFrom.get(key);
			if (matches == null) {
				Assert.fail("Match " + key + " - " + v + ":" + value + " is unexpected.");
			}
			
			Assert.assertTrue("Produced match was not contained: " + key + " - " + v + ":" + value,
				matches.remove(new TupleIntPairMatch(v, value)));
			
			if (matches.isEmpty()) {
				this.toRemoveFrom.remove(key);
			}
		}
	}
	
	static final class IntPairTuplePairComparator extends TypePairComparator<IntPair, Tuple2<Integer, String>>
	{
		private int reference;
		
		@Override
		public void setReference(IntPair reference) {
			this.reference = reference.getKey();	
		}

		@Override
		public boolean equalToReference(Tuple2<Integer, String> candidate) {
			try {
				return candidate.f0 == this.reference;
			} catch (NullPointerException npex) {
				throw new NullKeyFieldException();
			}
		}

		@Override
		public int compareToReference(Tuple2<Integer, String> candidate) {
			try {
				return candidate.f0 - this.reference;
			} catch (NullPointerException npex) {
				throw new NullKeyFieldException();
			}
		}
	}
	
	static final class TupleIntPairPairComparator extends TypePairComparator<Tuple2<Integer, String>, IntPair>
	{
		private int reference;
		
		@Override
		public void setReference(Tuple2<Integer, String> reference) {
			this.reference = reference.f0;
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
