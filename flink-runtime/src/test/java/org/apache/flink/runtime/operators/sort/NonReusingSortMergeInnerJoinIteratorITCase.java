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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.GenericPairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.Match;
import org.apache.flink.runtime.operators.testutils.MatchRemovingJoiner;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("deprecation")
public class NonReusingSortMergeInnerJoinIteratorITCase extends TestLogger {
	
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
	private final AbstractInvokable parentTask = new DummyInvokable();

	private IOManager ioManager;
	private MemoryManager memoryManager;

	private TypeSerializer<Tuple2<Integer, String>> serializer1;
	private TypeSerializer<Tuple2<Integer, String>> serializer2;
	private TypeComparator<Tuple2<Integer, String>> comparator1;
	private TypeComparator<Tuple2<Integer, String>> comparator2;
	private TypePairComparator<Tuple2<Integer, String>, Tuple2<Integer, String>> pairComparator;
	

	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest() {
		serializer1 = new TupleSerializer<Tuple2<Integer, String>>(
				(Class<Tuple2<Integer, String>>) (Class<?>) Tuple2.class,
				new TypeSerializer<?>[] { IntSerializer.INSTANCE, StringSerializer.INSTANCE });
		serializer2 = new TupleSerializer<Tuple2<Integer, String>>(
				(Class<Tuple2<Integer, String>>) (Class<?>) Tuple2.class,
				new TypeSerializer<?>[] { IntSerializer.INSTANCE, StringSerializer.INSTANCE });
		comparator1 =  new TupleComparator<Tuple2<Integer, String>>(
				new int[]{0},
				new TypeComparator<?>[] { new IntComparator(true) },
				new TypeSerializer<?>[] { IntSerializer.INSTANCE });
		comparator2 =  new TupleComparator<Tuple2<Integer, String>>(
				new int[]{0},
				new TypeComparator<?>[] { new IntComparator(true) },
				new TypeSerializer<?>[] { IntSerializer.INSTANCE });
		pairComparator = new GenericPairComparator<Tuple2<Integer, String>, Tuple2<Integer, String>>(comparator1, comparator2);
		
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
	public void testMerge() {
		try {

			final TupleGenerator generator1 = new TupleGenerator(SEED1, 500, 4096, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);
			final TupleGenerator generator2 = new TupleGenerator(SEED2, 500, 2048, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);

			final TestData.TupleGeneratorIterator input1 = new TestData.TupleGeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.TupleGeneratorIterator input2 = new TestData.TupleGeneratorIterator(generator2, INPUT_2_SIZE);

			// collect expected data
			final Map<Integer, Collection<Match>> expectedMatchesMap = matchValues(
					collectData(input1),
					collectData(input2));

			final FlatJoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> joinFunction =
					new MatchRemovingJoiner(expectedMatchesMap);

			final Collector<Tuple2<Integer, String>> collector = new DiscardingOutputCollector<Tuple2<Integer, String>>();
	
			// reset the generators
			generator1.reset();
			generator2.reset();
			input1.reset();
			input2.reset();
	
			// compare with iterator values
			NonReusingMergeInnerJoinIterator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> iterator =
				new NonReusingMergeInnerJoinIterator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>>(
					input1, input2, this.serializer1, this.comparator1, this.serializer2, this.comparator2,
					this.pairComparator, this.memoryManager, this.ioManager, PAGES_FOR_BNLJN, this.parentTask);
	
			iterator.open();
			
			while (iterator.callWithNextKey(joinFunction, collector));
			
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<Integer, Collection<Match>> entry : expectedMatchesMap.entrySet()) {
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
			final TupleGenerator generator1 = new TupleGenerator(SEED1, 500, 4096, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);
			final TupleGenerator generator2 = new TupleGenerator(SEED2, 500, 2048, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);

			final TestData.TupleGeneratorIterator gen1Iter = new TestData.TupleGeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.TupleGeneratorIterator gen2Iter = new TestData.TupleGeneratorIterator(generator2, INPUT_2_SIZE);

			final TestData.TupleConstantValueIterator const1Iter = new TestData.TupleConstantValueIterator(DUPLICATE_KEY, "LEFT String for Duplicate Keys", INPUT_1_DUPLICATES);
			final TestData.TupleConstantValueIterator const2Iter = new TestData.TupleConstantValueIterator(DUPLICATE_KEY, "RIGHT String for Duplicate Keys", INPUT_2_DUPLICATES);

			final List<MutableObjectIterator<Tuple2<Integer, String>>> inList1 = new ArrayList<MutableObjectIterator<Tuple2<Integer, String>>>();
			inList1.add(gen1Iter);
			inList1.add(const1Iter);

			final List<MutableObjectIterator<Tuple2<Integer, String>>> inList2 = new ArrayList<MutableObjectIterator<Tuple2<Integer, String>>>();
			inList2.add(gen2Iter);
			inList2.add(const2Iter);

			MutableObjectIterator<Tuple2<Integer, String>> input1 = new MergeIterator<Tuple2<Integer, String>>(inList1, comparator1.duplicate());
			MutableObjectIterator<Tuple2<Integer, String>> input2 = new MergeIterator<Tuple2<Integer, String>>(inList2, comparator2.duplicate());
			
			// collect expected data
			final Map<Integer, Collection<Match>> expectedMatchesMap = matchValues(
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
	
			input1 = new MergeIterator<Tuple2<Integer, String>>(inList1, comparator1.duplicate());
			input2 = new MergeIterator<Tuple2<Integer, String>>(inList2, comparator2.duplicate());
			
			final FlatJoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> joinFunction = new MatchRemovingJoiner(expectedMatchesMap);
			
			final Collector<Tuple2<Integer, String>> collector = new DiscardingOutputCollector<Tuple2<Integer, String>>();
	
			
			// we create this sort-merge iterator with little memory for the block-nested-loops fall-back to make sure it
			// needs to spill for the duplicate keys
			NonReusingMergeInnerJoinIterator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> iterator =
				new NonReusingMergeInnerJoinIterator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>>(
					input1, input2, this.serializer1, this.comparator1, this.serializer2, this.comparator2,
					this.pairComparator, this.memoryManager, this.ioManager, PAGES_FOR_BNLJN, this.parentTask);
	
			iterator.open();
			
			while (iterator.callWithNextKey(joinFunction, collector));
			
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<Integer, Collection<Match>> entry : expectedMatchesMap.entrySet()) {
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

	private Map<Integer, Collection<Match>> matchValues(
			Map<Integer, Collection<String>> leftMap,
			Map<Integer, Collection<String>> rightMap)
	{
		Map<Integer, Collection<Match>> map = new HashMap<Integer, Collection<Match>>();

		for (Integer key : leftMap.keySet()) {
			Collection<String> leftValues = leftMap.get(key);
			Collection<String> rightValues = rightMap.get(key);

			if (rightValues == null) {
				continue;
			}

			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<Match>());
			}

			Collection<Match> matchedValues = map.get(key);

			for (String leftValue : leftValues) {
				for (String rightValue : rightValues) {
					matchedValues.add(new Match(leftValue, rightValue));
				}
			}
		}

		return map;
	}


	private Map<Integer, Collection<String>> collectData(MutableObjectIterator<Tuple2<Integer, String>> iter)
			throws Exception
	{
		Map<Integer, Collection<String>> map = new HashMap<Integer, Collection<String>>();
		Tuple2<Integer, String> pair = new Tuple2<Integer, String>();

		while ((pair = iter.next(pair)) != null) {
			final Integer key = pair.getField(0);

			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<String>());
			}

			Collection<String> values = map.get(key);
			final String value = pair.getField(1);
			values.add(value);
		}

		return map;
	}

}
