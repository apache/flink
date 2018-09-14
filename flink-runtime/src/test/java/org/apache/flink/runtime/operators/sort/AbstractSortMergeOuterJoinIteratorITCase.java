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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.operators.base.OuterJoinOperatorBase.OuterJoinType;
import org.apache.flink.api.common.typeutils.GenericPairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.CollectionIterator;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.Match;
import org.apache.flink.runtime.operators.testutils.MatchRemovingJoiner;
import org.apache.flink.runtime.operators.testutils.SimpleTupleJoinFunction;
import org.apache.flink.runtime.operators.testutils.TestData.TupleConstantValueIterator;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGeneratorIterator;
import org.apache.flink.runtime.util.ResettableMutableObjectIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public abstract class AbstractSortMergeOuterJoinIteratorITCase extends TestLogger {

	// total memory
	private static final int MEMORY_SIZE = 1024 * 1024 * 16;
	private static final int PAGES_FOR_BNLJN = 2;

	// random seeds for the left and right input data generators
	private static final long SEED1 = 561349061987311L;

	private static final long SEED2 = 231434613412342L;

	// dummy abstract task
	private final AbstractInvokable parentTask = new DummyInvokable();

	private IOManager ioManager;
	private MemoryManager memoryManager;
	
	private TupleSerializer<Tuple2<String, String>> serializer1;
	private TupleSerializer<Tuple2<String, Integer>> serializer2;
	private TypeComparator<Tuple2<String, String>> comparator1;
	private TypeComparator<Tuple2<String, Integer>> comparator2;
	private TypePairComparator<Tuple2<String, String>, Tuple2<String, Integer>> pairComp;


	@Before
	public void beforeTest() {
		ExecutionConfig config = new ExecutionConfig();
		config.disableObjectReuse();
		
		TupleTypeInfo<Tuple2<String, String>> typeInfo1 = TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class);
		TupleTypeInfo<Tuple2<String, Integer>> typeInfo2 = TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class);
		serializer1 = typeInfo1.createSerializer(config);
		serializer2 = typeInfo2.createSerializer(config);
		comparator1 = typeInfo1.createComparator(new int[]{0}, new boolean[]{true}, 0, config);
		comparator2 = typeInfo2.createComparator(new int[]{0}, new boolean[]{true}, 0, config);
		pairComp = new GenericPairComparator<>(comparator1, comparator2);

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

	@SuppressWarnings("unchecked")
	protected void testFullOuterWithSample() throws Exception {
		CollectionIterator<Tuple2<String, String>> input1 = CollectionIterator.of(
				new Tuple2<>("Jack", "Engineering"),
				new Tuple2<>("Tim", "Sales"),
				new Tuple2<>("Zed", "HR")
		);
		CollectionIterator<Tuple2<String, Integer>> input2 = CollectionIterator.of(
				new Tuple2<>("Allison", 100),
				new Tuple2<>("Jack", 200),
				new Tuple2<>("Zed", 150),
				new Tuple2<>("Zed", 250)
		);

		OuterJoinType outerJoinType = OuterJoinType.FULL;
		List<Tuple4<String, String, String, Object>> actual = computeOuterJoin(input1, input2, outerJoinType);

		List<Tuple4<String, String, String, Object>> expected = Arrays.asList(
				new Tuple4<String, String, String, Object>(null, null, "Allison", 100),
				new Tuple4<String, String, String, Object>("Jack", "Engineering", "Jack", 200),
				new Tuple4<String, String, String, Object>("Tim", "Sales", null, null),
				new Tuple4<String, String, String, Object>("Zed", "HR", "Zed", 150),
				new Tuple4<String, String, String, Object>("Zed", "HR", "Zed", 250)
		);

		Assert.assertEquals(expected, actual);
	}

	@SuppressWarnings("unchecked")
	protected void testLeftOuterWithSample() throws Exception {
		CollectionIterator<Tuple2<String, String>> input1 = CollectionIterator.of(
				new Tuple2<>("Jack", "Engineering"),
				new Tuple2<>("Tim", "Sales"),
				new Tuple2<>("Zed", "HR")
		);
		CollectionIterator<Tuple2<String, Integer>> input2 = CollectionIterator.of(
				new Tuple2<>("Allison", 100),
				new Tuple2<>("Jack", 200),
				new Tuple2<>("Zed", 150),
				new Tuple2<>("Zed", 250)
		);

		List<Tuple4<String, String, String, Object>> actual = computeOuterJoin(input1, input2, OuterJoinType.LEFT);

		List<Tuple4<String, String, String, Object>> expected = Arrays.asList(
				new Tuple4<String, String, String, Object>("Jack", "Engineering", "Jack", 200),
				new Tuple4<String, String, String, Object>("Tim", "Sales", null, null),
				new Tuple4<String, String, String, Object>("Zed", "HR", "Zed", 150),
				new Tuple4<String, String, String, Object>("Zed", "HR", "Zed", 250)
		);

		Assert.assertEquals(expected, actual);
	}

	@SuppressWarnings("unchecked")
	protected void testRightOuterWithSample() throws Exception {
		CollectionIterator<Tuple2<String, String>> input1 = CollectionIterator.of(
				new Tuple2<>("Jack", "Engineering"),
				new Tuple2<>("Tim", "Sales"),
				new Tuple2<>("Zed", "HR")
		);
		CollectionIterator<Tuple2<String, Integer>> input2 = CollectionIterator.of(
				new Tuple2<>("Allison", 100),
				new Tuple2<>("Jack", 200),
				new Tuple2<>("Zed", 150),
				new Tuple2<>("Zed", 250)
		);

		List<Tuple4<String, String, String, Object>> actual = computeOuterJoin(input1, input2, OuterJoinType.RIGHT);

		List<Tuple4<String, String, String, Object>> expected = Arrays.asList(
				new Tuple4<String, String, String, Object>(null, null, "Allison", 100),
				new Tuple4<String, String, String, Object>("Jack", "Engineering", "Jack", 200),
				new Tuple4<String, String, String, Object>("Zed", "HR", "Zed", 150),
				new Tuple4<String, String, String, Object>("Zed", "HR", "Zed", 250)
		);

		Assert.assertEquals(expected, actual);
	}

	@SuppressWarnings("unchecked")
	protected void testRightSideEmpty() throws Exception {
		CollectionIterator<Tuple2<String, String>> input1 = CollectionIterator.of(
				new Tuple2<>("Jack", "Engineering"),
				new Tuple2<>("Tim", "Sales"),
				new Tuple2<>("Zed", "HR")
		);
		CollectionIterator<Tuple2<String, Integer>> input2 = CollectionIterator.of();

		List<Tuple4<String, String, String, Object>> actualLeft = computeOuterJoin(input1, input2, OuterJoinType.LEFT);
		List<Tuple4<String, String, String, Object>> actualRight = computeOuterJoin(input1, input2, OuterJoinType.RIGHT);
		List<Tuple4<String, String, String, Object>> actualFull = computeOuterJoin(input1, input2, OuterJoinType.FULL);

		List<Tuple4<String, String, String, Object>> expected = Arrays.asList(
				new Tuple4<String, String, String, Object>("Jack", "Engineering", null, null),
				new Tuple4<String, String, String, Object>("Tim", "Sales", null, null),
				new Tuple4<String, String, String, Object>("Zed", "HR", null, null)
		);

		Assert.assertEquals(expected, actualLeft);
		Assert.assertEquals(expected, actualFull);
		Assert.assertEquals(Collections.<Tuple4<String,String,String,Object>>emptyList(), actualRight);
	}

	@SuppressWarnings("unchecked")
	protected void testLeftSideEmpty() throws Exception {
		CollectionIterator<Tuple2<String, String>> input1 = CollectionIterator.of();
		CollectionIterator<Tuple2<String, Integer>> input2 = CollectionIterator.of(
				new Tuple2<>("Allison", 100),
				new Tuple2<>("Jack", 200),
				new Tuple2<>("Zed", 150),
				new Tuple2<>("Zed", 250)
		);

		List<Tuple4<String, String, String, Object>> actualLeft = computeOuterJoin(input1, input2, OuterJoinType.LEFT);
		List<Tuple4<String, String, String, Object>> actualRight = computeOuterJoin(input1, input2, OuterJoinType.RIGHT);
		List<Tuple4<String, String, String, Object>> actualFull = computeOuterJoin(input1, input2, OuterJoinType.FULL);

		List<Tuple4<String, String, String, Object>> expected = Arrays.asList(
				new Tuple4<String, String, String, Object>(null, null, "Allison", 100),
				new Tuple4<String, String, String, Object>(null, null, "Jack", 200),
				new Tuple4<String, String, String, Object>(null, null, "Zed", 150),
				new Tuple4<String, String, String, Object>(null, null, "Zed", 250)
		);

		Assert.assertEquals(Collections.<Tuple4<String,String,String,Object>>emptyList(), actualLeft);
		Assert.assertEquals(expected, actualRight);
		Assert.assertEquals(expected, actualFull);
	}

	@SuppressWarnings("unchecked, rawtypes")
	private List<Tuple4<String, String, String, Object>> computeOuterJoin(ResettableMutableObjectIterator<Tuple2<String, String>> input1,
																		  ResettableMutableObjectIterator<Tuple2<String, Integer>> input2,
																		  OuterJoinType outerJoinType) throws Exception {
		input1.reset();
		input2.reset();
		AbstractMergeOuterJoinIterator iterator =
				createOuterJoinIterator(
						outerJoinType, input1, input2, serializer1, comparator1, serializer2, comparator2,
						pairComp, this.memoryManager, this.ioManager, PAGES_FOR_BNLJN, this.parentTask
				);

		List<Tuple4<String, String, String, Object>> actual = new ArrayList<>();
		ListCollector<Tuple4<String, String, String, Object>> collector = new ListCollector<>(actual);
		while (iterator.callWithNextKey(new SimpleTupleJoinFunction(), collector)) ;
		iterator.close();

		return actual;
	}

	@SuppressWarnings("unchecked, rawtypes")
	protected void testOuterJoinWithHighNumberOfCommonKeys(OuterJoinType outerJoinType, int input1Size, int input1Duplicates, int input1ValueLength,
														float input1KeyDensity, int input2Size, int input2Duplicates, int input2ValueLength, float input2KeyDensity) {
		TypeSerializer<Tuple2<Integer, String>> serializer1 = new TupleSerializer<>(
				(Class<Tuple2<Integer, String>>) (Class<?>) Tuple2.class,
				new TypeSerializer<?>[]{IntSerializer.INSTANCE, StringSerializer.INSTANCE}
		);
		TypeSerializer<Tuple2<Integer, String>> serializer2 = new TupleSerializer<>(
				(Class<Tuple2<Integer, String>>) (Class<?>) Tuple2.class,
				new TypeSerializer<?>[]{IntSerializer.INSTANCE, StringSerializer.INSTANCE}
		);
		TypeComparator<Tuple2<Integer, String>> comparator1 = new TupleComparator<>(
				new int[]{0},
				new TypeComparator<?>[]{new IntComparator(true)},
				new TypeSerializer<?>[]{IntSerializer.INSTANCE}
		);
		TypeComparator<Tuple2<Integer, String>> comparator2 = new TupleComparator<>(
				new int[]{0},
				new TypeComparator<?>[]{new IntComparator(true)},
				new TypeSerializer<?>[]{IntSerializer.INSTANCE}
		);

		TypePairComparator<Tuple2<Integer, String>, Tuple2<Integer, String>> pairComparator = new GenericPairComparator<>(comparator1, comparator2);

		final int DUPLICATE_KEY = 13;

		try {
			final TupleGenerator generator1 = new TupleGenerator(SEED1, 500, input1KeyDensity, input1ValueLength, KeyMode.SORTED_SPARSE, ValueMode.RANDOM_LENGTH, null);
			final TupleGenerator generator2 = new TupleGenerator(SEED2, 500, input2KeyDensity, input2ValueLength, KeyMode.SORTED_SPARSE, ValueMode.RANDOM_LENGTH, null);

			final TupleGeneratorIterator gen1Iter = new TupleGeneratorIterator(generator1, input1Size);
			final TupleGeneratorIterator gen2Iter = new TupleGeneratorIterator(generator2, input2Size);

			final TupleConstantValueIterator const1Iter = new TupleConstantValueIterator(DUPLICATE_KEY, "LEFT String for Duplicate Keys", input1Duplicates);
			final TupleConstantValueIterator const2Iter = new TupleConstantValueIterator(DUPLICATE_KEY, "RIGHT String for Duplicate Keys", input2Duplicates);

			final List<MutableObjectIterator<Tuple2<Integer, String>>> inList1 = new ArrayList<>();
			inList1.add(gen1Iter);
			inList1.add(const1Iter);

			final List<MutableObjectIterator<Tuple2<Integer, String>>> inList2 = new ArrayList<>();
			inList2.add(gen2Iter);
			inList2.add(const2Iter);

			MutableObjectIterator<Tuple2<Integer, String>> input1 = new MergeIterator<>(inList1, comparator1.duplicate());
			MutableObjectIterator<Tuple2<Integer, String>> input2 = new MergeIterator<>(inList2, comparator2.duplicate());

			// collect expected data
			final Map<Integer, Collection<Match>> expectedMatchesMap = joinValues(
					collectData(input1),
					collectData(input2),
					outerJoinType);

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

			input1 = new MergeIterator<>(inList1, comparator1.duplicate());
			input2 = new MergeIterator<>(inList2, comparator2.duplicate());

			final FlatJoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> joinFunction =
					new MatchRemovingJoiner(expectedMatchesMap);

			final Collector<Tuple2<Integer, String>> collector = new DiscardingOutputCollector<>();


			// we create this sort-merge iterator with little memory for the block-nested-loops fall-back to make sure it
			// needs to spill for the duplicate keys
			AbstractMergeOuterJoinIterator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> iterator =
					createOuterJoinIterator(
							outerJoinType, input1, input2, serializer1, comparator1, serializer2, comparator2,
							pairComparator, this.memoryManager, this.ioManager, PAGES_FOR_BNLJN, this.parentTask);

			iterator.open();

			while (iterator.callWithNextKey(joinFunction, collector)) ;

			iterator.close();

			// assert that each expected match was seen
			for (Entry<Integer, Collection<Match>> entry : expectedMatchesMap.entrySet()) {
				if (!entry.getValue().isEmpty()) {
					Assert.fail("Collection for key " + entry.getKey() + " is not empty");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}

	protected abstract <T1, T2, T3> AbstractMergeOuterJoinIterator<T1, T2, T3> createOuterJoinIterator(OuterJoinType outerJoinType,
																			  MutableObjectIterator<T1> input1,
																			  MutableObjectIterator<T2> input2,
																			  TypeSerializer<T1> serializer1, TypeComparator<T1> comparator1,
																			  TypeSerializer<T2> serializer2, TypeComparator<T2> comparator2,
																			  TypePairComparator<T1, T2> pairComparator,
																			  MemoryManager memoryManager,
																			  IOManager ioManager,
																			  int numMemoryPages,
																			  AbstractInvokable parentTask) throws Exception;

	// --------------------------------------------------------------------------------------------
	//                                    Utilities
	// --------------------------------------------------------------------------------------------


	private Map<Integer, Collection<Match>> joinValues(
			Map<Integer, Collection<String>> leftMap,
			Map<Integer, Collection<String>> rightMap,
			OuterJoinType outerJoinType) {
		Map<Integer, Collection<Match>> map = new HashMap<>();

		for (Integer key : leftMap.keySet()) {
			Collection<String> leftValues = leftMap.get(key);
			Collection<String> rightValues = rightMap.get(key);

			if (outerJoinType == OuterJoinType.RIGHT && rightValues == null) {
				continue;
			}

			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<Match>());
			}

			Collection<Match> joinedValues = map.get(key);

			for (String leftValue : leftValues) {
				if (rightValues != null) {
					for (String rightValue : rightValues) {
						joinedValues.add(new Match(leftValue, rightValue));
					}
				} else {
					joinedValues.add(new Match(leftValue, null));
				}
			}
		}

		if (outerJoinType == OuterJoinType.RIGHT || outerJoinType == OuterJoinType.FULL) {
			for (Integer key : rightMap.keySet()) {
				Collection<String> leftValues = leftMap.get(key);
				Collection<String> rightValues = rightMap.get(key);

				if (leftValues != null) {
					continue;
				}

				if (!map.containsKey(key)) {
					map.put(key, new ArrayList<Match>());
				}

				Collection<Match> joinedValues = map.get(key);

				for (String rightValue : rightValues) {
					joinedValues.add(new Match(null, rightValue));
				}
			}
		}

		return map;
	}


	private Map<Integer, Collection<String>> collectData(MutableObjectIterator<Tuple2<Integer, String>> iter)
			throws Exception {
		final Map<Integer, Collection<String>> map = new HashMap<>();
		Tuple2<Integer, String> pair = new Tuple2<>();

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
