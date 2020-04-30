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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.runtime.operators.sort.MergeIterator;
import org.apache.flink.runtime.operators.testutils.Match;
import org.apache.flink.runtime.operators.testutils.TestData.TupleConstantValueIterator;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGeneratorIterator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Random test for sort merge outer join.
 */
public class RandomSortMergeOuterJoinTest {

	// random seeds for the left and right input data generators
	private static final long SEED1 = 561349061987311L;

	private static final long SEED2 = 231434613412342L;

	@Test
	public void testFullOuterJoinWithHighNumberOfCommonKeys() {
		testOuterJoinWithHighNumberOfCommonKeys(FlinkJoinType.FULL, 200, 500, 2048, 0.02f, 200, 500, 2048, 0.02f);
	}

	@Test
	public void testLeftOuterJoinWithHighNumberOfCommonKeys() {
		testOuterJoinWithHighNumberOfCommonKeys(FlinkJoinType.LEFT, 200, 10, 4096, 0.02f, 100, 4000, 2048, 0.02f);
	}

	@Test
	public void testRightOuterJoinWithHighNumberOfCommonKeys() {
		testOuterJoinWithHighNumberOfCommonKeys(FlinkJoinType.RIGHT, 100, 10, 2048, 0.02f, 200, 4000, 4096, 0.02f);
	}

	@SuppressWarnings("unchecked, rawtypes")
	protected void testOuterJoinWithHighNumberOfCommonKeys(
			FlinkJoinType outerJoinType, int input1Size, int input1Duplicates, int input1ValueLength,
			float input1KeyDensity, int input2Size, int input2Duplicates, int input2ValueLength,
			float input2KeyDensity) {
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

		final int duplicateKey = 13;

		try {
			final TupleGenerator generator1 = new TupleGenerator(SEED1, 500, input1KeyDensity, input1ValueLength, KeyMode.SORTED_SPARSE, ValueMode.RANDOM_LENGTH, null);
			final TupleGenerator generator2 = new TupleGenerator(SEED2, 500, input2KeyDensity, input2ValueLength, KeyMode.SORTED_SPARSE, ValueMode.RANDOM_LENGTH, null);

			final TupleGeneratorIterator gen1Iter = new TupleGeneratorIterator(generator1, input1Size);
			final TupleGeneratorIterator gen2Iter = new TupleGeneratorIterator(generator2, input2Size);

			final TupleConstantValueIterator const1Iter = new TupleConstantValueIterator(duplicateKey, "LEFT String for Duplicate Keys", input1Duplicates);
			final TupleConstantValueIterator const2Iter = new TupleConstantValueIterator(duplicateKey, "RIGHT String for Duplicate Keys", input2Duplicates);

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
					RandomSortMergeInnerJoinTest.collectData(input1),
					RandomSortMergeInnerJoinTest.collectData(input2),
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

			StreamOperator operator = getOperator(outerJoinType);
			RandomSortMergeInnerJoinTest.match(expectedMatchesMap,
					RandomSortMergeInnerJoinTest.transformToBinary(myJoin(operator, input1, input2)));

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

	public LinkedBlockingQueue<Object> myJoin(
			StreamOperator operator,
			MutableObjectIterator<Tuple2<Integer, String>> input1,
			MutableObjectIterator<Tuple2<Integer, String>> input2) throws Exception {
		return RandomSortMergeInnerJoinTest.join(operator, input1, input2);
	}

	// --------------------------------------------------------------------------------------------
	//                                    Utilities
	// --------------------------------------------------------------------------------------------

	private Map<Integer, Collection<Match>> joinValues(
			Map<Integer, Collection<String>> leftMap,
			Map<Integer, Collection<String>> rightMap,
			FlinkJoinType outerJoinType) {
		Map<Integer, Collection<Match>> map = new HashMap<>();

		for (Integer key : leftMap.keySet()) {
			Collection<String> leftValues = leftMap.get(key);
			Collection<String> rightValues = rightMap.get(key);

			if (outerJoinType == FlinkJoinType.RIGHT && rightValues == null) {
				continue;
			}

			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<>());
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

		if (outerJoinType == FlinkJoinType.RIGHT || outerJoinType == FlinkJoinType.FULL) {
			for (Integer key : rightMap.keySet()) {
				Collection<String> leftValues = leftMap.get(key);
				Collection<String> rightValues = rightMap.get(key);

				if (leftValues != null) {
					continue;
				}

				if (!map.containsKey(key)) {
					map.put(key, new ArrayList<>());
				}

				Collection<Match> joinedValues = map.get(key);

				for (String rightValue : rightValues) {
					joinedValues.add(new Match(null, rightValue));
				}
			}
		}

		return map;
	}

	protected StreamOperator getOperator(FlinkJoinType outerJoinType) {
		return Int2SortMergeJoinOperatorTest.newOperator(outerJoinType, false);
	}
}
