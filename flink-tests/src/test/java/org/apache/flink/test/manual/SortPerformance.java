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

package org.apache.flink.test.manual;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.SplittableIterator;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

/**
 * This is for testing the performance of sorting, with and without code generation
 */
public class SortPerformance {
	
	public static void main(String[] args) throws Exception {

		final int numElements  = 10_000_000;
		final int keyRange     =  8_000_000;
		final int rounds       = 20;
		final int warmupRounds = 5;


		HashMap<String,SummaryStatistics> statCollection = new HashMap<>();

		for(boolean codeGeneration : new boolean[]{ false, true }) {

			SummaryStatistics stat = new SummaryStatistics();
			for( int i = 0; i < rounds + warmupRounds; i++ ){
				int keys = keyRange;
				int elements = numElements;

				if( i < warmupRounds ) {
					keys = 1000;
					elements = 10000;
				}
				long executionTime = testSortPerformance(new TupleIntIntIterator(keys),
					TupleTypeInfo.<Tuple2<Integer, Integer>>getBasicTupleTypeInfo(Integer.class, Integer.class), elements, codeGeneration);

				if( i >= warmupRounds ) {
					stat.addValue(executionTime);
				}
			}
			statCollection.put(buildTestCaseKey("TupleIntIntIterator", codeGeneration), stat);

			stat = new SummaryStatistics();
			for( int i = 0; i < rounds + warmupRounds; i++ ){
				int keys = keyRange;
				int elements = numElements;

				if( i < warmupRounds ) {
					keys = 1000;
					elements = 10000;
				}

				long executionTime = testSortPerformance(new TupleStringIntIterator(keys),
					TupleTypeInfo.<Tuple2<String, Integer>>getBasicTupleTypeInfo(String.class, Integer.class), elements, codeGeneration);

				if( i >= warmupRounds ) {
					stat.addValue(executionTime);
				}
			}
			statCollection.put(buildTestCaseKey("TupleStringIntIterator", codeGeneration), stat);
		}


		String[] keys = new String[statCollection.keySet().size()];

		statCollection.keySet().toArray(keys);

		Arrays.sort(keys);

		System.out.println("------------------------------------------------------------------");
		System.out.println(String.format("Collected from %d runs with %d items, key range %d", rounds, numElements, keyRange));
		System.out.println("------------------------------------------------------------------");
		for( String k : keys ){
			SummaryStatistics stat = statCollection.get(k);
			String line = String.format("%s : avg execution time : %10.4fms std. %.4f", k, stat.getMean(), stat.getStandardDeviation());
			System.out.println(line);
		}

	}

	private static <T, B extends CopyableIterator<T>> long testSortPerformance
		(B iterator, TypeInformation<T> typeInfo, int numRecords, boolean codeGeneration) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();
		env.getConfig().setCodeGenerationForSorterEnabled(codeGeneration);

		@SuppressWarnings("unchecked")
		DataSet<T> output =
			env.fromParallelCollection(new SplittableRandomIterator<T, B>(numRecords, iterator), typeInfo)
				.sortPartition(0, Order.ASCENDING);

		long start = System.currentTimeMillis();

		System.out.println(output.count());

		long end = System.currentTimeMillis();

		return end - start;
	}

	private static final class SplittableRandomIterator<T, B extends CopyableIterator<T>> extends SplittableIterator<T> implements Serializable {

		private int numElements;
		private final B baseIterator;

		public SplittableRandomIterator(int numElements, B baseIterator) {
			this.numElements = numElements;
			this.baseIterator = baseIterator;
		}

		@Override
		public boolean hasNext() {
			return numElements > 0;
		}

		@Override
		public T next() {
			numElements--;
			return baseIterator.next();
		}

		@SuppressWarnings("unchecked")
		@Override
		public SplittableRandomIterator<T, B>[] split(int numPartitions) {
			int splitSize = numElements / numPartitions;
			int rem = numElements % numPartitions;
			SplittableRandomIterator<T, B>[] res = new SplittableRandomIterator[numPartitions];
			for (int i = 0; i < numPartitions; i++) {
				res[i] = new SplittableRandomIterator<T, B>(i < rem ? splitSize : splitSize + 1, (B)baseIterator.copy());
			}
			return res;
		}

		@Override
		public int getMaximumNumberOfSplits() {
			return numElements;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	private interface CopyableIterator<T> extends Iterator<T> {
		CopyableIterator<T> copy();
	}


	private static final class TupleIntIntIterator implements CopyableIterator<Tuple2<Integer, Integer>>, Serializable {

		private final int keyRange;
		private Tuple2<Integer, Integer> reuse = new Tuple2<Integer, Integer>();

		private int rndSeed = 11;
		private Random rnd;

		public TupleIntIntIterator(int keyRange) {
			this.keyRange = keyRange;
			this.rnd = new Random(this.rndSeed);
		}

		public TupleIntIntIterator(int keyRange, int rndSeed) {
			this.keyRange = keyRange;
			this.rndSeed = rndSeed;
			this.rnd = new Random(rndSeed);
		}

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public Tuple2<Integer, Integer> next() {
			reuse.f0 = rnd.nextInt(keyRange);
			reuse.f1 = 1;
			return reuse;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public CopyableIterator<Tuple2<Integer, Integer>> copy() {
			return new TupleIntIntIterator(keyRange, rndSeed + rnd.nextInt(10000));
		}
	}


	private static final class TupleStringIntIterator implements CopyableIterator<Tuple2<String, Integer>>, Serializable {

		private final int keyRange;
		private Tuple2<String, Integer> reuse = new Tuple2<>();

		private int rndSeed = 11;
		private Random rnd;

		public TupleStringIntIterator(int keyRange) {
			this.keyRange = keyRange;
			this.rnd = new Random(this.rndSeed);
		}

		public TupleStringIntIterator(int keyRange, int rndSeed) {
			this.keyRange = keyRange;
			this.rndSeed = rndSeed;
			this.rnd = new Random(rndSeed);
		}

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public Tuple2<String, Integer> next() {
			reuse.f0 = String.valueOf(rnd.nextInt(keyRange));
			reuse.f1 = 1;
			return reuse;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public CopyableIterator<Tuple2<String, Integer>> copy() {
			return new TupleStringIntIterator(keyRange, rndSeed + rnd.nextInt(10000));
		}
	}


	private static final class SumReducer<K> implements ReduceFunction<Tuple2<K, Integer>> {
		@Override
		public Tuple2<K, Integer> reduce(Tuple2<K, Integer> a, Tuple2<K, Integer> b) throws Exception {
			if (!a.f0.equals(b.f0)) {
				throw new RuntimeException("SumReducer was called with two record that have differing keys.");
			}
			a.f1 = a.f1 + b.f1;
			return a;
		}
	}

	private static final String buildTestCaseKey(String name, boolean codeGeneration){
		return String.format("%25s(codegeneration=%5s)", name, codeGeneration);
	}
}
