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


package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.RuntimePairComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.operators.testutils.BinaryOperatorTestBase;
import org.apache.flink.runtime.operators.testutils.UniformIntTupleGenerator;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

public abstract class AbstractOuterJoinTaskExternalITCase extends BinaryOperatorTestBase<FlatJoinFunction<Tuple2<Integer, Integer>,
		Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

	protected static final long HASH_MEM = 4 * 1024 * 1024;

	private static final long SORT_MEM = 3 * 1024 * 1024;
	
	private static final long BNLJN_MEM = 10 * PAGE_SIZE;
	
	private final double bnljn_frac;
	
	@SuppressWarnings("unchecked")
	protected final TypeComparator<Tuple2<Integer, Integer>> comparator1 = new TupleComparator<>(
			new int[]{0},
			new TypeComparator<?>[]{new IntComparator(true)},
			new TypeSerializer<?>[]{IntSerializer.INSTANCE}
	);
	
	@SuppressWarnings("unchecked")
	protected final TypeComparator<Tuple2<Integer, Integer>> comparator2 = new TupleComparator<>(
			new int[]{0},
			new TypeComparator<?>[]{new IntComparator(true)},
			new TypeSerializer<?>[]{IntSerializer.INSTANCE}
	);
	
	@SuppressWarnings("unchecked")
	protected final TypeSerializer<Tuple2<Integer, Integer>> serializer = new TupleSerializer<>(
			(Class<Tuple2<Integer, Integer>>) (Class<?>) Tuple2.class,
			new TypeSerializer<?>[]{IntSerializer.INSTANCE, IntSerializer.INSTANCE}
	);
	
	protected final CountingOutputCollector<Tuple2<Integer, Integer>> output = new CountingOutputCollector<>();
	
	public AbstractOuterJoinTaskExternalITCase(ExecutionConfig config) {
		super(config, HASH_MEM, 2, SORT_MEM);
		bnljn_frac = (double) BNLJN_MEM / this.getMemoryManager().getMemorySize();
	}
	
	@Test
	public void testExternalSortOuterJoinTask() throws Exception {
		final int keyCnt1 = 16384 * 4;
		final int valCnt1 = 2;
		
		final int keyCnt2 = 8192;
		final int valCnt2 = 4 * 2;
		
		final int expCnt = calculateExpectedCount(keyCnt1, valCnt1, keyCnt2, valCnt2);
		
		setOutput(this.output);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(this.getSortStrategy());
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);
		
		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask = getOuterJoinDriver();
		
		addInputSorted(new UniformIntTupleGenerator(keyCnt1, valCnt1, false), serializer, this.comparator1.duplicate());
		addInputSorted(new UniformIntTupleGenerator(keyCnt2, valCnt2, false), serializer, this.comparator2.duplicate());
		testDriver(testTask, MockJoinStub.class);
		
		Assert.assertEquals("Wrong result set size.", expCnt, this.output.getNumberOfRecords());
	}
	
	protected abstract int calculateExpectedCount(int keyCnt1, int valCnt1, int keyCnt2, int valCnt2);
	
	protected abstract AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> getOuterJoinDriver();

	protected abstract DriverStrategy getSortStrategy();
	
	// =================================================================================================

	@SuppressWarnings("serial")
	public static final class MockJoinStub implements FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
		
		@Override
		public void join(Tuple2<Integer, Integer> first, Tuple2<Integer, Integer> second, Collector<Tuple2<Integer, Integer>> out) throws Exception {
			out.collect(first != null ? first : second);
		}
	}
}
