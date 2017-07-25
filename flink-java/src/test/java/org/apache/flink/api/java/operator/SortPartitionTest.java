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

package org.apache.flink.api.java.operator;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link DataSet#sortPartition(int, Order)}.
 */
public class SortPartitionTest {

	// TUPLE DATA
	private final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData =
			new ArrayList<Tuple5<Integer, Long, String, Long, Integer>>();

	private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo = new
			TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>>(
					BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.LONG_TYPE_INFO,
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.LONG_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO
			);

	private final TupleTypeInfo<Tuple4<Integer, Long, CustomType, Long[]>> tupleWithCustomInfo = new
			TupleTypeInfo<Tuple4<Integer, Long, CustomType, Long[]>>(
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO,
				TypeExtractor.createTypeInfo(CustomType.class),
				BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO
			);

	// LONG DATA
	private final List<Long> emptyLongData = new ArrayList<Long>();

	private final List<CustomType> customTypeData = new ArrayList<CustomType>();

	private final List<Tuple4<Integer, Long, CustomType, Long[]>> tupleWithCustomData =
			new ArrayList<Tuple4<Integer, Long, CustomType, Long[]>>();

	@Test
	public void testSortPartitionPositionKeys1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			tupleDs.sortPartition(0, Order.ASCENDING);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	public void testSortPartitionPositionKeys2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			tupleDs
					.sortPartition(0, Order.ASCENDING)
					.sortPartition(3, Order.DESCENDING);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test(expected = InvalidProgramException.class)
	public void testSortPartitionWithPositionKeys3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs = env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// must not work
		tupleDs.sortPartition(2, Order.ASCENDING);
	}

	@Test(expected = InvalidProgramException.class)
	public void testSortPartitionWithPositionKeys4() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs = env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// must not work
		tupleDs.sortPartition(3, Order.ASCENDING);
	}

	@Test
	public void testSortPartitionExpressionKeys1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			tupleDs.sortPartition("f1", Order.ASCENDING);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	public void testSortPartitionExpressionKeys2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs = env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// should work
		try {
			tupleDs
					.sortPartition("f0", Order.ASCENDING)
					.sortPartition("f2.nested.myInt", Order.DESCENDING);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test(expected = InvalidProgramException.class)
	public void testSortPartitionWithExpressionKeys3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs = env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// must not work
		tupleDs.sortPartition("f2.nested", Order.ASCENDING);
	}

	@Test(expected = InvalidProgramException.class)
	public void testSortPartitionWithExpressionKeys4() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs = env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// must not work
		tupleDs.sortPartition("f3", Order.ASCENDING);
	}

	@Test
	public void testSortPartitionWithKeySelector1() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs = env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// should work
		try {
			tupleDs.sortPartition(new KeySelector<Tuple4<Integer, Long, CustomType, Long[]>, Integer>() {
				@Override
				public Integer getKey(Tuple4<Integer, Long, CustomType, Long[]> value) throws Exception {
					return value.f0;
				}
			}, Order.ASCENDING);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test(expected = InvalidProgramException.class)
	public void testSortPartitionWithKeySelector2() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs = env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// must not work
		tupleDs.sortPartition(new KeySelector<Tuple4<Integer, Long, CustomType, Long[]>, Long[]>() {
			@Override
			public Long[] getKey(Tuple4<Integer, Long, CustomType, Long[]> value) throws Exception {
				return value.f3;
			}
		}, Order.ASCENDING);
	}

	@Test(expected = InvalidProgramException.class)
	public void testSortPartitionWithKeySelector3() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs = env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// must not work
		tupleDs
			.sortPartition("f1", Order.ASCENDING)
			.sortPartition(new KeySelector<Tuple4<Integer, Long, CustomType, Long[]>, CustomType>() {
				@Override
				public CustomType getKey(Tuple4<Integer, Long, CustomType, Long[]> value) throws Exception {
					return value.f2;
				}
			}, Order.ASCENDING);
	}

	@Test
	public void testSortPartitionWithKeySelector4() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs = env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// should work
		try {
			tupleDs.sortPartition(new KeySelector<Tuple4<Integer, Long, CustomType, Long[]>, Tuple2<Integer, Long>>() {
				@Override
				public Tuple2<Integer, Long> getKey(Tuple4<Integer, Long, CustomType, Long[]> value) throws Exception {
					return new Tuple2<>(value.f0, value.f1);
				}
			}, Order.ASCENDING);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test(expected = InvalidProgramException.class)
	public void testSortPartitionWithKeySelector5() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs = env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// must not work
		tupleDs
			.sortPartition(new KeySelector<Tuple4<Integer, Long, CustomType, Long[]>, CustomType>() {
				@Override
				public CustomType getKey(Tuple4<Integer, Long, CustomType, Long[]> value) throws Exception {
					return value.f2;
				}
			}, Order.ASCENDING)
			.sortPartition("f1", Order.ASCENDING);
	}

	/**
	 * Custom data type, for testing purposes.
	 */
	public static class CustomType implements Serializable {

		/**
		 * Custom nested data type, for testing purposes.
		 */
		public static class Nest {
			public int myInt;
		}

		private static final long serialVersionUID = 1L;

		public int myInt;
		public long myLong;
		public String myString;
		public Nest nested;

		public CustomType() {}

		public CustomType(int i, long l, String s) {
			myInt = i;
			myLong = l;
			myString = s;
		}

		@Override
		public String toString() {
			return myInt + "," + myLong + "," + myString;
		}
	}

}
