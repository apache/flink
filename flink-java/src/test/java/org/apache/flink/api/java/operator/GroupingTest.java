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
 * Tests for {@link DataSet#groupBy(int...)}.
 */
public class GroupingTest {

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

	private final List<Tuple2<byte[], byte[]>> byteArrayData = new ArrayList<Tuple2<byte[], byte[]>>();

	@Test
	public void testGroupByKeyFields1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			tupleDs.groupBy(0);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test(expected = InvalidProgramException.class)
	public void testGroupByKeyFields2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Long> longDs = env.fromCollection(emptyLongData, BasicTypeInfo.LONG_TYPE_INFO);
		// should not work: groups on basic type
		longDs.groupBy(0);
	}

	@Test(expected = InvalidProgramException.class)
	public void testGroupByKeyFields3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		this.customTypeData.add(new CustomType());

		DataSet<CustomType> customDs = env.fromCollection(customTypeData);
		// should not work: groups on custom type
		customDs.groupBy(0);

	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testGroupByKeyFields4() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, key out of tuple bounds
		tupleDs.groupBy(5);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testGroupByKeyFields5() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, negative field position
		tupleDs.groupBy(-1);
	}

	@Test
	public void testGroupByKeyFieldsOnPrimitiveArray() {
		this.byteArrayData.add(new Tuple2(new byte[]{0}, new byte[]{1}));

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple2<byte[], byte[]>> tupleDs = env.fromCollection(byteArrayData);
		tupleDs.groupBy(0);
	}

	@Test
	public void testGroupByKeyExpressions1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		this.customTypeData.add(new CustomType());

		DataSet<CustomType> ds = env.fromCollection(customTypeData);

		// should work
		try {
			ds.groupBy("myInt");
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test(expected = InvalidProgramException.class)
	public void testGroupByKeyExpressions2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Long> longDs = env.fromCollection(emptyLongData, BasicTypeInfo.LONG_TYPE_INFO);
		// should not work: groups on basic type
		longDs.groupBy("myInt");
	}

	@Test(expected = InvalidProgramException.class)
	public void testGroupByKeyExpressions3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		this.customTypeData.add(new CustomType());

		DataSet<CustomType> customDs = env.fromCollection(customTypeData);
		// should not work: tuple selector on custom type
		customDs.groupBy(0);

	}

	@Test(expected = IllegalArgumentException.class)
	public void testGroupByKeyExpressions4() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds = env.fromCollection(customTypeData);

		// should not work, key out of tuple bounds
		ds.groupBy("myNonExistent");
	}

	@Test
	public void testGroupByKeyExpressions1Nested() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		this.customTypeData.add(new CustomType());

		DataSet<CustomType> ds = env.fromCollection(customTypeData);

		// should work
		try {
			ds.groupBy("nested.myInt");
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGroupByKeyExpressions2Nested() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds = env.fromCollection(customTypeData);

		// should not work, key out of tuple bounds
		ds.groupBy("nested.myNonExistent");
	}

	@Test
	@SuppressWarnings("serial")
	public void testGroupByKeySelector1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		this.customTypeData.add(new CustomType());

		try {
			DataSet<CustomType> customDs = env.fromCollection(customTypeData);
			// should work
			customDs.groupBy(
					new KeySelector<GroupingTest.CustomType, Long>() {

						@Override
						public Long getKey(CustomType value) {
							return value.myLong;
					}
			});
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	@SuppressWarnings("serial")
	public void testGroupByKeySelector2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		this.customTypeData.add(new CustomType());

		try {
			DataSet<CustomType> customDs = env.fromCollection(customTypeData);
			// should work
			customDs.groupBy(
					new KeySelector<GroupingTest.CustomType, Tuple2<Integer, Long>>() {
						@Override
						public Tuple2<Integer, Long> getKey(CustomType value) {
							return new Tuple2<Integer, Long>(value.myInt, value.myLong);
					}
			});
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	@SuppressWarnings("serial")
	public void testGroupByKeySelector3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		this.customTypeData.add(new CustomType());

		try {
			DataSet<CustomType> customDs = env.fromCollection(customTypeData);
			// should not work
			customDs.groupBy(
					new KeySelector<GroupingTest.CustomType, CustomType>() {
						@Override
						public CustomType getKey(CustomType value) {
							return value;
						}
					});
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	@SuppressWarnings("serial")
	public void testGroupByKeySelector4() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		this.customTypeData.add(new CustomType());

		try {
			DataSet<CustomType> customDs = env.fromCollection(customTypeData);
			// should not work
			customDs.groupBy(
					new KeySelector<GroupingTest.CustomType, Tuple2<Integer, GroupingTest.CustomType>>() {
						@Override
						public Tuple2<Integer, CustomType> getKey(CustomType value) {
							return new Tuple2<Integer, CustomType>(value.myInt, value);
						}
					});
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test(expected = InvalidProgramException.class)
	@SuppressWarnings("serial")
	public void testGroupByKeySelector5() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		this.customTypeData.add(new CustomType());

		DataSet<CustomType> customDs = env.fromCollection(customTypeData);
		// should not work
		customDs.groupBy(
				new KeySelector<GroupingTest.CustomType, CustomType2>() {
					@Override
					public CustomType2 getKey(CustomType value) {
						return new CustomType2();
					}
				});
	}

	@Test
	public void testGroupSortKeyFields1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			tupleDs.groupBy(0).sortGroup(0, Order.ASCENDING);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testGroupSortKeyFields2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, field index out of bounds
		tupleDs.groupBy(0).sortGroup(5, Order.ASCENDING);

	}

	@Test(expected = InvalidProgramException.class)
	public void testGroupSortKeyFields3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Long> longDs = env.fromCollection(emptyLongData, BasicTypeInfo.LONG_TYPE_INFO);

		// should not work: sorted groups on groupings by key selectors
		longDs.groupBy(new KeySelector<Long, Long>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Long getKey(Long value) {
				return value;
			}

		}).sortGroup(0, Order.ASCENDING);

	}

	@Test(expected = InvalidProgramException.class)
	public void testGroupSortKeyFields4() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
				env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// should not work
		tupleDs.groupBy(0)
				.sortGroup(2, Order.ASCENDING);
	}

	@Test(expected = InvalidProgramException.class)
	public void testGroupSortKeyFields5() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
				env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// should not work
		tupleDs.groupBy(0)
				.sortGroup(3, Order.ASCENDING);
	}

	@Test
	public void testChainedGroupSortKeyFields() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			tupleDs.groupBy(0).sortGroup(0, Order.ASCENDING).sortGroup(2, Order.DESCENDING);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	public void testGroupSortByKeyExpression1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
				env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// should work
		try {
			tupleDs.groupBy("f0").sortGroup("f1", Order.ASCENDING);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	public void testGroupSortByKeyExpression2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
				env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// should work
		try {
			tupleDs.groupBy("f0").sortGroup("f2.myString", Order.ASCENDING);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	public void testGroupSortByKeyExpression3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
				env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// should work
		try {
			tupleDs.groupBy("f0")
					.sortGroup("f2.myString", Order.ASCENDING)
					.sortGroup("f1", Order.DESCENDING);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test(expected = InvalidProgramException.class)
	public void testGroupSortByKeyExpression4() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
				env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// should not work
		tupleDs.groupBy("f0")
				.sortGroup("f2", Order.ASCENDING);
	}

	@Test(expected = InvalidProgramException.class)
	public void testGroupSortByKeyExpression5() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
				env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// should not work
		tupleDs.groupBy("f0")
				.sortGroup("f1", Order.ASCENDING)
				.sortGroup("f2", Order.ASCENDING);
	}

	@Test(expected = InvalidProgramException.class)
	public void testGroupSortByKeyExpression6() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
				env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// should not work
		tupleDs.groupBy("f0")
				.sortGroup("f3", Order.ASCENDING);
	}

	@SuppressWarnings("serial")
	@Test
	public void testGroupSortByKeySelector1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
				env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// should not work
		tupleDs.groupBy(
				new KeySelector<Tuple4<Integer, Long, CustomType, Long[]>, Long>() {
					@Override
					public Long getKey(Tuple4<Integer, Long, CustomType, Long[]> value) throws Exception {
						return value.f1;
					}
				})
				.sortGroup(
						new KeySelector<Tuple4<Integer, Long, CustomType, Long[]>, Integer>() {
							@Override
							public Integer getKey(Tuple4<Integer, Long, CustomType, Long[]> value) throws Exception {
								return value.f0;
							}
						}, Order.ASCENDING);
	}

	@SuppressWarnings("serial")
	@Test(expected = InvalidProgramException.class)
	public void testGroupSortByKeySelector2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
				env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// should not work
		tupleDs.groupBy(
				new KeySelector<Tuple4<Integer, Long, CustomType, Long[]>, Long>() {
					@Override
					public Long getKey(Tuple4<Integer, Long, CustomType, Long[]> value) throws Exception {
						return value.f1;
					}
				})
				.sortGroup(
						new KeySelector<Tuple4<Integer, Long, CustomType, Long[]>, CustomType>() {
							@Override
							public CustomType getKey(Tuple4<Integer, Long, CustomType, Long[]> value) throws Exception {
								return value.f2;
							}
						}, Order.ASCENDING);
	}

	@SuppressWarnings("serial")
	@Test(expected = InvalidProgramException.class)
	public void testGroupSortByKeySelector3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
				env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

		// should not work
		tupleDs.groupBy(
				new KeySelector<Tuple4<Integer, Long, CustomType, Long[]>, Long>() {
					@Override
					public Long getKey(Tuple4<Integer, Long, CustomType, Long[]> value) throws Exception {
						return value.f1;
					}
				})
				.sortGroup(
						new KeySelector<Tuple4<Integer, Long, CustomType, Long[]>, Long[]>() {
							@Override
							public Long[] getKey(Tuple4<Integer, Long, CustomType, Long[]> value) throws Exception {
								return value.f3;
							}
						}, Order.ASCENDING);
	}

	@Test
	public void testGroupingAtomicType() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Integer> dataSet = env.fromElements(0, 1, 1, 2, 0, 0);

		dataSet.groupBy("*");
	}

	@Test(expected = InvalidProgramException.class)
	public void testGroupAtomicTypeWithInvalid1() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Integer> dataSet = env.fromElements(0, 1, 2, 3);

		dataSet.groupBy("*", "invalidField");
	}

	@Test(expected = InvalidProgramException.class)
	public void testGroupAtomicTypeWithInvalid2() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Integer> dataSet = env.fromElements(0, 1, 2, 3);

		dataSet.groupBy("invalidField");
	}

	@Test(expected = InvalidProgramException.class)
	public void testGroupAtomicTypeWithInvalid3() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<ArrayList<Integer>> dataSet = env.fromElements(new ArrayList<Integer>());

		dataSet.groupBy("*");
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

	/**
	 * Custom non-nested data type, for testing purposes.
	 */
	public static class CustomType2 implements Serializable {

		public int myInt;
		public Integer[] myIntArray;

	}
}
