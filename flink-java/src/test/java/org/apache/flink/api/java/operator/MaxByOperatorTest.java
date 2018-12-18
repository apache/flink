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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link DataSet#maxBy(int...)}.
 */
public class MaxByOperatorTest {

	// TUPLE DATA
	private final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData = new ArrayList<Tuple5<Integer, Long, String, Long, Integer>>();

	private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo = new TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>>(
			BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO);

	/**
	 * This test validates that no exceptions is thrown when an empty dataset
	 * calls maxBy().
	 */
	@Test
	public void testMaxByKeyFieldsDataset() {

		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env
				.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			tupleDs.maxBy(4, 0, 1, 2, 3);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	private final List<CustomType> customTypeData = new ArrayList<CustomType>();

	/**
	 * This test validates that an InvalidProgramException is thrown when maxBy
	 * is used on a custom data type.
	 */
	@Test(expected = InvalidProgramException.class)
	public void testCustomKeyFieldsDataset() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		this.customTypeData.add(new CustomType());

		DataSet<CustomType> customDs = env.fromCollection(customTypeData);
		// should not work: groups on custom type
		customDs.maxBy(0);
	}

	/**
	 * This test validates that an index which is out of bounds throws an
	 * IndexOutOfBoundsException.
	 */
	@Test(expected = IndexOutOfBoundsException.class)
	public void testOutOfTupleBoundsDataset1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, key out of tuple bounds
		tupleDs.maxBy(5);
	}

	/**
	 * This test validates that an index which is out of bounds throws an
	 * IndexOutOfBoundsException.
	 */
	@Test(expected = IndexOutOfBoundsException.class)
	public void testOutOfTupleBoundsDataset2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, key out of tuple bounds
		tupleDs.maxBy(-1);
	}

	/**
	 * This test validates that an index which is out of bounds throws an
	 * IndexOutOfBoundsException.
	 */
	@Test(expected = IndexOutOfBoundsException.class)
	public void testOutOfTupleBoundsDataset3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, key out of tuple bounds
		tupleDs.maxBy(1, 2, 3, 4, -1);
	}

	//---------------------------- GROUPING TESTS BELOW --------------------------------------

	/**
	 * This test validates that no exceptions is thrown when an empty grouping
	 * calls maxBy().
	 */
	@Test
	public void testMaxByKeyFieldsGrouping() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		UnsortedGrouping<Tuple5<Integer, Long, String, Long, Integer>> groupDs = env.fromCollection(emptyTupleData, tupleTypeInfo).groupBy(0);

		// should work
		try {
			groupDs.maxBy(4, 0, 1, 2, 3);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * This test validates that an InvalidProgramException is thrown when maxBy
	 * is used on a custom data type.
	 */
	@Test(expected = InvalidProgramException.class)
	public void testCustomKeyFieldsGrouping() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		this.customTypeData.add(new CustomType());

		UnsortedGrouping<CustomType> groupDs = env.fromCollection(customTypeData).groupBy(0);
		// should not work: groups on custom type
		groupDs.maxBy(0);
	}

	/**
	 * This test validates that an index which is out of bounds throws an
	 * IndexOutOfBoundsException.
	 */
	@Test(expected = IndexOutOfBoundsException.class)
	public void testOutOfTupleBoundsGrouping1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		UnsortedGrouping<Tuple5<Integer, Long, String, Long, Integer>> groupDs = env.fromCollection(emptyTupleData, tupleTypeInfo).groupBy(0);

		// should not work, key out of tuple bounds
		groupDs.maxBy(5);
	}

	/**
	 * This test validates that an index which is out of bounds throws an
	 * IndexOutOfBoundsException.
	 */
	@Test(expected = IndexOutOfBoundsException.class)
	public void testOutOfTupleBoundsGrouping2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		UnsortedGrouping<Tuple5<Integer, Long, String, Long, Integer>> groupDs = env.fromCollection(emptyTupleData, tupleTypeInfo).groupBy(0);

		// should not work, key out of tuple bounds
		groupDs.maxBy(-1);
	}

	/**
	 * This test validates that an index which is out of bounds throws an
	 * IndexOutOfBoundsException.
	 */
	@Test(expected = IndexOutOfBoundsException.class)
	public void testOutOfTupleBoundsGrouping3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		UnsortedGrouping<Tuple5<Integer, Long, String, Long, Integer>> groupDs = env.fromCollection(emptyTupleData, tupleTypeInfo).groupBy(0);

		// should not work, key out of tuple bounds
		groupDs.maxBy(1, 2, 3, 4, -1);
	}

	/**
	 * Custom data type, for testing purposes.
	 */
	public static class CustomType implements Serializable {

		private static final long serialVersionUID = 1L;

		public int myInt;
		public long myLong;
		public String myString;

		public CustomType() {
		}

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
