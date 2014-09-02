/**
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

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
	
	// LONG DATA
	private final List<Long> emptyLongData = new ArrayList<Long>();
	
	private final List<CustomType> customTypeData = new ArrayList<CustomType>();
	
	@Test  
	public void testGroupByKeyFields1() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			tupleDs.groupBy(0);
		} catch(Exception e) {
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
	
	@Test(expected = IllegalArgumentException.class)
	public void testGroupByKeyFields4() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, key out of tuple bounds
		tupleDs.groupBy(5);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testGroupByKeyFields5() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, negative field position
		tupleDs.groupBy(-1);
	}

	@Ignore
	@Test
	public void testGroupByKeyExpressions1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		this.customTypeData.add(new CustomType());

		DataSet<CustomType> ds = env.fromCollection(customTypeData);

		// should work
		try {
//			ds.groupBy("myInt");
		} catch(Exception e) {
			Assert.fail();
		}
	}

	@Ignore
	@Test(expected = UnsupportedOperationException.class)
	public void testGroupByKeyExpressions2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Long> longDs = env.fromCollection(emptyLongData, BasicTypeInfo.LONG_TYPE_INFO);
		// should not work: groups on basic type
//		longDs.groupBy("myInt");
	}

	@Ignore
	@Test(expected = InvalidProgramException.class)
	public void testGroupByKeyExpressions3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		this.customTypeData.add(new CustomType());

		DataSet<CustomType> customDs = env.fromCollection(customTypeData);
		// should not work: groups on custom type
		customDs.groupBy(0);

	}

	@Ignore
	@Test(expected = IllegalArgumentException.class)
	public void testGroupByKeyExpressions4() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds = env.fromCollection(customTypeData);

		// should not work, key out of tuple bounds
//		ds.groupBy("myNonExistent");
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
		} catch(Exception e) {
			Assert.fail();
		}
		
	}
	
	@Test
	public void testGroupSortKeyFields1() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			tupleDs.groupBy(0).sortGroup(0, Order.ASCENDING);
		} catch(Exception e) {
			Assert.fail();
		}
	}
	
	@Test(expected = IllegalArgumentException.class)
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
	
	@Test
	public void testChainedGroupSortKeyFields() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			tupleDs.groupBy(0).sortGroup(0, Order.ASCENDING).sortGroup(2, Order.DESCENDING);
		} catch(Exception e) {
			Assert.fail();
		}
	}
	

	public static class CustomType implements Serializable {
		
		private static final long serialVersionUID = 1L;
		
		public int myInt;
		public long myLong;
		public String myString;
		
		public CustomType() {};
		
		public CustomType(int i, long l, String s) {
			myInt = i;
			myLong = l;
			myString = s;
		}
		
		@Override
		public String toString() {
			return myInt+","+myLong+","+myString;
		}
	}
}
