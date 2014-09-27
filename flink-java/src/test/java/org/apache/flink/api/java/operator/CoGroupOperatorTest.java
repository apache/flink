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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

@SuppressWarnings("serial")
public class CoGroupOperatorTest {

	// TUPLE DATA
	private static final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData = 
			new ArrayList<Tuple5<Integer, Long, String, Long, Integer>>();
	
	private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo = new 
			TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>>(
					BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.LONG_TYPE_INFO,
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.LONG_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO
			);
	
	private static List<CustomType> customTypeData = new ArrayList<CustomType>();
	
	@BeforeClass
	public static void insertCustomData() {
		customTypeData.add(new CustomType());
	}
	
	@Test  
	public void testCoGroupKeyFields1() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			ds1.coGroup(ds2).where(0).equalTo(0);
		} catch(Exception e) {
			Assert.fail();
		}
	}
	
	@Test(expected = InvalidProgramException.class)
	public void testCoGroupKeyFields2() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, incompatible cogroup key types
		ds1.coGroup(ds2).where(0).equalTo(2);
	}
	
	@Test(expected = InvalidProgramException.class)
	public void testCoGroupKeyFields3() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, incompatible number of cogroup keys
		ds1.coGroup(ds2).where(0,1).equalTo(2);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testCoGroupKeyFields4() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, cogroup key out of range
		ds1.coGroup(ds2).where(5).equalTo(0);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testCoGroupKeyFields5() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, negative key field position
		ds1.coGroup(ds2).where(-1).equalTo(-1);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testCoGroupKeyFields6() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, cogroup key fields on custom type
		ds1.coGroup(ds2).where(5).equalTo(0);
	}

	@Ignore
	@Test
	public void testCoGroupKeyExpressions1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should work
		try {
//			ds1.coGroup(ds2).where("myInt").equalTo("myInt");
		} catch(Exception e) {
			Assert.fail();
		}
	}

	@Ignore
	@Test(expected = InvalidProgramException.class)
	public void testCoGroupKeyExpressions2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, incompatible cogroup key types
//		ds1.coGroup(ds2).where("myInt").equalTo("myString");
	}

	@Ignore
	@Test(expected = InvalidProgramException.class)
	public void testCoGroupKeyExpressions3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, incompatible number of cogroup keys
//		ds1.coGroup(ds2).where("myInt", "myString").equalTo("myString");
	}

	@Ignore
	@Test(expected = IllegalArgumentException.class)
	public void testCoGroupKeyExpressions4() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, cogroup key non-existent
//		ds1.coGroup(ds2).where("myNonExistent").equalTo("myInt");
	}

	@Test
	public void testCoGroupKeySelectors1() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should work
		try {
			ds1.coGroup(ds2)
			.where(
					new KeySelector<CustomType, Long>() {
							
							@Override
							public Long getKey(CustomType value) {
								return value.myLong;
							}
						}
					)
			.equalTo(
					new KeySelector<CustomType, Long>() {
							
							@Override
							public Long getKey(CustomType value) {
								return value.myLong;
							}
						}
					);
		} catch(Exception e) {
			Assert.fail();
		}
	}
	
	@Test
	public void testCoGroupKeyMixing1() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		

		// should work
		try {
			ds1.coGroup(ds2)
			.where(
					new KeySelector<CustomType, Long>() {
							
							@Override
							public Long getKey(CustomType value) {
								return value.myLong;
							}
						}
					)
			.equalTo(3);
		} catch(Exception e) {
			Assert.fail();
		}
	}
	
	@Test
	public void testCoGroupKeyMixing2() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should work
		try {
			ds1.coGroup(ds2)
			.where(3)
			.equalTo(
					new KeySelector<CustomType, Long>() {
							
							@Override
							public Long getKey(CustomType value) {
								return value.myLong;
							}
						}
					);
		} catch(Exception e) {
			Assert.fail();
		}
	}
	
	@Test(expected = InvalidProgramException.class)
	public void testCoGroupKeyMixing3() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, incompatible types
		ds1.coGroup(ds2)
		.where(2)
		.equalTo(
				new KeySelector<CustomType, Long>() {
						
						@Override
						public Long getKey(CustomType value) {
							return value.myLong;
						}
					}
				);
	}
	
	@Test(expected = InvalidProgramException.class)
	public void testCoGroupKeyMixing4() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, more than one key field position
		ds1.coGroup(ds2)
		.where(1,3)
		.equalTo(
				new KeySelector<CustomType, Long>() {
						
						@Override
						public Long getKey(CustomType value) {
							return value.myLong;
						}
					}
				);
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
