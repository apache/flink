/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.operator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;

@SuppressWarnings("serial")
public class JoinOperatorTest {

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
	public void testJoinKeyFields1() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			ds1.join(ds2).where(0).equalTo(0);
		} catch(Exception e) {
			Assert.fail();
		}
	}
	
	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyFields2() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, incompatible join key types
		ds1.join(ds2).where(0).equalTo(2);
	}
	
	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyFields3() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, incompatible number of join keys
		ds1.join(ds2).where(0,1).equalTo(2);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testJoinKeyFields4() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, join key out of range
		ds1.join(ds2).where(5).equalTo(0);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testJoinKeyFields5() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, empty join key
		ds1.join(ds2).where().equalTo();
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testJoinKeyFields6() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, negative key field position
		ds1.join(ds2).where(-1).equalTo(-1);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testJoinKeyFields7() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, join key fields on custom type
		ds1.join(ds2).where(5).equalTo(0);
	}
	
	@Test
	public void testJoinKeySelectors1() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should work
		try {
			ds1.join(ds2)
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
	public void testJoinKeyMixing1() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		

		// should work
		try {
			ds1.join(ds2)
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
	
	// TODO: Activate once mixed key types work for join
//	@Test
//	public void testJoinKeyMixing2() {
//		
//		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
//		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);
//
//		// should work
//		try {
//			ds1.join(ds2)
//			   .where(3)
//			   .equalTo(
//					   new KeySelector<CustomType, Long>() {
//							
//							@Override
//							public Long getKey(CustomType value) {
//								return value.myLong;
//							}
//						}
//					   );
//		} catch(Exception e) {
//			Assert.fail();
//		}
//	}
	
	// TODO: Activate once mixed key types work for join
//	@Test(expected = IllegalArgumentException.class)
//	public void testJoinKeyMixing3() {
//		
//		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
//		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);
//
//		// should not work, incompatible types
//		ds1.join(ds2)
//		   .where(2)
//		   .equalTo(
//				   new KeySelector<CustomType, Long>() {
//						
//						@Override
//						public Long getKey(CustomType value) {
//							return value.myLong;
//						}
//					}
//				   );
//	}
	
	// TODO: Activate once mixed key types work for join
//	@Test(expected = IllegalArgumentException.class)
//	public void testJoinKeyMixing4() {
//		
//		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
//		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);
//
//		// should not work, more than one key field position
//		ds1.join(ds2)
//		   .where(1,3)
//		   .equalTo(
//				   new KeySelector<CustomType, Long>() {
//						
//						@Override
//						public Long getKey(CustomType value) {
//							return value.myLong;
//						}
//					}
//				   );
//	}
		
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
