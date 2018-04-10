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
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DataSet#join(DataSet)}.
 */
@SuppressWarnings("serial")
public class JoinOperatorTest {

	// TUPLE DATA
	private static final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData =
			new ArrayList<Tuple5<Integer, Long, String, Long, Integer>>();

	private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo =
			new TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>>(
					BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.LONG_TYPE_INFO,
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.LONG_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO
			);
	// TUPLE DATA with nested Tuple2
	private static final List<Tuple5<Tuple2<Integer, String>, Long, String, Long, Integer>> emptyNestedTupleData =
			new ArrayList<Tuple5<Tuple2<Integer, String>, Long, String, Long, Integer>>();

	private final TupleTypeInfo<Tuple5<Tuple2<Integer, String>, Long, String, Long, Integer>> nestedTupleTypeInfo =
			new TupleTypeInfo<Tuple5<Tuple2<Integer, String>, Long, String, Long, Integer>>(
					new TupleTypeInfo<Tuple2<Integer, String>> (BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
					BasicTypeInfo.LONG_TYPE_INFO,
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.LONG_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO
			);

	// TUPLE DATA with nested CustomType
	private static final List<Tuple5<CustomType, Long, String, Long, Integer>> emptyNestedCustomTupleData =
			new ArrayList<Tuple5<CustomType, Long, String, Long, Integer>>();

	private final TupleTypeInfo<Tuple5<CustomType, Long, String, Long, Integer>> nestedCustomTupleTypeInfo =
			new TupleTypeInfo<Tuple5<CustomType, Long, String, Long, Integer>>(
					TypeExtractor.getForClass(CustomType.class),
					BasicTypeInfo.LONG_TYPE_INFO,
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.LONG_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO
			);

	private static List<CustomTypeWithTuple> customTypeWithTupleData = new ArrayList<CustomTypeWithTuple>();
	private static List<CustomType> customTypeData = new ArrayList<CustomType>();

	private static List<NestedCustomType> customNestedTypeData = new ArrayList<NestedCustomType>();

	@BeforeClass
	public static void insertCustomData() {
		customTypeData.add(new CustomType());
		customTypeWithTupleData.add(new CustomTypeWithTuple());
		customNestedTypeData.add(new NestedCustomType());
	}

	@Test
	public void testJoinKeyFields1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			ds1.join(ds2).where(0).equalTo(0);
		} catch (Exception e) {
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
		ds1.join(ds2).where(0, 1).equalTo(2);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testJoinKeyFields4() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, join key out of range
		ds1.join(ds2).where(5).equalTo(0);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testJoinKeyFields5() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, negative key field position
		ds1.join(ds2).where(-1).equalTo(-1);
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyFields6() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, join key fields on custom type
		ds1.join(ds2).where(4).equalTo(0);
	}

	@Test
	public void testJoinKeyExpressions1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should work
		try {
			ds1.join(ds2).where("myInt").equalTo("myInt");
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	public void testJoinKeyExpressionsNested() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<NestedCustomType> ds1 = env.fromCollection(customNestedTypeData);
		DataSet<NestedCustomType> ds2 = env.fromCollection(customNestedTypeData);

		// should work
		try {
			ds1.join(ds2).where("myInt").equalTo("myInt");
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyExpressions2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, incompatible join key types
		ds1.join(ds2).where("myInt").equalTo("myString");
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyExpressions3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, incompatible number of join keys
		ds1.join(ds2).where("myInt", "myString").equalTo("myString");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testJoinKeyExpressions4() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, join key non-existent
		ds1.join(ds2).where("myNonExistent").equalTo("myInt");
	}

	/**
	 * Test if mixed types of key selectors are properly working.
	 */
	@Test
	public void testJoinKeyMixedKeySelector() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);
		try {
			ds1.join(ds2).where("myInt").equalTo(new KeySelector<CustomType, Integer>() {
				@Override
				public Integer getKey(CustomType value) throws Exception {
					return value.myInt;
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testJoinKeyMixedKeySelectorTurned() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);
		try {
			ds1.join(ds2).where(new KeySelector<CustomType, Integer>() {
				@Override
				public Integer getKey(CustomType value) throws Exception {
					return value.myInt;
				}
			}).equalTo("myInt");
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testJoinKeyMixedTupleIndex() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		try {
			ds1.join(ds2).where("f0").equalTo(4);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testJoinKeyNestedTuples() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Tuple2<Integer, String>, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyNestedTupleData, nestedTupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		try {
			ds1.join(ds2).where("f0.f0").equalTo(4);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testJoinKeyNestedTuplesWithCustom() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<CustomType, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyNestedCustomTupleData, nestedCustomTupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		try {
			TypeInformation<?> t = ds1.join(ds2).where("f0.myInt").equalTo(4).getType();
			assertTrue("not a composite type", t instanceof CompositeType);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testJoinKeyWithCustomContainingTuple0() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomTypeWithTuple> ds1 = env.fromCollection(customTypeWithTupleData);
		DataSet<CustomTypeWithTuple> ds2 = env.fromCollection(customTypeWithTupleData);
		try {
			ds1.join(ds2).where("intByString.f0").equalTo("myInt");
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testJoinKeyWithCustomContainingTuple1() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomTypeWithTuple> ds1 = env.fromCollection(customTypeWithTupleData);
		DataSet<CustomTypeWithTuple> ds2 = env.fromCollection(customTypeWithTupleData);
		try {
			ds1.join(ds2).where("nested.myInt").equalTo("intByString.f0");
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testJoinKeyWithCustomContainingTuple2() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomTypeWithTuple> ds1 = env.fromCollection(customTypeWithTupleData);
		DataSet<CustomTypeWithTuple> ds2 = env.fromCollection(customTypeWithTupleData);
		try {
			ds1.join(ds2).where("nested.myInt", "myInt", "intByString.f1").equalTo("intByString.f0", "myInt", "myString");
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyNestedTuplesWrongType() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Tuple2<Integer, String>, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyNestedTupleData, nestedTupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		ds1.join(ds2).where("f0.f1").equalTo(4); // f0.f1 is a String
	}

	@Test
	public void testJoinKeyMixedTupleIndexTurned() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		try {
			ds1.join(ds2).where(0).equalTo("f0");
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyMixedTupleIndexWrongType() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		ds1.join(ds2).where("f0").equalTo(3); // 3 is of type long, so it should fail
	}

	@Test
	public void testJoinKeyMixedTupleIndex2() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		try {
			ds1.join(ds2).where("myInt").equalTo(4);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyMixedWrong() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);
		// wrongly mix String and Integer
		ds1.join(ds2).where("myString").equalTo(new KeySelector<CustomType, Integer>() {
			@Override
			public Integer getKey(CustomType value) throws Exception {
				return value.myInt;
			}
		});
	}

	@Test
	public void testJoinKeyExpressions1Nested() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should work
		try {
			ds1.join(ds2).where("nested.myInt").equalTo("nested.myInt");
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyExpressions2Nested() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, incompatible join key types
		ds1.join(ds2).where("nested.myInt").equalTo("nested.myString");
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyExpressions3Nested() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, incompatible number of join keys
		ds1.join(ds2).where("nested.myInt", "nested.myString").equalTo("nested.myString");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testJoinKeyExpressions4Nested() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, join key non-existent
		ds1.join(ds2).where("nested.myNonExistent").equalTo("nested.myInt");
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
		} catch (Exception e) {
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
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testJoinKeyMixing2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should work
		try {
			ds1.join(ds2)
			.where(3)
			.equalTo(
					new KeySelector<CustomType, Long>() {

							@Override
							public Long getKey(CustomType value) {
								return value.myLong;
							}
						}
					);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyMixing3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, incompatible types
		ds1.join(ds2)
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
	public void testJoinKeyMixing4() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

		// should not work, more than one key field position
		ds1.join(ds2)
		.where(1, 3)
		.equalTo(
				new KeySelector<CustomType, Long>() {

						@Override
						public Long getKey(CustomType value) {
							return value.myLong;
						}
					}
				);
	}

	@Test
	public void testJoinKeyAtomic1() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Integer> ds1 = env.fromElements(0, 0, 0);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		ds1.join(ds2).where("*").equalTo(0);
	}

	@Test
	public void testJoinKeyAtomic2() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Integer> ds2 = env.fromElements(0, 0, 0);

		ds1.join(ds2).where(0).equalTo("*");
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyInvalidAtomic1() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Integer> ds1 = env.fromElements(0, 0, 0);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		ds1.join(ds2).where("*", "invalidKey");
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyInvalidAtomic2() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Integer> ds2 = env.fromElements(0, 0, 0);

		ds1.join(ds2).where(0).equalTo("*", "invalidKey");
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyInvalidAtomic3() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Integer> ds1 = env.fromElements(0, 0, 0);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		ds1.join(ds2).where("invalidKey");
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyInvalidAtomic4() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Integer> ds2 = env.fromElements(0, 0, 0);

		ds1.join(ds2).where(0).equalTo("invalidKey");
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyInvalidAtomic5() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<ArrayList<Integer>> ds1 = env.fromElements(new ArrayList<Integer>());
		DataSet<Integer> ds2 = env.fromElements(0, 0, 0);

		ds1.join(ds2).where("*").equalTo("*");
	}

	@Test(expected = InvalidProgramException.class)
	public void testJoinKeyInvalidAtomic6() {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Integer> ds1 = env.fromElements(0, 0, 0);
		DataSet<ArrayList<Integer>> ds2 = env.fromElements(new ArrayList<Integer>());

		ds1.join(ds2).where("*").equalTo("*");
	}

	@Test
	public void testJoinProjection1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			ds1.join(ds2).where(0).equalTo(0)
			.projectFirst(0);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	public void testJoinProjection21() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			ds1.join(ds2).where(0).equalTo(0)
			.projectFirst(0);
		} catch (Exception e) {
			Assert.fail();
		}

		// should not work: field index is out of bounds of input tuple
		try {
			ds1.join(ds2).where(0).equalTo(0).projectFirst(-1);
			Assert.fail();
		} catch (IndexOutOfBoundsException iob) {
			// we're good here
		} catch (Exception e) {
			Assert.fail();
		}

		// should not work: field index is out of bounds of input tuple
		try {
			ds1.join(ds2).where(0).equalTo(0).project(9);
			Assert.fail();
		} catch (IndexOutOfBoundsException iob) {
			// we're good here
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	public void testJoinProjection2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			ds1.join(ds2).where(0).equalTo(0)
			.projectFirst(0, 3);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	public void testJoinProjection3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			ds1.join(ds2).where(0).equalTo(0)
			.projectFirst(0)
			.projectSecond(3);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	public void testJoinProjection4() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			ds1.join(ds2).where(0).equalTo(0)
			.projectFirst(0, 2)
			.projectSecond(1, 4)
			.projectFirst(1);
		} catch (Exception e) {
			Assert.fail();
		}

	}

	@Test
	public void testJoinProjection5() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			ds1.join(ds2).where(0).equalTo(0)
			.projectSecond(0, 2)
			.projectFirst(1, 4)
			.projectFirst(1);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	public void testJoinProjection6() {

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
			)
			.projectFirst()
			.projectSecond();
		} catch (Exception e) {
			System.out.println("FAILED: " + e);
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testJoinProjection26() {

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
			)
			.projectFirst()
			.projectSecond();
		} catch (Exception e) {
			System.out.println("FAILED: " + e);
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testJoinProjection7() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			ds1.join(ds2).where(0).equalTo(0)
			.projectSecond()
			.projectFirst(1, 4);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	public void testJoinProjection27() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			ds1.join(ds2).where(0).equalTo(0)
			.projectSecond()
			.projectFirst(1, 4);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testJoinProjection8() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, index out of range
		ds1.join(ds2).where(0).equalTo(0)
		.projectFirst(5);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testJoinProjection28() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, index out of range
		ds1.join(ds2).where(0).equalTo(0)
		.projectFirst(5);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testJoinProjection9() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, index out of range
		ds1.join(ds2).where(0).equalTo(0)
		.projectSecond(5);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testJoinProjection29() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, index out of range
		ds1.join(ds2).where(0).equalTo(0)
		.projectSecond(5);
	}

	public void testJoinProjection10() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		ds1.join(ds2).where(0).equalTo(0)
		.projectFirst(2);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testJoinProjection30() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, type does not match
		ds1.join(ds2).where(0).equalTo(0)
		.projectFirst(-1);
	}

	public void testJoinProjection11() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, type does not match
		ds1.join(ds2).where(0).equalTo(0)
		.projectSecond(2);
	}

	public void testJoinProjection12() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should  work
		ds1.join(ds2).where(0).equalTo(0)
		.projectSecond(2)
		.projectFirst(1);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testJoinProjection13() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, index out of range
		ds1.join(ds2).where(0).equalTo(0)
		.projectSecond(0)
		.projectFirst(5);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testJoinProjection33() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, index out of range
		ds1.join(ds2).where(0).equalTo(0)
		.projectSecond(-1)
		.projectFirst(3);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testJoinProjection14() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, index out of range
		ds1.join(ds2).where(0).equalTo(0)
		.projectFirst(0)
		.projectSecond(5);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testJoinProjection34() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should not work, index out of range
		ds1.join(ds2).where(0).equalTo(0)
		.projectFirst(0)
		.projectSecond(-1);
	}

	@Test
	public void testSemanticPropsWithKeySelector1() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		JoinOperator<?, ?, ?> joinOp = tupleDs1.join(tupleDs2)
				.where(new DummyTestKeySelector()).equalTo(new DummyTestKeySelector())
				.with(new DummyTestJoinFunction1());

		SemanticProperties semProps = joinOp.getSemanticProperties();

		assertTrue(semProps.getForwardingTargetFields(0, 0).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(0, 1).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(0, 2).size() == 1);
		assertTrue(semProps.getForwardingTargetFields(0, 2).contains(4));
		assertTrue(semProps.getForwardingTargetFields(0, 3).size() == 2);
		assertTrue(semProps.getForwardingTargetFields(0, 3).contains(1));
		assertTrue(semProps.getForwardingTargetFields(0, 3).contains(3));
		assertTrue(semProps.getForwardingTargetFields(0, 4).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(0, 5).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(0, 6).size() == 0);

		assertTrue(semProps.getForwardingTargetFields(1, 0).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(1, 1).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(1, 2).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(1, 3).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(1, 4).size() == 1);
		assertTrue(semProps.getForwardingTargetFields(1, 4).contains(2));
		assertTrue(semProps.getForwardingTargetFields(1, 5).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(1, 6).size() == 1);
		assertTrue(semProps.getForwardingTargetFields(1, 6).contains(0));

		assertTrue(semProps.getReadFields(0).size() == 3);
		assertTrue(semProps.getReadFields(0).contains(2));
		assertTrue(semProps.getReadFields(0).contains(4));
		assertTrue(semProps.getReadFields(0).contains(6));

		assertTrue(semProps.getReadFields(1).size() == 2);
		assertTrue(semProps.getReadFields(1).contains(3));
		assertTrue(semProps.getReadFields(1).contains(5));
	}

	@Test
	public void testSemanticPropsWithKeySelector2() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		JoinOperator<?, ?, ?> joinOp = tupleDs1.join(tupleDs2)
				.where(new DummyTestKeySelector()).equalTo(new DummyTestKeySelector())
				.with(new DummyTestJoinFunction2())
					.withForwardedFieldsFirst("2;4->0")
					.withForwardedFieldsSecond("0->4;1;1->3");

		SemanticProperties semProps = joinOp.getSemanticProperties();

		assertTrue(semProps.getForwardingTargetFields(0, 0).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(0, 1).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(0, 2).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(0, 3).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(0, 4).size() == 1);
		assertTrue(semProps.getForwardingTargetFields(0, 4).contains(2));
		assertTrue(semProps.getForwardingTargetFields(0, 5).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(0, 6).size() == 1);
		assertTrue(semProps.getForwardingTargetFields(0, 6).contains(0));

		assertTrue(semProps.getForwardingTargetFields(1, 0).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(1, 1).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(1, 2).size() == 1);
		assertTrue(semProps.getForwardingTargetFields(1, 2).contains(4));
		assertTrue(semProps.getForwardingTargetFields(1, 3).size() == 2);
		assertTrue(semProps.getForwardingTargetFields(1, 3).contains(1));
		assertTrue(semProps.getForwardingTargetFields(1, 3).contains(3));
		assertTrue(semProps.getForwardingTargetFields(1, 4).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(1, 5).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(1, 6).size() == 0);

		assertTrue(semProps.getReadFields(0).size() == 3);
		assertTrue(semProps.getReadFields(0).contains(2));
		assertTrue(semProps.getReadFields(0).contains(3));
		assertTrue(semProps.getReadFields(0).contains(4));

		assertTrue(semProps.getReadFields(1) == null);
	}

	@Test
	public void testSemanticPropsWithKeySelector3() {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs1 = env.fromCollection(emptyTupleData, tupleTypeInfo);
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs2 = env.fromCollection(emptyTupleData, tupleTypeInfo);

		JoinOperator<?, ?, ? extends Tuple> joinOp = tupleDs1.join(tupleDs2)
				.where(new DummyTestKeySelector()).equalTo(new DummyTestKeySelector())
				.projectFirst(2)
				.projectSecond(0, 0, 3)
				.projectFirst(0, 4)
				.projectSecond(2);

		SemanticProperties semProps = joinOp.getSemanticProperties();

		assertTrue(semProps.getForwardingTargetFields(0, 0).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(0, 1).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(0, 2).size() == 1);
		assertTrue(semProps.getForwardingTargetFields(0, 2).contains(4));
		assertTrue(semProps.getForwardingTargetFields(0, 3).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(0, 4).size() == 1);
		assertTrue(semProps.getForwardingTargetFields(0, 4).contains(0));
		assertTrue(semProps.getForwardingTargetFields(0, 5).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(0, 6).size() == 1);
		assertTrue(semProps.getForwardingTargetFields(0, 6).contains(5));

		assertTrue(semProps.getForwardingTargetFields(1, 0).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(1, 1).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(1, 2).size() == 2);
		assertTrue(semProps.getForwardingTargetFields(1, 2).contains(1));
		assertTrue(semProps.getForwardingTargetFields(1, 2).contains(2));
		assertTrue(semProps.getForwardingTargetFields(1, 3).size() == 0);
		assertTrue(semProps.getForwardingTargetFields(1, 4).size() == 1);
		assertTrue(semProps.getForwardingTargetFields(1, 4).contains(6));
		assertTrue(semProps.getForwardingTargetFields(1, 5).size() == 1);
		assertTrue(semProps.getForwardingTargetFields(1, 5).contains(3));
		assertTrue(semProps.getForwardingTargetFields(1, 6).size() == 0);

	}

	/*
	 * ####################################################################
	 */

	/**
	 * Custom type for testing.
	 */
	public static class Nested implements Serializable {

		private static final long serialVersionUID = 1L;

		public int myInt;

		public Nested() {}

		public Nested(int i, long l, String s) {
			myInt = i;
		}

		@Override
		public String toString() {
			return "" + myInt;
		}
	}

	/**
	 * Simple nested type (only basic types).
	 */
	public static class NestedCustomType implements Serializable {

		private static final long serialVersionUID = 1L;

		public int myInt;
		public long myLong;
		public String myString;
		public Nested nest;

		public NestedCustomType() {
		}

		public NestedCustomType(int i, long l, String s) {
			myInt = i;
			myLong = l;
			myString = s;
		}

		@Override
		public String toString() {
			return myInt + "," + myLong + "," + myString + "," + nest;
		}
	}

	/**
	 * Custom type for testing.
	 */
	public static class CustomType implements Serializable {

		private static final long serialVersionUID = 1L;

		public int myInt;
		public long myLong;
		public NestedCustomType nested;
		public String myString;
		public Object nothing;
		public List<String> countries;

		public CustomType() {
		}

		public CustomType(int i, long l, String s) {
			myInt = i;
			myLong = l;
			myString = s;
			countries = null;
			nested = new NestedCustomType(i, l, s);
		}

		@Override
		public String toString() {
			return myInt + "," + myLong + "," + myString;
		}
	}

	/**
	 * Custom type for testing.
	 */
	public static class CustomTypeWithTuple implements Serializable {

		private static final long serialVersionUID = 1L;

		public int myInt;
		public long myLong;
		public NestedCustomType nested;
		public String myString;
		public Tuple2<Integer, String> intByString;

		public CustomTypeWithTuple() {
		}

		public CustomTypeWithTuple(int i, long l, String s) {
			myInt = i;
			myLong = l;
			myString = s;
			nested = new NestedCustomType(i, l, s);
			intByString = new Tuple2<Integer, String>(i, s);
		}

		@Override
		public String toString() {
			return myInt + "," + myLong + "," + myString;
		}
	}

	private static class DummyTestKeySelector implements KeySelector<Tuple5<Integer, Long, String, Long, Integer>, Tuple2<Long, Integer>> {
		@Override
		public Tuple2<Long, Integer> getKey(Tuple5<Integer, Long, String, Long, Integer> value) throws Exception {
			return new Tuple2<Long, Integer>();
		}
	}

	@FunctionAnnotation.ForwardedFieldsFirst("0->4;1;1->3")
	@FunctionAnnotation.ForwardedFieldsSecond("2;4->0")
	@FunctionAnnotation.ReadFieldsFirst("0;2;4")
	@FunctionAnnotation.ReadFieldsSecond("1;3")
	private static class DummyTestJoinFunction1
			implements JoinFunction<Tuple5<Integer, Long, String, Long, Integer>,
									Tuple5<Integer, Long, String, Long, Integer>,
									Tuple5<Integer, Long, String, Long, Integer>> {
		@Override
		public Tuple5<Integer, Long, String, Long, Integer> join(
				Tuple5<Integer, Long, String, Long, Integer> first,
				Tuple5<Integer, Long, String, Long, Integer> second) throws Exception {
			return new Tuple5<Integer, Long, String, Long, Integer>();
		}
	}

	@FunctionAnnotation.ReadFieldsFirst("0;1;2")
	private static class DummyTestJoinFunction2
			implements JoinFunction<Tuple5<Integer, Long, String, Long, Integer>,
			Tuple5<Integer, Long, String, Long, Integer>,
			Tuple5<Integer, Long, String, Long, Integer>> {
		@Override
		public Tuple5<Integer, Long, String, Long, Integer> join(
				Tuple5<Integer, Long, String, Long, Integer> first,
				Tuple5<Integer, Long, String, Long, Integer> second) throws Exception {
			return new Tuple5<Integer, Long, String, Long, Integer>();
		}
	}

}
