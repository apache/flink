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
package eu.stratosphere.api.java.type.extractor;

import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.CrossFunction;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.InvalidTypesException;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple9;
import eu.stratosphere.api.java.typeutils.BasicArrayTypeInfo;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.GenericTypeInfo;
import eu.stratosphere.api.java.typeutils.ObjectArrayTypeInfo;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.api.java.typeutils.ValueTypeInfo;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

public class TypeExtractorTest {

	@Test
	public void testBasicType() {
		// use getGroupReduceReturnTypes()
		GroupReduceFunction<?, ?> function = new GroupReduceFunction<Boolean, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void reduce(Iterator<Boolean> values, Collector<Boolean> out) throws Exception {
				// nothing to do
			}
		};

		TypeInformation<?> ti = TypeExtractor.getGroupReduceReturnTypes(function, null);

		Assert.assertTrue(ti.isBasicType());
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti);
		Assert.assertEquals(Boolean.class, ti.getTypeClass());

		// use getForClass()
		Assert.assertTrue(TypeExtractor.getForClass(Boolean.class).isBasicType());
		Assert.assertEquals(ti, TypeExtractor.getForClass(Boolean.class));

		// use getForObject()
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, TypeExtractor.getForObject(Boolean.valueOf(true)));
	}

	@Test
	public void testTupleWithBasicTypes() throws Exception {
		// use getMapReturnTypes()
		MapFunction<?, ?> function = new MapFunction<Tuple9<Integer, Long, Double, Float, Boolean, String, Character, Short, Byte>, Tuple9<Integer, Long, Double, Float, Boolean, String, Character, Short, Byte>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple9<Integer, Long, Double, Float, Boolean, String, Character, Short, Byte> map(
					Tuple9<Integer, Long, Double, Float, Boolean, String, Character, Short, Byte> value) throws Exception {
				return null;
			}

		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(9, ti.getArity());
		Assert.assertTrue(ti instanceof TupleTypeInfo);

		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;

		for (int i = 0; i < 9; i++) {
			Assert.assertTrue(tti.getTypeAt(i) instanceof BasicTypeInfo);
		}

		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(1));
		Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, tti.getTypeAt(2));
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti.getTypeAt(3));
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti.getTypeAt(4));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(5));
		Assert.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, tti.getTypeAt(6));
		Assert.assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, tti.getTypeAt(7));
		Assert.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, tti.getTypeAt(8));

		// use getForObject()
		Tuple9<Integer, Long, Double, Float, Boolean, String, Character, Short, Byte> t = new Tuple9<Integer, Long, Double, Float, Boolean, String, Character, Short, Byte>(
				1, 1L, 1.0, 1.0F, false, "Hello World", 'w', (short) 1, (byte) 1);

		Assert.assertTrue(TypeExtractor.getForObject(t) instanceof TupleTypeInfo);
		TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>) TypeExtractor.getForObject(t);

		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti2.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti2.getTypeAt(1));
		Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, tti2.getTypeAt(2));
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti2.getTypeAt(3));
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti2.getTypeAt(4));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti2.getTypeAt(5));
		Assert.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, tti2.getTypeAt(6));
		Assert.assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, tti2.getTypeAt(7));
		Assert.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, tti2.getTypeAt(8));
	}

	@Test
	public void testTupleWithTuples() {
		// use getFlatMapReturnTypes()
		FlatMapFunction<?, ?> function = new FlatMapFunction<Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>>, Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>> value,
					Collector<Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>>> out) throws Exception {
				// nothing to do
			}
		};

		TypeInformation<?> ti = TypeExtractor.getFlatMapReturnTypes(function, null);
		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(3, ti.getArity());
		Assert.assertTrue(ti instanceof TupleTypeInfo);

		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;

		Assert.assertTrue(tti.getTypeAt(0).isTupleType());
		Assert.assertTrue(tti.getTypeAt(1).isTupleType());
		Assert.assertTrue(tti.getTypeAt(2).isTupleType());

		Assert.assertEquals(1, tti.getTypeAt(0).getArity());
		Assert.assertEquals(1, tti.getTypeAt(1).getArity());
		Assert.assertEquals(2, tti.getTypeAt(2).getArity());

		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ((TupleTypeInfo<?>) tti.getTypeAt(0)).getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ((TupleTypeInfo<?>) tti.getTypeAt(1)).getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, ((TupleTypeInfo<?>) tti.getTypeAt(2)).getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, ((TupleTypeInfo<?>) tti.getTypeAt(2)).getTypeAt(1));

		// use getForObject()
		Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>> t = new Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>>(
				new Tuple1<String>("hello"), new Tuple1<Integer>(1), new Tuple2<Long, Long>(2L, 3L));
		Assert.assertTrue(TypeExtractor.getForObject(t) instanceof TupleTypeInfo);
		TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>) TypeExtractor.getForObject(t);

		Assert.assertEquals(1, tti2.getTypeAt(0).getArity());
		Assert.assertEquals(1, tti2.getTypeAt(1).getArity());
		Assert.assertEquals(2, tti2.getTypeAt(2).getArity());

		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ((TupleTypeInfo<?>) tti2.getTypeAt(0)).getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ((TupleTypeInfo<?>) tti2.getTypeAt(1)).getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, ((TupleTypeInfo<?>) tti2.getTypeAt(2)).getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, ((TupleTypeInfo<?>) tti2.getTypeAt(2)).getTypeAt(1));
	}

	@Test
	public void testSubclassOfTuple() {
		// use getJoinReturnTypes()
		JoinFunction<?, ?, ?> function = new JoinFunction<String, String, CustomTuple>() {
			private static final long serialVersionUID = 1L;

			@Override
			public CustomTuple join(String first, String second) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getJoinReturnTypes(function, null, null);

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ((TupleTypeInfo<?>) ti).getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ((TupleTypeInfo<?>) ti).getTypeAt(1));

		// use getForObject()
		CustomTuple t = new CustomTuple("hello", 1);
		TypeInformation<?> ti2 = TypeExtractor.getForObject(t);

		Assert.assertTrue(ti2.isTupleType());
		Assert.assertEquals(2, ti2.getArity());
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ((TupleTypeInfo<?>) ti2).getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ((TupleTypeInfo<?>) ti2).getTypeAt(1));
	}

	public static class CustomTuple extends Tuple2<String, Integer> {
		private static final long serialVersionUID = 1L;

		public CustomTuple(String myField1, Integer myField2) {
			this.setFields(myField1, myField2);
		}

		public String getMyField1() {
			return this.f0;
		}

		public int getMyField2() {
			return this.f1;
		}
	}

	@Test
	public void testCustomType() {
		// use getCrossReturnTypes()
		CrossFunction<?, ?, ?> function = new CrossFunction<Integer, Integer, CustomType>() {
			private static final long serialVersionUID = 1L;

			@Override
			public CustomType cross(Integer first, Integer second) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getCrossReturnTypes(function, null, null);

		Assert.assertFalse(ti.isBasicType());
		Assert.assertFalse(ti.isTupleType());
		Assert.assertTrue(ti instanceof GenericTypeInfo);
		Assert.assertEquals(ti.getTypeClass(), CustomType.class);

		// use getForClass()
		Assert.assertTrue(TypeExtractor.getForClass(CustomType.class) instanceof GenericTypeInfo);
		Assert.assertEquals(TypeExtractor.getForClass(CustomType.class).getTypeClass(), ti.getTypeClass());

		// use getForObject()
		CustomType t = new CustomType("World", 1);
		TypeInformation<?> ti2 = TypeExtractor.getForObject(t);

		Assert.assertFalse(ti2.isBasicType());
		Assert.assertFalse(ti2.isTupleType());
		Assert.assertTrue(ti2 instanceof GenericTypeInfo);
		Assert.assertEquals(ti2.getTypeClass(), CustomType.class);
	}

	public static class CustomType {
		public String myField1;
		public int myField2;

		public CustomType() {
		}

		public CustomType(String myField1, int myField2) {
			this.myField1 = myField1;
			this.myField2 = myField2;
		}
	}

	@Test
	public void testTupleWithCustomType() {
		// use getMapReturnTypes()
		MapFunction<?, ?> function = new MapFunction<String, Tuple2<Long, CustomType>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, CustomType> map(String value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(Long.class, tti.getTypeAt(0).getTypeClass());
		Assert.assertTrue(tti.getTypeAt(1) instanceof GenericTypeInfo);
		Assert.assertEquals(CustomType.class, tti.getTypeAt(1).getTypeClass());

		// use getForObject()
		Tuple2<?, ?> t = new Tuple2<Long, CustomType>(1L, new CustomType("Hello", 1));
		TypeInformation<?> ti2 = TypeExtractor.getForObject(t);

		Assert.assertTrue(ti2.isTupleType());
		Assert.assertEquals(2, ti2.getArity());
		TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>) ti2;
		Assert.assertEquals(Long.class, tti2.getTypeAt(0).getTypeClass());
		Assert.assertTrue(tti2.getTypeAt(1) instanceof GenericTypeInfo);
		Assert.assertEquals(CustomType.class, tti2.getTypeAt(1).getTypeClass());
	}

	@Test
	public void testValue() {
		// use getKeyExtractorType()
		KeySelector<?, ?> function = new KeySelector<String, StringValue>() {
			private static final long serialVersionUID = 1L;

			@Override
			public StringValue getKey(String value) {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getKeyExtractorType(function, null);

		Assert.assertFalse(ti.isBasicType());
		Assert.assertFalse(ti.isTupleType());
		Assert.assertTrue(ti instanceof ValueTypeInfo);
		Assert.assertEquals(ti.getTypeClass(), StringValue.class);

		// use getForClass()
		Assert.assertTrue(TypeExtractor.getForClass(StringValue.class) instanceof ValueTypeInfo);
		Assert.assertEquals(TypeExtractor.getForClass(StringValue.class).getTypeClass(), ti.getTypeClass());

		// use getForObject()
		StringValue v = new StringValue("Hello");
		Assert.assertTrue(TypeExtractor.getForObject(v) instanceof ValueTypeInfo);
		Assert.assertEquals(TypeExtractor.getForObject(v).getTypeClass(), ti.getTypeClass());
	}

	@Test
	public void testTupleOfValues() {
		// use getMapReturnTypes()
		MapFunction<?, ?> function = new MapFunction<String, Tuple2<StringValue, IntValue>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<StringValue, IntValue> map(String value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);

		Assert.assertFalse(ti.isBasicType());
		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(StringValue.class, ((TupleTypeInfo<?>) ti).getTypeAt(0).getTypeClass());
		Assert.assertEquals(IntValue.class, ((TupleTypeInfo<?>) ti).getTypeAt(1).getTypeClass());

		// use getForObject()
		Tuple2<StringValue, IntValue> t = new Tuple2<StringValue, IntValue>(new StringValue("x"), new IntValue(1));
		TypeInformation<?> ti2 = TypeExtractor.getForObject(t);

		Assert.assertFalse(ti2.isBasicType());
		Assert.assertTrue(ti2.isTupleType());
		Assert.assertEquals(((TupleTypeInfo<?>) ti2).getTypeAt(0).getTypeClass(), StringValue.class);
		Assert.assertEquals(((TupleTypeInfo<?>) ti2).getTypeAt(1).getTypeClass(), IntValue.class);
	}

	public static class LongKeyValue<V> extends Tuple2<Long, V> {
		private static final long serialVersionUID = 1L;

		public LongKeyValue(Long field1, V field2) {
			this.f0 = field1;
			this.f1 = field2;
		}
	}

	@Test
	public void testGenericsNotInSuperclass() {
		// use getMapReturnTypes()
		MapFunction<?, ?> function = new MapFunction<String, LongKeyValue<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public LongKeyValue<String> map(String value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
	}

	public static class ChainedOne<X, Y> extends Tuple3<X, Long, Y> {
		private static final long serialVersionUID = 1L;

		public ChainedOne(X field0, Long field1, Y field2) {
			this.f0 = field0;
			this.f1 = field1;
			this.f2 = field2;
		}
	}

	public static class ChainedTwo<V> extends ChainedOne<String, V> {
		private static final long serialVersionUID = 1L;

		public ChainedTwo(String field0, Long field1, V field2) {
			super(field0, field1, field2);
		}
	}

	@Test
	public void testChainedGenericsNotInSuperclass() {
		// use TypeExtractor
		MapFunction<?, ?> function = new MapFunction<String, ChainedTwo<Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public ChainedTwo<Integer> map(String value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(3, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(1));
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti.getTypeAt(2));
	}

	public static class ChainedThree extends ChainedTwo<String> {
		private static final long serialVersionUID = 1L;

		public ChainedThree(String field0, Long field1, String field2) {
			super(field0, field1, field2);
		}
	}

	public static class ChainedFour extends ChainedThree {
		private static final long serialVersionUID = 1L;

		public ChainedFour(String field0, Long field1, String field2) {
			super(field0, field1, field2);
		}
	}

	@Test
	public void testGenericsNotInSuperclassWithNonGenericClassAtEnd() {
		// use TypeExtractor
		MapFunction<?, ?> function = new MapFunction<String, ChainedFour>() {
			private static final long serialVersionUID = 1L;

			@Override
			public ChainedFour map(String value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(3, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(1));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(2));
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testMissingTupleGenericsException() {
		MapFunction<?, ?> function = new MapFunction<String, Tuple2>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2 map(String value) throws Exception {
				return null;
			}
		};

		boolean exception = false;
		try {
			TypeExtractor.getMapReturnTypes(function, null);
		} catch (InvalidTypesException e) {
			exception = true;
		}
		Assert.assertTrue(exception);
	}

	@Test
	public void testTupleSupertype() {
		MapFunction<?, ?> function = new MapFunction<String, Tuple>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple map(String value) throws Exception {
				return null;
			}
		};

		boolean exception = false;
		try {
			TypeExtractor.getMapReturnTypes(function, null);
		} catch (InvalidTypesException e) {
			exception = true;
		}
		Assert.assertTrue(exception);
	}

	public static class SameTypeVariable<X> extends Tuple2<X, X> {
		private static final long serialVersionUID = 1L;

		public SameTypeVariable(X field0, X field1) {
			super(field0, field1);
		}
	}

	@Test
	public void testSameGenericVariable() {
		MapFunction<?, ?> function = new MapFunction<String, SameTypeVariable<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public SameTypeVariable<String> map(String value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
	}

	public static class Nested<V, T> extends Tuple2<V, Tuple2<T, T>> {
		private static final long serialVersionUID = 1L;

		public Nested(V field0, Tuple2<T, T> field1) {
			super(field0, field1);
		}
	}

	@Test
	public void testNestedTupleGenerics() {
		MapFunction<?, ?> function = new MapFunction<String, Nested<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Nested<String, Integer> map(String value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertTrue(tti.getTypeAt(1).isTupleType());
		Assert.assertEquals(2, tti.getTypeAt(1).getArity());

		// Nested
		TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>) tti.getTypeAt(1);
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti2.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti2.getTypeAt(1));
	}

	public static class Nested2<T> extends Nested<T, Nested<Integer, T>> {
		private static final long serialVersionUID = 1L;

		public Nested2(T field0, Tuple2<Nested<Integer, T>, Nested<Integer, T>> field1) {
			super(field0, field1);
		}
	}

	@Test
	public void testNestedTupleGenerics2() {
		MapFunction<?, ?> function = new MapFunction<String, Nested2<Boolean>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Nested2<Boolean> map(String value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);

		// Should be 
		// Tuple2<Boolean, Tuple2<Tuple2<Integer, Tuple2<Boolean, Boolean>>, Tuple2<Integer, Tuple2<Boolean, Boolean>>>>

		// 1st nested level
		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertTrue(tti.getTypeAt(1).isTupleType());

		// 2nd nested level
		TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>) tti.getTypeAt(1);
		Assert.assertTrue(tti2.getTypeAt(0).isTupleType());
		Assert.assertTrue(tti2.getTypeAt(1).isTupleType());

		// 3rd nested level
		TupleTypeInfo<?> tti3 = (TupleTypeInfo<?>) tti2.getTypeAt(0);
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti3.getTypeAt(0));
		Assert.assertTrue(tti3.getTypeAt(1).isTupleType());

		// 4th nested level
		TupleTypeInfo<?> tti4 = (TupleTypeInfo<?>) tti3.getTypeAt(1);
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti4.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti4.getTypeAt(1));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFunctionWithMissingGenerics() {
		MapFunction function = new MapFunction() {
			private static final long serialVersionUID = 1L;

			@Override
			public String map(Object value) throws Exception {
				return null;
			}
		};

		boolean exception = false;
		try {
			TypeExtractor.getMapReturnTypes(function, null);
		} catch (Exception e) {
			exception = true;
		}
		Assert.assertTrue(exception);
	}

	@Test
	public void testFunctionDependingOnInputAsSuperclass() {
		IdentityMapper<Boolean> function = new IdentityMapper<Boolean>() {
			private static final long serialVersionUID = 1L;
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);

		Assert.assertTrue(ti.isBasicType());
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti);
	}

	public class IdentityMapper<T> extends MapFunction<T, T> {
		private static final long serialVersionUID = 1L;

		@Override
		public T map(T value) throws Exception {
			return null;
		}
	}

	@Test
	public void testFunctionDependingOnInputFromInput() {
		IdentityMapper<Boolean> function = new IdentityMapper<Boolean>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, BasicTypeInfo.BOOLEAN_TYPE_INFO);

		Assert.assertTrue(ti.isBasicType());
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti);
	}

	public class IdentityMapper2<T> extends MapFunction<Tuple2<T, String>, T> {
		private static final long serialVersionUID = 1L;

		@Override
		public T map(Tuple2<T, String> value) throws Exception {
			return null;
		}
	}

	@Test
	public void testFunctionDependingOnInputWithTupleInput() {
		IdentityMapper2<Boolean> function = new IdentityMapper2<Boolean>();

		TypeInformation<Tuple2<Boolean, String>> inputType = new TupleTypeInfo<Tuple2<Boolean, String>>(BasicTypeInfo.BOOLEAN_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, inputType);

		Assert.assertTrue(ti.isBasicType());
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti);
	}

	@Test
	public void testFunctionDependingOnInputWithCustomTupleInput() {
		IdentityMapper<SameTypeVariable<String>> function = new IdentityMapper<SameTypeVariable<String>>();

		// input (without type erasure)
		IdentityMapper<SameTypeVariable<String>> input = new IdentityMapper<SameTypeVariable<String>>() {
			private static final long serialVersionUID = 1L;
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, TypeExtractor.getMapReturnTypes(input, null));

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
	}

	public class IdentityMapper3<T, V> extends MapFunction<T, V> {
		private static final long serialVersionUID = 1L;

		@Override
		public V map(T value) throws Exception {
			return null;
		}
	}

	@Test
	public void testFunctionDependingOnInputException() {
		IdentityMapper3<Boolean, String> function = new IdentityMapper3<Boolean, String>();

		boolean exception = false;
		try {
			TypeExtractor.getMapReturnTypes(function, BasicTypeInfo.BOOLEAN_TYPE_INFO);
		} catch (InvalidTypesException e) {
			exception = true;
		}
		Assert.assertTrue(exception);
	}

	public class IdentityMapper4<D> extends IdentityMapper<D> {
		private static final long serialVersionUID = 1L;
	}

	@Test
	public void testFunctionDependingOnInputWithFunctionHierarchy() {
		IdentityMapper4<String> function = new IdentityMapper4<String>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, BasicTypeInfo.STRING_TYPE_INFO);

		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ti);
	}

	public class IdentityMapper5<D> extends IdentityMapper<Tuple2<D, D>> {
		private static final long serialVersionUID = 1L;
	}

	@Test
	public void testFunctionDependingOnInputWithFunctionHierarchy2() {
		IdentityMapper5<String> function = new IdentityMapper5<String>();

		@SuppressWarnings({ "rawtypes", "unchecked" })
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, new TupleTypeInfo(BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO));

		Assert.assertTrue(ti.isTupleType());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
	}

	public class Mapper extends IdentityMapper<String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String map(String value) throws Exception {
			return null;
		}
	}

	public class Mapper2 extends Mapper {
		private static final long serialVersionUID = 1L;

		@Override
		public String map(String value) throws Exception {
			return null;
		}
	}

	@Test
	public void testFunctionWithNoGenericSuperclass() {
		MapFunction<?, ?> function = new Mapper2();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);

		Assert.assertTrue(ti.isBasicType());
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ti);
	}

	public class OneAppender<T> extends MapFunction<T, Tuple2<T, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple2<T, Integer> map(T value) {
			return new Tuple2<T, Integer>(value, 1);
		}
	}

	@Test
	public void testFunctionDependingPartialOnInput() {
		MapFunction<?, ?> function = new OneAppender<DoubleValue>() {
			private static final long serialVersionUID = 1L;
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;

		Assert.assertTrue(tti.getTypeAt(0) instanceof ValueTypeInfo<?>);
		ValueTypeInfo<?> vti = (ValueTypeInfo<?>) tti.getTypeAt(0);
		Assert.assertEquals(DoubleValue.class, vti.getTypeClass());
	}

	@Test
	public void testFunctionDependingPartialOnInput2() {
		MapFunction<DoubleValue, ?> function = new OneAppender<DoubleValue>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, new ValueTypeInfo<DoubleValue>(DoubleValue.class));

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;

		Assert.assertTrue(tti.getTypeAt(0) instanceof ValueTypeInfo<?>);
		ValueTypeInfo<?> vti = (ValueTypeInfo<?>) tti.getTypeAt(0);
		Assert.assertEquals(DoubleValue.class, vti.getTypeClass());
	}

	public class FieldDuplicator<T> extends MapFunction<T, Tuple2<T, T>> {
		private static final long serialVersionUID = 1L;

		public Tuple2<T, T> map(T value) {
			return new Tuple2<T, T>(value, value);
		}
	}

	@Test
	public void testFunctionInputInOutputMultipleTimes() {
		MapFunction<Float, ?> function = new FieldDuplicator<Float>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, BasicTypeInfo.FLOAT_TYPE_INFO);

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti.getTypeAt(1));
	}

	@Test
	public void testFunctionInputInOutputMultipleTimes2() {
		MapFunction<Tuple2<Float, Float>, ?> function = new FieldDuplicator<Tuple2<Float, Float>>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, new TupleTypeInfo<Tuple2<Float, Float>>(
				BasicTypeInfo.FLOAT_TYPE_INFO, BasicTypeInfo.FLOAT_TYPE_INFO));

		// should be
		// Tuple2<Tuple2<Float, Float>, Tuple2<Float, Float>>

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;

		// 2nd nested level	
		Assert.assertTrue(tti.getTypeAt(0).isTupleType());
		TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>) tti.getTypeAt(0);
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti2.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti2.getTypeAt(1));
		Assert.assertTrue(tti.getTypeAt(0).isTupleType());
		TupleTypeInfo<?> tti3 = (TupleTypeInfo<?>) tti.getTypeAt(1);
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti3.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti3.getTypeAt(1));
	}

	public interface Testable {

	}

	public abstract class AbstractClass {

	}

	@Test
	public void testAbstractAndInterfaceTypesException() {
		MapFunction<?, ?> function = new MapFunction<String, Testable>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Testable map(String value) throws Exception {
				return null;
			}
		};

		boolean exception = false;
		try {
			TypeExtractor.getMapReturnTypes(function, null);
		} catch (InvalidTypesException e) {
			exception = true;
		}
		Assert.assertTrue(exception);

		MapFunction<?, ?> function2 = new MapFunction<String, AbstractClass>() {
			private static final long serialVersionUID = 1L;

			@Override
			public AbstractClass map(String value) throws Exception {
				return null;
			}
		};

		boolean exception2 = false;
		try {
			TypeExtractor.getMapReturnTypes(function2, null);
		} catch (InvalidTypesException e) {
			exception2 = true;
		}
		Assert.assertTrue(exception2);
	}

	@Test
	public void testValueSupertypeException() {
		MapFunction<?, ?> function = new MapFunction<Value, Value>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Value map(Value value) throws Exception {
				return null;
			}
		};

		boolean exception = false;
		try {
			TypeExtractor.getMapReturnTypes(function, null);
		} catch (InvalidTypesException e) {
			exception = true;
		}
		Assert.assertTrue(exception);
	}

	@Test
	public void testBasicArray() {
		// use getCoGroupReturnTypes()
		CoGroupFunction<?, ?, ?> function = new CoGroupFunction<String, String, String[]>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void combineFirst(Iterator<String> records, Collector<String> out) throws Exception {
				// nothing to do
			}

			@Override
			public void combineSecond(Iterator<String> records, Collector<String> out) throws Exception {
				// nothing to do
			}

			@Override
			public void coGroup(Iterator<String> first, Iterator<String> second, Collector<String[]> out) throws Exception {
				// nothing to do
			}
		};

		TypeInformation<?> ti = TypeExtractor.getCoGroupReturnTypes(function, null, null);

		Assert.assertFalse(ti.isBasicType());
		Assert.assertFalse(ti.isTupleType());
		
		// Due to a Java 6 bug the classification can be slightly wrong
		Assert.assertTrue(ti instanceof BasicArrayTypeInfo<?,?> || ti instanceof ObjectArrayTypeInfo<?,?>);
		
		if(ti instanceof BasicArrayTypeInfo<?,?>) {
			Assert.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, ti);
		}
		else {
			Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ((ObjectArrayTypeInfo<?,?>) ti).getComponentInfo());
		}		
	}

	@Test
	public void testBasicArray2() {
		MapFunction<Boolean[], ?> function = new IdentityMapper<Boolean[]>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO);

		Assert.assertTrue(ti instanceof BasicArrayTypeInfo<?, ?>);
		BasicArrayTypeInfo<?, ?> bati = (BasicArrayTypeInfo<?, ?>) ti;
		Assert.assertTrue(bati.getComponentInfo().isBasicType());
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, bati.getComponentInfo());
	}

	public class CustomArrayObject {

	}

	@Test
	public void testCustomArray() {
		MapFunction<?, ?> function = new MapFunction<String, CustomArrayObject[]>() {
			private static final long serialVersionUID = 1L;

			@Override
			public CustomArrayObject[] map(String value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);

		Assert.assertTrue(ti instanceof ObjectArrayTypeInfo<?, ?>);
		Assert.assertEquals(CustomArrayObject.class, ((ObjectArrayTypeInfo<?, ?>) ti).getComponentType());
	}

	@Test
	public void testTupleArray() {
		MapFunction<?, ?> function = new MapFunction<String, Tuple2<String, String>[]>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String>[] map(String value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);

		Assert.assertTrue(ti instanceof ObjectArrayTypeInfo<?, ?>);
		ObjectArrayTypeInfo<?, ?> oati = (ObjectArrayTypeInfo<?, ?>) ti;
		Assert.assertTrue(oati.getComponentInfo().isTupleType());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) oati.getComponentInfo();
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
	}

	public class CustomArrayObject2<F> extends Tuple1<F> {
		private static final long serialVersionUID = 1L;

	}

	@Test
	public void testCustomArrayWithTypeVariable() {
		MapFunction<CustomArrayObject2<Boolean>[], ?> function = new IdentityMapper<CustomArrayObject2<Boolean>[]>();

		// input (without type erasure)
		IdentityMapper<CustomArrayObject2<Boolean>[]> input = new IdentityMapper<CustomArrayObject2<Boolean>[]>() {
			private static final long serialVersionUID = 1L;
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, TypeExtractor.getMapReturnTypes(input, null));

		Assert.assertTrue(ti instanceof ObjectArrayTypeInfo<?, ?>);
		ObjectArrayTypeInfo<?, ?> oati = (ObjectArrayTypeInfo<?, ?>) ti;
		Assert.assertTrue(oati.getComponentInfo().isTupleType());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) oati.getComponentInfo();
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti.getTypeAt(0));
	}
	
	public class GenericArrayClass<T> extends MapFunction<String, T[]> {
		private static final long serialVersionUID = 1L;

		@Override
		public T[] map(String value) throws Exception {
			return null;
		}		
	}
	
	@Test
	public void testParameterizedArrays() {
		GenericArrayClass<Boolean> function = new GenericArrayClass<Boolean>(){
			private static final long serialVersionUID = 1L;			
		};
		
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);
		Assert.assertTrue(ti instanceof ObjectArrayTypeInfo<?,?>);
		ObjectArrayTypeInfo<?, ?> oati = (ObjectArrayTypeInfo<?, ?>) ti;
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, oati.getComponentInfo());		
	}
	
	public class MyObject<T> {
		public T myField;
	}
	
	@Test
	public void testParamertizedCustomObject() {
		MapFunction<?, ?> function = new MapFunction<Boolean, MyObject<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MyObject<String> map(Boolean value) throws Exception {
				return null;
			}
		};
		
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, null);
		Assert.assertTrue(ti instanceof GenericTypeInfo<?>);
	}

}
