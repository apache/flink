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

package org.apache.flink.api.java.type.extractor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichCrossFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.api.java.typeutils.WritableTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

public class TypeExtractorTest {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testBasicType() {
		// use getGroupReduceReturnTypes()
		RichGroupReduceFunction<?, ?> function = new RichGroupReduceFunction<Boolean, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void reduce(Iterable<Boolean> values, Collector<Boolean> out) throws Exception {
				// nothing to do
			}
		};

		TypeInformation<?> ti = TypeExtractor.getGroupReduceReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Boolean"));

		Assert.assertTrue(ti.isBasicType());
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti);
		Assert.assertEquals(Boolean.class, ti.getTypeClass());

		// use getForClass()
		Assert.assertTrue(TypeExtractor.getForClass(Boolean.class).isBasicType());
		Assert.assertEquals(ti, TypeExtractor.getForClass(Boolean.class));

		// use getForObject()
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, TypeExtractor.getForObject(Boolean.valueOf(true)));
	}
	
	public static class MyWritable implements Writable {
		
		@Override
		public void write(DataOutput out) throws IOException {
			
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
		}
		
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWritableType() {
		RichMapFunction<?, ?> function = new RichMapFunction<MyWritable, MyWritable>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public MyWritable map(MyWritable value) throws Exception {
				return null;
			}
			
		};
		
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) new WritableTypeInfo<MyWritable>(MyWritable.class));
		
		Assert.assertTrue(ti instanceof WritableTypeInfo<?>);
		Assert.assertEquals(MyWritable.class, ((WritableTypeInfo<?>) ti).getTypeClass());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testTupleWithBasicTypes() throws Exception {
		// use getMapReturnTypes()
		RichMapFunction<?, ?> function = new RichMapFunction<Tuple9<Integer, Long, Double, Float, Boolean, String, Character, Short, Byte>, Tuple9<Integer, Long, Double, Float, Boolean, String, Character, Short, Byte>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple9<Integer, Long, Double, Float, Boolean, String, Character, Short, Byte> map(
					Tuple9<Integer, Long, Double, Float, Boolean, String, Character, Short, Byte> value) throws Exception {
				return null;
			}

		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple9<Integer, Long, Double, Float, Boolean, String, Character, Short, Byte>"));

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(9, ti.getArity());
		Assert.assertTrue(ti instanceof TupleTypeInfo);
		

		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(Tuple9.class, tti.getTypeClass());
		
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
		
		// test that getForClass does not work
		try {
			TypeExtractor.getForClass(Tuple9.class);
			Assert.fail("Exception expected here");
		} catch (InvalidTypesException e) {
			// that is correct
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testTupleWithTuples() {
		// use getFlatMapReturnTypes()
		RichFlatMapFunction<?, ?> function = new RichFlatMapFunction<Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>>, Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>> value,
					Collector<Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>>> out) throws Exception {
				// nothing to do
			}
		};

		TypeInformation<?> ti = TypeExtractor.getFlatMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>>"));
		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(3, ti.getArity());
		Assert.assertTrue(ti instanceof TupleTypeInfo);

		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(Tuple3.class, tti.getTypeClass());

		Assert.assertTrue(tti.getTypeAt(0).isTupleType());
		Assert.assertTrue(tti.getTypeAt(1).isTupleType());
		Assert.assertTrue(tti.getTypeAt(2).isTupleType());
		
		Assert.assertEquals(Tuple1.class, tti.getTypeAt(0).getTypeClass());
		Assert.assertEquals(Tuple1.class, tti.getTypeAt(1).getTypeClass());
		Assert.assertEquals(Tuple2.class, tti.getTypeAt(2).getTypeClass());

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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testSubclassOfTuple() {
		// use getJoinReturnTypes()
		RichFlatJoinFunction<?, ?, ?> function = new RichFlatJoinFunction<CustomTuple, String, CustomTuple>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void join(CustomTuple first, String second, Collector<CustomTuple> out) throws Exception {
				out.collect(null);
			}			
		};

		TypeInformation<?> ti = TypeExtractor.getFlatJoinReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple2<String, Integer>"), (TypeInformation) TypeInfoParser.parse("String"));

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ((TupleTypeInfo<?>) ti).getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ((TupleTypeInfo<?>) ti).getTypeAt(1));
		Assert.assertEquals(CustomTuple.class, ((TupleTypeInfo<?>) ti).getTypeClass());

		// use getForObject()
		CustomTuple t = new CustomTuple("hello", 1);
		TypeInformation<?> ti2 = TypeExtractor.getForObject(t);

		Assert.assertTrue(ti2.isTupleType());
		Assert.assertEquals(2, ti2.getArity());
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ((TupleTypeInfo<?>) ti2).getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ((TupleTypeInfo<?>) ti2).getTypeAt(1));
		Assert.assertEquals(CustomTuple.class, ((TupleTypeInfo<?>) ti2).getTypeClass());
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCustomType() {
		// use getCrossReturnTypes()
		RichCrossFunction<?, ?, ?> function = new RichCrossFunction<CustomType, Integer, CustomType>() {
			private static final long serialVersionUID = 1L;

			@Override
			public CustomType cross(CustomType first, Integer second) throws Exception {
				return null;
			}			
		};

		TypeInformation<?> ti = TypeExtractor.getCrossReturnTypes(function, (TypeInformation) TypeInfoParser.parse("org.apache.flink.api.java.type.extractor.TypeExtractorTest$CustomType"), (TypeInformation) TypeInfoParser.parse("Integer"));

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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testTupleWithCustomType() {
		// use getMapReturnTypes()
		RichMapFunction<?, ?> function = new RichMapFunction<Tuple2<Long, CustomType>, Tuple2<Long, CustomType>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, CustomType> map(Tuple2<Long, CustomType> value) throws Exception {
				return null;
			}

		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple2<Long,org.apache.flink.api.java.type.extractor.TypeExtractorTest$CustomType>"));

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(Tuple2.class, tti.getTypeClass());
		
		Assert.assertEquals(Long.class, tti.getTypeAt(0).getTypeClass());
		Assert.assertTrue(tti.getTypeAt(1) instanceof GenericTypeInfo);
		Assert.assertEquals(CustomType.class, tti.getTypeAt(1).getTypeClass());

		// use getForObject()
		Tuple2<?, ?> t = new Tuple2<Long, CustomType>(1L, new CustomType("Hello", 1));
		TypeInformation<?> ti2 = TypeExtractor.getForObject(t);

		Assert.assertTrue(ti2.isTupleType());
		Assert.assertEquals(2, ti2.getArity());
		TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>) ti2;
		
		Assert.assertEquals(Tuple2.class, tti2.getTypeClass());
		Assert.assertEquals(Long.class, tti2.getTypeAt(0).getTypeClass());
		Assert.assertTrue(tti2.getTypeAt(1) instanceof GenericTypeInfo);
		Assert.assertEquals(CustomType.class, tti2.getTypeAt(1).getTypeClass());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testValue() {
		// use getKeyExtractorType()
		KeySelector<?, ?> function = new KeySelector<StringValue, StringValue>() {
			private static final long serialVersionUID = 1L;

			@Override
			public StringValue getKey(StringValue value) {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getKeySelectorTypes(function, (TypeInformation) TypeInfoParser.parse("StringValue"));

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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testTupleOfValues() {
		// use getMapReturnTypes()
		RichMapFunction<?, ?> function = new RichMapFunction<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<StringValue, IntValue> map(Tuple2<StringValue, IntValue> value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple2<StringValue, IntValue>"));

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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testGenericsNotInSuperclass() {
		// use getMapReturnTypes()
		RichMapFunction<?, ?> function = new RichMapFunction<LongKeyValue<String>, LongKeyValue<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public LongKeyValue<String> map(LongKeyValue<String> value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple2<Long, String>"));

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(LongKeyValue.class, tti.getTypeClass());
		
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testChainedGenericsNotInSuperclass() {
		// use TypeExtractor
		RichMapFunction<?, ?> function = new RichMapFunction<ChainedTwo<Integer>, ChainedTwo<Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public ChainedTwo<Integer> map(ChainedTwo<Integer> value) throws Exception {
				return null;
			}			
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple3<String, Long, Integer>"));

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(3, ti.getArity());
		
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(ChainedTwo.class, tti.getTypeClass());
		
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testGenericsInDirectSuperclass() {
		// use TypeExtractor
		RichMapFunction<?, ?> function = new RichMapFunction<ChainedThree, ChainedThree>() {
			private static final long serialVersionUID = 1L;

			@Override
			public ChainedThree map(ChainedThree value) throws Exception {
				return null;
			}			
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple3<String, Long, String>"));

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(3, ti.getArity());
		
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(ChainedThree.class, tti.getTypeClass());
		
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(1));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(2));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testGenericsNotInSuperclassWithNonGenericClassAtEnd() {
		// use TypeExtractor
		RichMapFunction<?, ?> function = new RichMapFunction<ChainedFour, ChainedFour>() {
			private static final long serialVersionUID = 1L;

			@Override
			public ChainedFour map(ChainedFour value) throws Exception {
				return null;
			}			
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple3<String, Long, String>"));

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(3, ti.getArity());
		
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(ChainedFour.class, tti.getTypeClass());
		
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(1));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(2));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMissingTupleGenericsException() {
		RichMapFunction<?, ?> function = new RichMapFunction<String, Tuple2>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2 map(String value) throws Exception {
				return null;
			}
		};

		try {
			TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("String"));
			Assert.fail("exception expected");
		} catch (InvalidTypesException e) {
			// right
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testTupleSupertype() {
		RichMapFunction<?, ?> function = new RichMapFunction<String, Tuple>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple map(String value) throws Exception {
				return null;
			}
		};

		try {
			TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("String"));
			Assert.fail("exception expected");
		} catch (InvalidTypesException e) {
			// right
		}
	}

	public static class SameTypeVariable<X> extends Tuple2<X, X> {
		private static final long serialVersionUID = 1L;

		public SameTypeVariable(X field0, X field1) {
			super(field0, field1);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testSameGenericVariable() {
		RichMapFunction<?, ?> function = new RichMapFunction<SameTypeVariable<String>, SameTypeVariable<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public SameTypeVariable<String> map(SameTypeVariable<String> value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple2<String, String>"));

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(SameTypeVariable.class, tti.getTypeClass());
		
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
	}

	public static class Nested<V, T> extends Tuple2<V, Tuple2<T, T>> {
		private static final long serialVersionUID = 1L;

		public Nested(V field0, Tuple2<T, T> field1) {
			super(field0, field1);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testNestedTupleGenerics() {
		RichMapFunction<?, ?> function = new RichMapFunction<Nested<String, Integer>, Nested<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Nested<String, Integer> map(Nested<String, Integer> value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple2<String, Tuple2<Integer, Integer>>"));

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(Nested.class, tti.getTypeClass());
		
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertTrue(tti.getTypeAt(1).isTupleType());
		Assert.assertEquals(2, tti.getTypeAt(1).getArity());

		// Nested
		TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>) tti.getTypeAt(1);
		Assert.assertEquals(Tuple2.class, tti2.getTypeClass());
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti2.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti2.getTypeAt(1));
	}

	public static class Nested2<T> extends Nested<T, Nested<Integer, T>> {
		private static final long serialVersionUID = 1L;

		public Nested2(T field0, Tuple2<Nested<Integer, T>, Nested<Integer, T>> field1) {
			super(field0, field1);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testNestedTupleGenerics2() {
		RichMapFunction<?, ?> function = new RichMapFunction<Nested2<Boolean>, Nested2<Boolean>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Nested2<Boolean> map(Nested2<Boolean> value) throws Exception {
				return null;
			}
		};
		
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple2<Boolean, Tuple2<Tuple2<Integer, Tuple2<Boolean, Boolean>>, Tuple2<Integer, Tuple2<Boolean, Boolean>>>>"));

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
		RichMapFunction function = new RichMapFunction() {
			private static final long serialVersionUID = 1L;

			@Override
			public String map(Object value) throws Exception {
				return null;
			}
		};

		try {
			TypeExtractor.getMapReturnTypes(function, TypeInfoParser.parse("String"));
			Assert.fail("exception expected");
		} catch (InvalidTypesException e) {
			// right
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testFunctionDependingOnInputAsSuperclass() {
		IdentityMapper<Boolean> function = new IdentityMapper<Boolean>() {
			private static final long serialVersionUID = 1L;
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Boolean"));

		Assert.assertTrue(ti.isBasicType());
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti);
	}

	public class IdentityMapper<T> extends RichMapFunction<T, T> {
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
	
	@Test
	public void testFunctionDependingOnInputWithMissingInput() {
		IdentityMapper<Boolean> function = new IdentityMapper<Boolean>();

		try {
			TypeExtractor.getMapReturnTypes(function, null);
			Assert.fail("exception expected");
		} catch (InvalidTypesException e) {
			// right
		}
	}

	public class IdentityMapper2<T> extends RichMapFunction<Tuple2<T, String>, T> {
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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testFunctionDependingOnInputWithCustomTupleInput() {
		IdentityMapper<SameTypeVariable<String>> function = new IdentityMapper<SameTypeVariable<String>>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple2<String, String>"));

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1));
	}

	public class IdentityMapper3<T, V> extends RichMapFunction<T, V> {
		private static final long serialVersionUID = 1L;

		@Override
		public V map(T value) throws Exception {
			return null;
		}
	}

	@Test
	public void testFunctionDependingOnInputException() {
		IdentityMapper3<Boolean, String> function = new IdentityMapper3<Boolean, String>();

		try {
			TypeExtractor.getMapReturnTypes(function, BasicTypeInfo.BOOLEAN_TYPE_INFO);
			Assert.fail("exception expected");
		} catch (InvalidTypesException e) {
			// right
		}
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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testFunctionWithNoGenericSuperclass() {
		RichMapFunction<?, ?> function = new Mapper2();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("String"));

		Assert.assertTrue(ti.isBasicType());
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, ti);
	}

	public class OneAppender<T> extends RichMapFunction<T, Tuple2<T, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple2<T, Integer> map(T value) {
			return new Tuple2<T, Integer>(value, 1);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testFunctionDependingPartialOnInput() {
		RichMapFunction<?, ?> function = new OneAppender<DoubleValue>() {
			private static final long serialVersionUID = 1L;
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("DoubleValue"));

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;

		Assert.assertTrue(tti.getTypeAt(0) instanceof ValueTypeInfo<?>);
		ValueTypeInfo<?> vti = (ValueTypeInfo<?>) tti.getTypeAt(0);
		Assert.assertEquals(DoubleValue.class, vti.getTypeClass());
		
		Assert.assertTrue(tti.getTypeAt(1).isBasicType());
		Assert.assertEquals(Integer.class , tti.getTypeAt(1).getTypeClass());
	}

	@Test
	public void testFunctionDependingPartialOnInput2() {
		RichMapFunction<DoubleValue, ?> function = new OneAppender<DoubleValue>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, new ValueTypeInfo<DoubleValue>(DoubleValue.class));

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;

		Assert.assertTrue(tti.getTypeAt(0) instanceof ValueTypeInfo<?>);
		ValueTypeInfo<?> vti = (ValueTypeInfo<?>) tti.getTypeAt(0);
		Assert.assertEquals(DoubleValue.class, vti.getTypeClass());
		
		Assert.assertTrue(tti.getTypeAt(1).isBasicType());
		Assert.assertEquals(Integer.class , tti.getTypeAt(1).getTypeClass());
	}

	public class FieldDuplicator<T> extends RichMapFunction<T, Tuple2<T, T>> {
		private static final long serialVersionUID = 1L;

		public Tuple2<T, T> map(T value) {
			return new Tuple2<T, T>(value, value);
		}
	}

	@Test
	public void testFunctionInputInOutputMultipleTimes() {
		RichMapFunction<Float, ?> function = new FieldDuplicator<Float>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, BasicTypeInfo.FLOAT_TYPE_INFO);

		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti.getTypeAt(1));
	}

	@Test
	public void testFunctionInputInOutputMultipleTimes2() {
		RichMapFunction<Tuple2<Float, Float>, ?> function = new FieldDuplicator<Tuple2<Float, Float>>();

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

	public interface Testable {}

	public abstract class AbstractClass {}

	@Test
	public void testAbstractAndInterfaceTypesException() {
		RichMapFunction<String, ?> function = new RichMapFunction<String, Testable>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Testable map(String value) throws Exception {
				return null;
			}
		};
		
		try {
			TypeExtractor.getMapReturnTypes(function, BasicTypeInfo.STRING_TYPE_INFO);
			Assert.fail("exception expected");
		} catch (InvalidTypesException e) {
			// good
		}

		RichMapFunction<String, ?> function2 = new RichMapFunction<String, AbstractClass>() {
			private static final long serialVersionUID = 1L;

			@Override
			public AbstractClass map(String value) throws Exception {
				return null;
			}
		};

		try {
			TypeExtractor.getMapReturnTypes(function2, BasicTypeInfo.STRING_TYPE_INFO);
			Assert.fail("exception expected");
		} catch (InvalidTypesException e) {
			// slick!
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testValueSupertypeException() {
		RichMapFunction<?, ?> function = new RichMapFunction<StringValue, Value>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Value map(StringValue value) throws Exception {
				return null;
			}
		};

		try {
			TypeExtractor.getMapReturnTypes(function, (TypeInformation)TypeInfoParser.parse("StringValue"));
			Assert.fail("exception expected");
		} catch (InvalidTypesException e) {
			// bam! go type extractor!
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testBasicArray() {
		// use getCoGroupReturnTypes()
		RichCoGroupFunction<?, ?, ?> function = new RichCoGroupFunction<String[], String[], String[]>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void coGroup(Iterable<String[]> first, Iterable<String[]> second, Collector<String[]> out) throws Exception {
				// nothing to do
			}
		};

		TypeInformation<?> ti = TypeExtractor.getCoGroupReturnTypes(function, (TypeInformation) TypeInfoParser.parse("String[]"), (TypeInformation) TypeInfoParser.parse("String[]"));

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
		RichMapFunction<Boolean[], ?> function = new IdentityMapper<Boolean[]>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO);

		Assert.assertTrue(ti instanceof BasicArrayTypeInfo<?, ?>);
		BasicArrayTypeInfo<?, ?> bati = (BasicArrayTypeInfo<?, ?>) ti;
		Assert.assertTrue(bati.getComponentInfo().isBasicType());
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, bati.getComponentInfo());
	}

	public static class CustomArrayObject {}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testCustomArray() {
		RichMapFunction<?, ?> function = new RichMapFunction<CustomArrayObject[], CustomArrayObject[]>() {
			private static final long serialVersionUID = 1L;

			@Override
			public CustomArrayObject[] map(CustomArrayObject[] value) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("org.apache.flink.api.java.type.extractor.TypeExtractorTest$CustomArrayObject[]"));

		Assert.assertTrue(ti instanceof ObjectArrayTypeInfo<?, ?>);
		Assert.assertEquals(CustomArrayObject.class, ((ObjectArrayTypeInfo<?, ?>) ti).getComponentType());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testTupleArray() {
		RichMapFunction<?, ?> function = new RichMapFunction<Tuple2<String, String>[], Tuple2<String, String>[]>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String>[] map(Tuple2<String, String>[] value) throws Exception {
				return null;
			}
		};
		
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple2<String, String>[]"));

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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testCustomArrayWithTypeVariable() {
		RichMapFunction<CustomArrayObject2<Boolean>[], ?> function = new IdentityMapper<CustomArrayObject2<Boolean>[]>();

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple1<Boolean>[]"));

		Assert.assertTrue(ti instanceof ObjectArrayTypeInfo<?, ?>);
		ObjectArrayTypeInfo<?, ?> oati = (ObjectArrayTypeInfo<?, ?>) ti;
		Assert.assertTrue(oati.getComponentInfo().isTupleType());
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) oati.getComponentInfo();
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti.getTypeAt(0));
	}
	
	public class GenericArrayClass<T> extends RichMapFunction<T[], T[]> {
		private static final long serialVersionUID = 1L;

		@Override
		public T[] map(T[] value) throws Exception {
			return null;
		}		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testParameterizedArrays() {
		GenericArrayClass<Boolean> function = new GenericArrayClass<Boolean>(){
			private static final long serialVersionUID = 1L;			
		};
		
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Boolean[]"));
		Assert.assertTrue(ti instanceof ObjectArrayTypeInfo<?,?>);
		ObjectArrayTypeInfo<?, ?> oati = (ObjectArrayTypeInfo<?, ?>) ti;
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, oati.getComponentInfo());
	}
	
	public static class MyObject<T> {
		public T myField;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testParamertizedCustomObject() {
		RichMapFunction<?, ?> function = new RichMapFunction<MyObject<String>, MyObject<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public MyObject<String> map(MyObject<String> value) throws Exception {
				return null;
			}
		};
		
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("org.apache.flink.api.java.type.extractor.TypeExtractorTest$MyObject"));
		Assert.assertTrue(ti instanceof GenericTypeInfo);
	}
	
	@Test
	public void testFunctionDependingOnInputWithTupleInputWithTypeMismatch() {
		IdentityMapper2<Boolean> function = new IdentityMapper2<Boolean>();

		TypeInformation<Tuple2<Boolean, String>> inputType = new TupleTypeInfo<Tuple2<Boolean, String>>(BasicTypeInfo.BOOLEAN_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO);
		
		// input is: Tuple2<Boolean, Integer>
		// allowed: Tuple2<?, String>

		try {
			TypeExtractor.getMapReturnTypes(function, inputType);
			Assert.fail("exception expected");
		} catch (InvalidTypesException e) {
			// right
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testInputMismatchExceptions() {
		
		RichMapFunction<?, ?> function = new RichMapFunction<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String map(Tuple2<String, String> value) throws Exception {
				return null;
			}
		};
		
		try {
			TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple2<Integer, String>"));
			Assert.fail("exception expected");
		} catch (InvalidTypesException e) {
			// right
		}
		
		try {
			TypeExtractor.getMapReturnTypes(function, (TypeInformation) TypeInfoParser.parse("Tuple3<String, String, String>"));
			Assert.fail("exception expected");
		} catch (InvalidTypesException e) {
			// right
		}
		
		RichMapFunction<?, ?> function2 = new RichMapFunction<StringValue, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String map(StringValue value) throws Exception {
				return null;
			}
		};
		
		try {
			TypeExtractor.getMapReturnTypes(function2, (TypeInformation) TypeInfoParser.parse("IntValue"));
			Assert.fail("exception expected");
		} catch (InvalidTypesException e) {
			// right
		}
		
		RichMapFunction<?, ?> function3 = new RichMapFunction<Tuple1<Integer>[], String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String map(Tuple1<Integer>[] value) throws Exception {
				return null;
			}
		};
		
		try {
			TypeExtractor.getMapReturnTypes(function3, (TypeInformation) TypeInfoParser.parse("Integer[]"));
			Assert.fail("exception expected");
		} catch (InvalidTypesException e) {
			// right
		}
		
		RichMapFunction<?, ?> function4 = new RichMapFunction<Writable, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String map(Writable value) throws Exception {
				return null;
			}
		};
		
		try {
			TypeExtractor.getMapReturnTypes(function4, (TypeInformation) new WritableTypeInfo<MyWritable>(MyWritable.class));
			Assert.fail("exception expected");
		} catch (InvalidTypesException e) {
			// right
		}
	}
	
	public static class DummyFlatMapFunction<A,B,C,D> extends RichFlatMapFunction<Tuple2<A,B>, Tuple2<C,D>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple2<A, B> value, Collector<Tuple2<C, D>> out) throws Exception {
			
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testTypeErasureException() {
		try {
			TypeExtractor.getFlatMapReturnTypes(new DummyFlatMapFunction<String, Integer, String, Boolean>(), 
					(TypeInformation) TypeInfoParser.parse("Tuple2<String, Integer>"));
			Assert.fail("exception expected");
		}
		catch (InvalidTypesException e) {
			// right
		}
	}

	public static class MyQueryableMapper<A> extends RichMapFunction<String, A> implements ResultTypeQueryable<A> {
		private static final long serialVersionUID = 1L;
		
		@SuppressWarnings("unchecked")
		@Override
		public TypeInformation<A> getProducedType() {
			return (TypeInformation<A>) BasicTypeInfo.INT_TYPE_INFO;
		}
		
		@Override
		public A map(String value) throws Exception {
			return null;
		}
	}
	
	@Test
	public void testResultTypeQueryable() {
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(new MyQueryableMapper<Integer>(), BasicTypeInfo.STRING_TYPE_INFO);
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ti);
	}
	
	@Test
	public void testTupleWithPrimitiveArray() {
		RichMapFunction<Integer, Tuple9<int[],double[],long[],byte[],char[],float[],short[], boolean[], String[]>> function = new RichMapFunction<Integer, Tuple9<int[],double[],long[],byte[],char[],float[],short[], boolean[], String[]>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple9<int[], double[], long[], byte[], char[], float[], short[], boolean[], String[]> map(Integer value) throws Exception {
				return null;
			}
		};
		
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(function, BasicTypeInfo.INT_TYPE_INFO);
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertEquals(PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(0));
		Assert.assertEquals(PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(1));
		Assert.assertEquals(PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(2));
		Assert.assertEquals(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(3));
		Assert.assertEquals(PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(4));
		Assert.assertEquals(PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(5));
		Assert.assertEquals(PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(6));
		Assert.assertEquals(PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(7));
		Assert.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, tti.getTypeAt(8));
	}
	
	@Test
	public void testFunction() {
		RichMapFunction<String, Boolean> mapInterface = new RichMapFunction<String, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void setRuntimeContext(RuntimeContext t) {
				
			}
			
			@Override
			public void open(Configuration parameters) throws Exception {
			}
			
			@Override
			public RuntimeContext getRuntimeContext() {
				return null;
			}
			
			@Override
			public void close() throws Exception {
				
			}
			
			@Override
			public Boolean map(String record) throws Exception {
				return null;
			}
		};
		
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(mapInterface, BasicTypeInfo.STRING_TYPE_INFO);
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti);
	}

	@Test
	public void testInterface() {
		MapFunction<String, Boolean> mapInterface = new MapFunction<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Boolean map(String record) throws Exception {
				return null;
			}
		};

		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(mapInterface, BasicTypeInfo.STRING_TYPE_INFO);
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti);
	}
	
	@SuppressWarnings({ "serial", "unchecked", "rawtypes" })
	@Test
	public void testExtractKeySelector() {
		KeySelector<String, Integer> selector = new KeySelector<String, Integer>() {
			@Override
			public Integer getKey(String value) { return null; }
		};

		TypeInformation<?> ti = TypeExtractor.getKeySelectorTypes(selector, BasicTypeInfo.STRING_TYPE_INFO);
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ti);
		
		try {
			TypeExtractor.getKeySelectorTypes((KeySelector) selector, BasicTypeInfo.BOOLEAN_TYPE_INFO);
			Assert.fail();
		}
		catch (InvalidTypesException e) {
			// good
		}
		catch (Exception e) {
			Assert.fail("wrong exception type");
		}
	}
}
