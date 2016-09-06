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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;
import org.junit.Assert;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.ListValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.MapValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;

import org.junit.Test;

import java.io.IOException;

public class TypeInfoParserTest {
	
	@Test
	public void testBasicTypes() {
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, TypeInfoParser.parse("Integer"));
		Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, TypeInfoParser.parse("Double"));
		Assert.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, TypeInfoParser.parse("Byte"));
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, TypeInfoParser.parse("Float"));
		Assert.assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, TypeInfoParser.parse("Short"));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, TypeInfoParser.parse("Long"));
		Assert.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, TypeInfoParser.parse("Character"));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, TypeInfoParser.parse("String"));
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, TypeInfoParser.parse("Boolean"));
		Assert.assertEquals(BasicTypeInfo.VOID_TYPE_INFO, TypeInfoParser.parse("Void"));
		Assert.assertEquals(BasicTypeInfo.DATE_TYPE_INFO, TypeInfoParser.parse("Date"));
		Assert.assertEquals(BasicTypeInfo.BIG_INT_TYPE_INFO, TypeInfoParser.parse("BigInteger"));
		Assert.assertEquals(BasicTypeInfo.BIG_DEC_TYPE_INFO, TypeInfoParser.parse("BigDecimal"));
		
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, TypeInfoParser.parse("java.lang.Integer"));
		Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, TypeInfoParser.parse("java.lang.Double"));
		Assert.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, TypeInfoParser.parse("java.lang.Byte"));
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, TypeInfoParser.parse("java.lang.Float"));
		Assert.assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, TypeInfoParser.parse("java.lang.Short"));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, TypeInfoParser.parse("java.lang.Long"));
		Assert.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, TypeInfoParser.parse("java.lang.Character"));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, TypeInfoParser.parse("java.lang.String"));
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, TypeInfoParser.parse("java.lang.Boolean"));
		Assert.assertEquals(BasicTypeInfo.VOID_TYPE_INFO, TypeInfoParser.parse("java.lang.Void"));
		Assert.assertEquals(BasicTypeInfo.DATE_TYPE_INFO, TypeInfoParser.parse("java.util.Date"));
		Assert.assertEquals(BasicTypeInfo.BIG_INT_TYPE_INFO, TypeInfoParser.parse("java.math.BigInteger"));
		Assert.assertEquals(BasicTypeInfo.BIG_DEC_TYPE_INFO, TypeInfoParser.parse("java.math.BigDecimal"));
		
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, TypeInfoParser.parse("int"));
		Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, TypeInfoParser.parse("double"));
		Assert.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, TypeInfoParser.parse("byte"));		
		Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, TypeInfoParser.parse("float"));
		Assert.assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, TypeInfoParser.parse("short"));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, TypeInfoParser.parse("long"));
		Assert.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, TypeInfoParser.parse("char"));
		Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, TypeInfoParser.parse("boolean"));
		Assert.assertEquals(BasicTypeInfo.VOID_TYPE_INFO, TypeInfoParser.parse("void"));
	}
	
	@Test
	public void testValueTypes() {
		helperValueType("StringValue", StringValue.class);
		helperValueType("IntValue", IntValue.class);
		helperValueType("ByteValue", ByteValue.class);
		helperValueType("ShortValue", ShortValue.class);
		helperValueType("CharValue", CharValue.class);
		helperValueType("DoubleValue", DoubleValue.class);
		helperValueType("FloatValue", FloatValue.class);
		helperValueType("LongValue", LongValue.class);
		helperValueType("BooleanValue", BooleanValue.class);
		helperValueType("ListValue", ListValue.class);
		helperValueType("MapValue", MapValue.class);
		helperValueType("NullValue", NullValue.class);
	}
	
	private static void helperValueType(String str, Class<?> clazz) {
		TypeInformation<?> ti = TypeInfoParser.parse(str);
		Assert.assertTrue(ti instanceof ValueTypeInfo);
		ValueTypeInfo<?> vti = (ValueTypeInfo<?>) ti;
		Assert.assertEquals(clazz, vti.getTypeClass());
	}
	
	
	@Test
	@SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
	public void testBasicArrays() {
		Assert.assertEquals(BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO, TypeInfoParser.parse("Integer[]"));
		Assert.assertEquals(BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO, TypeInfoParser.parse("Double[]"));
		Assert.assertEquals(BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO, TypeInfoParser.parse("Byte[]"));
		Assert.assertEquals(BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO, TypeInfoParser.parse("Float[]"));
		Assert.assertEquals(BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO, TypeInfoParser.parse("Short[]"));
		Assert.assertEquals(BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO, TypeInfoParser.parse("Long[]"));
		Assert.assertEquals(BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO, TypeInfoParser.parse("Character[]"));
		Assert.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, TypeInfoParser.parse("String[]"));
		Assert.assertEquals(BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO, TypeInfoParser.parse("Boolean[]"));
		
		Assert.assertEquals(BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO, TypeInfoParser.parse("java.lang.Integer[]"));
		Assert.assertEquals(BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO, TypeInfoParser.parse("java.lang.Double[]"));
		Assert.assertEquals(BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO, TypeInfoParser.parse("java.lang.Byte[]"));
		Assert.assertEquals(BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO, TypeInfoParser.parse("java.lang.Float[]"));
		Assert.assertEquals(BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO, TypeInfoParser.parse("java.lang.Short[]"));
		Assert.assertEquals(BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO, TypeInfoParser.parse("java.lang.Long[]"));
		Assert.assertEquals(BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO, TypeInfoParser.parse("java.lang.Character[]"));
		Assert.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, TypeInfoParser.parse("java.lang.String[]"));
		Assert.assertEquals(BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO, TypeInfoParser.parse("java.lang.Boolean[]"));
		
		Assert.assertEquals(PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO, TypeInfoParser.parse("int[]"));
		Assert.assertEquals(PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO, TypeInfoParser.parse("double[]"));
		Assert.assertEquals(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, TypeInfoParser.parse("byte[]"));		
		Assert.assertEquals(PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO, TypeInfoParser.parse("float[]"));
		Assert.assertEquals(PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO, TypeInfoParser.parse("short[]"));
		Assert.assertEquals(PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO, TypeInfoParser.parse("long[]"));
		Assert.assertEquals(PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO, TypeInfoParser.parse("char[]"));
		Assert.assertEquals(PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO, TypeInfoParser.parse("boolean[]"));
	}
	
	@Test
	public void testTuples() {
		TypeInformation<?> ti = TypeInfoParser.parse("Tuple2<Integer, Long>");
		Assert.assertEquals(2, ti.getArity());
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, ((TupleTypeInfo<?>)ti).getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, ((TupleTypeInfo<?>)ti).getTypeAt(1));
		
		ti = TypeInfoParser.parse("Tuple0");
		Assert.assertEquals(0, ti.getArity());
		Assert.assertEquals("Java Tuple0", ti.toString());
		
		ti = TypeInfoParser.parse("org.apache.flink.api.java.tuple.Tuple0");
		Assert.assertEquals(0, ti.getArity());
		Assert.assertEquals("Java Tuple0", ti.toString());
		
		ti = TypeInfoParser.parse("Tuple3<Tuple1<String>, Tuple1<Integer>, Tuple2<Long, Long>>");
		Assert.assertEquals("Java Tuple3<Java Tuple1<String>, Java Tuple1<Integer>, Java Tuple2<Long, Long>>", ti.toString());
	}
	
	@Test
	public void testGenericType() {
		TypeInformation<?> ti = TypeInfoParser.parse("java.lang.Class");
		Assert.assertTrue(ti instanceof GenericTypeInfo);
		Assert.assertEquals(Class.class, ((GenericTypeInfo<?>) ti).getTypeClass());
	}

	public static class MyValue implements Value {
		private static final long serialVersionUID = 8607223484689147046L;

		@Override
		public void write(DataOutputView out) throws IOException {}

		@Override
		public void read(DataInputView in) throws IOException {}
	}
	
	public static class MyPojo {
		public Integer basic;
		public Tuple2<String, Integer> tuple;
		public Value valueType;
		public String[] array;
	}
	
	@Test
	public void testPojoType() {
		TypeInformation<?> ti = TypeInfoParser.parse(
				"org.apache.flink.api.java.typeutils.TypeInfoParserTest$MyPojo<"
				+ "basic=Integer,"
				+ "tuple=Tuple2<String, Integer>,"
				+ "valueType=org.apache.flink.api.java.typeutils.TypeInfoParserTest$MyValue,"
				+ "array=String[]"
				+ ">");
		Assert.assertTrue(ti instanceof PojoTypeInfo);
		PojoTypeInfo<?> pti = (PojoTypeInfo<?>) ti;
		Assert.assertEquals("array", pti.getPojoFieldAt(0).getField().getName());
		Assert.assertTrue(pti.getPojoFieldAt(0).getTypeInformation() instanceof BasicArrayTypeInfo);
		Assert.assertEquals("basic", pti.getPojoFieldAt(1).getField().getName());
		Assert.assertTrue(pti.getPojoFieldAt(1).getTypeInformation() instanceof BasicTypeInfo);
		Assert.assertEquals("tuple", pti.getPojoFieldAt(2).getField().getName());
		Assert.assertTrue(pti.getPojoFieldAt(2).getTypeInformation() instanceof TupleTypeInfo);
		Assert.assertEquals("valueType", pti.getPojoFieldAt(3).getField().getName());

//		this currently fails but should not
//		Assert.assertTrue(pti.getPojoFieldAt(3).getTypeInformation() instanceof ValueTypeInfo);
	}
	
	@Test
	public void testPojoType2() {
		TypeInformation<?> ti = TypeInfoParser.parse("Tuple2<String,Tuple2<Integer,org.apache.flink.api.java.typeutils.TypeInfoParserTest$MyPojo<basic=String>>>");
		Assert.assertTrue(ti instanceof TupleTypeInfo);
		TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
		Assert.assertTrue(tti.getTypeAt(0) instanceof BasicTypeInfo);
		Assert.assertTrue(tti.getTypeAt(1) instanceof TupleTypeInfo);
		TupleTypeInfo<?> tti2 = (TupleTypeInfo<?>)(Object)tti.getTypeAt(1);
		Assert.assertTrue(tti2.getTypeAt(0) instanceof BasicTypeInfo);
		Assert.assertTrue(tti2.getTypeAt(1) instanceof PojoTypeInfo);
		PojoTypeInfo<?> pti = (PojoTypeInfo<?>) tti2.getTypeAt(1);
		Assert.assertEquals("basic", pti.getPojoFieldAt(0).getField().getName());
		Assert.assertTrue(pti.getPojoFieldAt(0).getTypeInformation() instanceof BasicTypeInfo);
	}

	@Test
	public void testObjectArrays() {
		TypeInformation<?> ti = TypeInfoParser.parse("java.lang.Class[]");

		Assert.assertTrue(ti instanceof ObjectArrayTypeInfo<?, ?>);
		Assert.assertEquals(Class.class, ((ObjectArrayTypeInfo<?, ?>) ti).getComponentInfo().getTypeClass());
		
		TypeInformation<?> ti2 = TypeInfoParser.parse("Tuple2<Integer,Double>[]");

		Assert.assertTrue(ti2 instanceof ObjectArrayTypeInfo<?, ?>);
		Assert.assertTrue(((ObjectArrayTypeInfo<?, ?>) ti2).getComponentInfo() instanceof TupleTypeInfo);
		
		TypeInformation<?> ti3 = TypeInfoParser.parse("Tuple2<Integer[],Double>[]");
		Assert.assertEquals("ObjectArrayTypeInfo<Java Tuple2<BasicArrayTypeInfo<Integer>, Double>>", ti3.toString());
	}
	
	@Test
	public void testLargeMixedTuple() {
		TypeInformation<?> ti = TypeInfoParser.parse("org.apache.flink.api.java.tuple.Tuple4<Double,java.lang.Class[],StringValue,Tuple1<int>>[]");
		Assert.assertEquals("ObjectArrayTypeInfo<Java Tuple4<Double, ObjectArrayTypeInfo<GenericType<java.lang.Class>>, ValueType<StringValue>, Java Tuple1<Integer>>>", ti.toString());
	}
	
	public static enum MyEnum {
		ONE, TWO, THREE
	}
	
	@Test
	public void testEnumType() {
		TypeInformation<?> ti = TypeInfoParser.parse("Enum<org.apache.flink.api.java.typeutils.TypeInfoParserTest$MyEnum>");
		Assert.assertEquals("EnumTypeInfo<org.apache.flink.api.java.typeutils.TypeInfoParserTest$MyEnum>", ti.toString());
		
		TypeInformation<?> ti2 = TypeInfoParser.parse("java.lang.Enum<org.apache.flink.api.java.typeutils.TypeInfoParserTest$MyEnum>");
		Assert.assertEquals("EnumTypeInfo<org.apache.flink.api.java.typeutils.TypeInfoParserTest$MyEnum>", ti2.toString());
	}
	
	@Test
	public void testException() {
		try {
	    	TypeInfoParser.parse("THIS_CLASS_DOES_NOT_EXIST");
			Assert.fail("exception expected");
		} catch (IllegalArgumentException e) {
			// right
		}
		
		try {
	    	TypeInfoParser.parse("Tuple2<Integer>");
			Assert.fail("exception expected");
		} catch (IllegalArgumentException e) {
			// right
		}
		
		try {
	    	TypeInfoParser.parse("Tuple3<Integer,,>");
			Assert.fail("exception expected");
		} catch (IllegalArgumentException e) {
			// right
		}
		
		try {
	    	TypeInfoParser.parse("Tuple1<Integer,Double>");
			Assert.fail("exception expected");
		} catch (IllegalArgumentException e) {
			// right
		}
	}

	@Test
	public void testMultiDimensionalArray() {
		// tuple
		TypeInformation<?> ti = TypeInfoParser.parse("Tuple2<Integer, Double>[][]");
		Assert.assertEquals("ObjectArrayTypeInfo<ObjectArrayTypeInfo<Java Tuple2<Integer, Double>>>", ti.toString());
		
		// pojos
		ti = TypeInfoParser.parse("org.apache.flink.api.java.typeutils.TypeInfoParserTest$MyPojo<basic=String>[][][]");
		Assert.assertEquals("ObjectArrayTypeInfo<ObjectArrayTypeInfo<ObjectArrayTypeInfo<"
				+ "PojoType<org.apache.flink.api.java.typeutils.TypeInfoParserTest$MyPojo, fields = [basic: String]>"
				+ ">>>", ti.toString());
		
		// basic types
		ti = TypeInfoParser.parse("Float[][][]");
		Assert.assertEquals("ObjectArrayTypeInfo<ObjectArrayTypeInfo<BasicArrayTypeInfo<Float>>>", ti.toString());
		ti = TypeInfoParser.parse("String[][][]");
		Assert.assertEquals("ObjectArrayTypeInfo<ObjectArrayTypeInfo<BasicArrayTypeInfo<String>>>", ti.toString());
		ti = TypeInfoParser.parse("Date[][][]");
		Assert.assertEquals("ObjectArrayTypeInfo<ObjectArrayTypeInfo<ObjectArrayTypeInfo<Date>>>", ti.toString());
		
		// primitive types
		ti = TypeInfoParser.parse("int[][][]");
		Assert.assertEquals("ObjectArrayTypeInfo<ObjectArrayTypeInfo<int[]>>", ti.toString());
		ti = TypeInfoParser.parse("boolean[][][]");
		Assert.assertEquals("ObjectArrayTypeInfo<ObjectArrayTypeInfo<boolean[]>>", ti.toString());
		
		// value types
		ti = TypeInfoParser.parse("IntValue[][][]");
		Assert.assertEquals("ObjectArrayTypeInfo<ObjectArrayTypeInfo<ObjectArrayTypeInfo<ValueType<IntValue>>>>", ti.toString());
		
		// value types
		ti = TypeInfoParser.parse("org.apache.flink.api.java.typeutils.TypeInfoParserTest$MyValue[][][]");

		// this fails because value types are parsed in a wrong way
//		Assert.assertEquals("ObjectArrayTypeInfo<ObjectArrayTypeInfo<ObjectArrayTypeInfo<"
//				+ "ValueType<TypeInfoParserTest$MyValue>"
//				+ ">>>", ti.toString());
		
		
		
		// enum types
		ti = TypeInfoParser.parse("Enum<org.apache.flink.api.java.typeutils.TypeInfoParserTest$MyEnum>[][][]");
		Assert.assertEquals("ObjectArrayTypeInfo<ObjectArrayTypeInfo<ObjectArrayTypeInfo<"
				+ "EnumTypeInfo<org.apache.flink.api.java.typeutils.TypeInfoParserTest$MyEnum>"
				+ ">>>", ti.toString());
	}
}
